import asyncio
import logging
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from fastmcp import Client

# Assuming these are available from your API package
from api.environment import Environment
from api.task import BackgroundTask

logger = logging.getLogger(__name__)

# --- Configuration ----------------------------------------------------------
MCP_URL = "http://192.168.0.100:8322/sse"
SHUTTER_NAME = "office"
TZ = ZoneInfo("Europe/Berlin")

OPEN_HOUR = 8
CLOSE_HOUR = 22
OPEN_POS = 0     # 0 = fully OPEN
CLOSE_POS = 100  # 100 = fully CLOSED

CALL_TIMEOUT = 8.0
RECONNECT_BACKOFF_MAX = 60.0
K_LAST_SLOT = "last_executed_slot"
K_STATE = "mcp_state"


class OfficeShutterSchedule(BackgroundTask):
    """Daily open at 08:00 / close at 22:00 for the office rollershutter.

    Architecture
    ------------
    A long-lived asyncio supervisor runs on its own daemon thread and holds
    one open `fastmcp.Client` session for the lifetime of the task. on_execute
    is synchronous (cron / manual triggers run on the host thread) and bridges
    onto the supervisor loop via `run_coroutine_threadsafe`, so each invocation
    reuses the existing TCP/SSE connection instead of opening a new one.

    Robustness
    ----------
    * Supervisor reconnects with exponential backoff if the MCP session dies.
    * A per-day slot id (YYYY-MM-DD:open|close) is persisted, so repeated
      triggers within the same hour are a safe no-op.
    * Connection state is exposed via the persistent store under `mcp_state`
      for observability.
    """

    def __init__(self, environment: Environment) -> None:
        super().__init__(environment)
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client: Client | None = None
        self._client_ready = threading.Event()
        self._running = False

    # --- Lifecycle ----------------------------------------------------------

    def on_activate(self) -> None:
        self._running = True
        self._client_ready.clear()
        self._thread = threading.Thread(
            target=lambda: asyncio.run(self._supervisor()),
            name="office-shutter-mcp",
            daemon=True,
        )
        self._thread.start()
        self._set_state("starting")

    def on_deactivate(self) -> None:
        self._running = False
        # Wake the supervisor's sleep loop so it can exit promptly.
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(lambda: None)
        if self._thread:
            self._thread.join(timeout=3.0)
        self._set_state("stopped")

    # --- MCP supervisor (async, runs on its own thread) ---------------------

    async def _supervisor(self) -> None:
        """Keep one MCP client session alive for the lifetime of the task.

        Re-establishes the connection with exponential backoff on errors.
        Never reads/writes itself — on_execute is the sole caller of
        `set_position`, dispatched here via `run_coroutine_threadsafe`.
        """
        self._loop = asyncio.get_running_loop()
        backoff = 1.0

        while self._running:
            try:
                self._set_state("connecting")
                async with Client(MCP_URL) as client:
                    self._client = client
                    self._set_state("connected")
                    self._client_ready.set()
                    backoff = 1.0

                    # Park here until shutdown; tool calls hop onto this loop.
                    while self._running:
                        await asyncio.sleep(1.0)

            except Exception as e:
                self._set_state(f"error: {type(e).__name__}")
                logger.warning("office-shutter mcp connection error: %s", e)
            finally:
                self._client = None
                self._client_ready.clear()

            if not self._running:
                break

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX)

        self._set_state("supervisor_exit")

    # --- Sync -> async bridge ----------------------------------------------

    async def _do_set_position(self, position: int) -> str:
        if self._client is None:
            raise RuntimeError("MCP client not connected")
        result = await asyncio.wait_for(
            self._client.call_tool(
                "set_position",
                {"name": SHUTTER_NAME, "position": position},
            ),
            timeout=CALL_TIMEOUT,
        )
        return " ".join(getattr(c, "text", "") for c in result.content).strip()

    def _set_position_sync(self, position: int) -> str:
        # Wait briefly for the supervisor to come up on first call after activate.
        if not self._client_ready.wait(timeout=5.0):
            raise RuntimeError(f"MCP client not ready (state={self._mcp_state_cached()})")
        if not self._loop or not self._loop.is_running() or not self._client:
            raise RuntimeError("MCP supervisor loop unavailable")

        future = asyncio.run_coroutine_threadsafe(
            self._do_set_position(position), self._loop
        )
        return future.result(timeout=CALL_TIMEOUT + 1.0)

    # --- State helpers ------------------------------------------------------

    def _set_state(self, state: str) -> None:
        self.environment.store.put(K_STATE, state)

    def _mcp_state_cached(self) -> str:
        return self.environment.store.get(K_STATE) or "unknown"

    def _resolve_action(self, now: datetime):
        hour = now.hour
        day = now.date().isoformat()
        if hour == OPEN_HOUR:
            return f"{day}:open", OPEN_POS, "open"
        if hour == CLOSE_HOUR:
            return f"{day}:close", CLOSE_POS, "close"
        return None

    # --- Business logic -----------------------------------------------------

    def on_execute(self) -> str:
        now = datetime.now(TZ)
        action = self._resolve_action(now)
        if action is None:
            return f"No scheduled action at hour={now.hour}; ignored (mcp_state={self._mcp_state_cached()})."

        slot_id, target, label = action

        if self.environment.store.get(K_LAST_SLOT) == slot_id:
            return f"Slot {slot_id} already executed; skipping."

        try:
            response = self._set_position_sync(target)
        except Exception as e:
            logger.exception("office shutter %s failed", label)
            return f"Failed to {label} office shutter: {type(e).__name__}: {e}"

        self.environment.store.put(K_LAST_SLOT, slot_id)
        return (
            f"Office shutter -> {label} (position={target}) "
            f"at {now.isoformat(timespec='seconds')}. MCP: {response}"
        )