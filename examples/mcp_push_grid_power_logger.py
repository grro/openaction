import asyncio
import logging
import re
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from fastmcp import Client

# Assuming these are available from your API package
from api.environment import Environment
from api.task import BackgroundTask

logger = logging.getLogger(__name__)

# --- Configuration ----------------------------------------------------------

MCP_URL = "http://192.168.1.99:8411/sse"
RESOURCE_URI = "sensor://metrics/grid_power"
TZ = ZoneInfo("Europe/Berlin")
STALE_AFTER = timedelta(minutes=3)
READ_TIMEOUT = 5.0
RECONNECT_BACKOFF_MAX = 60.0

# Persistent store keys
K_CURRENT = "current_grid_value"
K_PREVIOUS = "previous_grid_value"
K_TIMESTAMP = "current_grid_timestamp"
K_STATE = "mcp_state"

# Parses lines like "Grid Power: -42 W (Positive: Import ...)"
_VALUE_RE = re.compile(r"Grid Power:\s*(-?\d+)\s*W", re.IGNORECASE)


def _classify(watts: int) -> tuple[str, str]:
    """Return (level, direction) for a signed grid-power value in watts."""
    if watts <= -1500:
        level = "export_high"
    elif watts < 0:
        level = "export"
    elif watts < 1500:
        level = "import"
    else:
        level = "import_high"
    direction = "exporting" if watts < 0 else "importing"
    return level, direction


class GridPowerLogger(BackgroundTask):
    """Event-driven grid-power listener using the Energy MCP service.

    The MCP server does not implement resources/subscribe, but it does push
    ResourceUpdatedNotification messages once a resource has been read at least
    once. We exploit that pattern:

      * A supervisor coroutine in a dedicated thread holds an open SSE
        connection to the MCP server and keeps it alive across reconnects.
      * Every server-pushed notification triggers on_execute via a worker
        thread (so on_execute, which is synchronous and blocks on a read,
        never runs on the supervisor's event loop and cannot deadlock it).
      * The actual read_resource call lives only in on_execute, which is
        also the entry point for cron and manual triggers. on_execute is
        serialized by _exec_lock — its state fields are therefore safe
        without a separate state lock.
    """

    def __init__(self, environment: Environment) -> None:
        super().__init__(environment)
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client: Client | None = None
        self._running = False
        self._exec_lock = threading.Lock()

        # In-memory cache of the latest state; rehydrated in on_activate.
        self._current: int | None = None
        self._previous: int | None = None
        self._last_dt: datetime | None = None
        self._mcp_state = "init"

    # --- Lifecycle ----------------------------------------------------------

    def on_activate(self) -> None:
        # Restore the last known reading so we have something to report even
        # before the first push arrives.
        try:
            self._current = int(self.environment.store.get(K_CURRENT))
            self._previous = int(self.environment.store.get(K_PREVIOUS))
            self._last_dt = datetime.fromisoformat(self.environment.store.get(K_TIMESTAMP))
        except (TypeError, ValueError):
            pass

        self._running = True
        self._thread = threading.Thread(
            target=lambda: asyncio.run(self._supervisor()),
            name="grid-mcp",
            daemon=True,
        )
        self._thread.start()
        self._set_state("starting")

    def on_deactivate(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=3.0)
        self._set_state("stopped")

    # --- MCP supervisor (async, runs on its own thread) ---------------------

    async def _supervisor(self) -> None:
        """Maintain the MCP connection and dispatch notifications.

        Never reads the resource itself — that responsibility lies with
        on_execute. The supervisor only keeps the SSE stream open and
        kicks on_execute via worker threads when something happens.
        """
        self._loop = asyncio.get_running_loop()
        backoff = 1.0

        while self._running:
            try:
                self._set_state("connecting")
                async with Client(MCP_URL, message_handler=self._on_message) as client:
                    self._client = client
                    self._set_state("connected")
                    backoff = 1.0  # reset on success

                    # Bootstrap read in a worker thread; it both fetches the
                    # initial value and tells the server we want push updates.
                    asyncio.create_task(asyncio.to_thread(self._safe_execute, "bootstrap"))

                    # Park the supervisor here — push notifications drive work.
                    while self._running:
                        await asyncio.sleep(1.0)

            except Exception as e:
                self._set_state(f"error: {type(e).__name__}")
                logger.warning("supervisor: mcp connection error: %s", e)
                self._client = None

            if not self._running:
                break

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX)

        self._set_state("supervisor_exit")

    async def _on_message(self, msg) -> None:
        """Trigger on_execute when our resource was updated.

        fastmcp wraps notifications in a root object; we tolerate both shapes.
        """
        data = getattr(msg, "root", msg)
        if getattr(data, "method", None) != "notifications/resources/updated":
            return
        if str(getattr(getattr(data, "params", None), "uri", "")) != RESOURCE_URI:
            return

        # Hand off to a worker thread so on_execute can synchronously wait on
        # a coroutine scheduled back onto this same event loop.
        asyncio.create_task(asyncio.to_thread(self._safe_execute, "push"))

    def _safe_execute(self, source: str) -> None:
        """Run on_execute and swallow exceptions so worker threads stay clean."""
        try:
            logger.info("on_execute(%s) -> %s", source, self.on_execute())
        except Exception as e:
            logger.exception("on_execute(%s) failed: %s", source, e)

    # --- Sync→async bridge for reading the resource -------------------------

    async def _do_read(self) -> int | None:
        """Read and parse the current grid-power value via MCP."""
        if self._client is None:
            return None
        result = await asyncio.wait_for(
            self._client.read_resource(RESOURCE_URI), timeout=READ_TIMEOUT
        )
        text = " ".join(getattr(c, "text", "") for c in result)
        match = _VALUE_RE.search(text)
        if not match:
            logger.warning("could not parse grid power from: %r", text[:200])
            return None
        return int(match.group(1))

    def _read_sync(self) -> int | None:
        """Synchronously fetch the current value by hopping onto the supervisor loop.

        Architecturally, no caller of on_execute should be running on the
        supervisor's event-loop thread — push handlers, cron and manual
        triggers all enter via worker threads. The guard below is a safety
        net: if a future refactor accidentally routes a call through the
        loop thread, fut.result() would deadlock waiting for a coroutine
        that can never be scheduled. We fail loudly instead.
        """
        if not self._loop or not self._loop.is_running() or not self._client:
            return None

        # Deadlock guard: refuse to dispatch onto our own running loop.
        try:
            if asyncio.get_running_loop() is self._loop:
                logger.error(
                    "_read_sync called from the supervisor loop thread; "
                    "refusing to deadlock"
                )
                return None
        except RuntimeError:
            # No running loop in this thread — the normal, safe case.
            pass

        try:
            future = asyncio.run_coroutine_threadsafe(self._do_read(), self._loop)
            return future.result(timeout=READ_TIMEOUT + 1.0)
        except Exception as e:
            logger.warning("sync read failed: %s", e)
            return None

    # --- State persistence --------------------------------------------------

    def _set_state(self, state: str) -> None:
        """Update both the in-memory and persistent MCP connection state."""
        self._mcp_state = state
        self.environment.store.put(K_STATE, state)

    def _commit_value(self, watts: int) -> None:
        """Persist a new value if it differs from the current one."""
        if watts == self._current:
            return
        self._previous = self._current
        self._current = watts
        self._last_dt = datetime.now(TZ)

        if self._previous is not None:
            self.environment.store.put(K_PREVIOUS, str(self._previous))
        self.environment.store.put(K_CURRENT, str(self._current))
        self.environment.store.put(K_TIMESTAMP, self._last_dt.isoformat(timespec="seconds"))

    # --- Business logic -----------------------------------------------------

    def on_execute(self) -> str:
        """Read the current grid-power value, persist it, and return a status line.

        Triggered by push notifications, the cron fallback, or manual calls.
        Serialized by _exec_lock so concurrent triggers don't interleave.
        """
        with self._exec_lock:
            watts = self._read_sync()
            if watts is not None:
                self._commit_value(watts)
            return self._build_report()

    def _build_report(self) -> str:
        """Format a human-readable status line from the current in-memory state."""
        if self._current is None:
            return f"Grid: no data yet (mcp_state={self._mcp_state})"

        level, direction = _classify(self._current)
        age = (datetime.now(TZ) - self._last_dt) if self._last_dt else timedelta.max

        if self._previous is None:
            delta_str = "initial"
        else:
            delta = self._current - self._previous
            sign = "+" if delta >= 0 else ""
            delta_str = f"{sign}{delta} W vs prev {self._previous} W"

        ts_str = self._last_dt.isoformat(timespec="seconds") if self._last_dt else "?"
        report = f"Grid {self._current:+d} W [{level}, {direction}] ({delta_str}) at {ts_str}"

        if age > STALE_AFTER:
            minutes_stale = int(age.total_seconds() // 60)
            return f"{report} | STALE: last update {minutes_stale}m ago (mcp_state={self._mcp_state})"

        return report