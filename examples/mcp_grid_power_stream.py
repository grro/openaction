"""
============================================================================
 Task: GridPowerStream
============================================================================

PURPOSE
-------
Streams near real-time grid power (Watts) from the Energy-MCP server.
- Positive = IMPORT (buying electricity)
- Negative = EXPORT (feeding back to grid)
- Zero     = BALANCED

ARCHITECTURE (Dual-Thread Bridge)
---------------------------------
1. Pump Thread: Persistent MCP session with a message handler. It "arms"
   the subscription via an initial read and forwards push notifications
   to the Task framework via self.subscription.notify().

2. Fetch Thread: A second persistent session used exclusively for
   synchronous, on-demand reads when on_execute() is triggered.

Note: Splitting into two sessions is mandatory as FastMCP clients
serialize requests; mixing streams and reads on one session causes deadlocks.

PERSISTENT STATE (self.store)
-----------------------------
Key 'last' -> JSON object:
  {
      "watts":       int | None,        # Last successfully read value
      "ts":          ISO-8601 string,   # UTC timestamp of the reading
      "count":       int,               # Total execution cycles
      "last_delta":  int | None         # Current watts - previous watts
  }
============================================================================
"""

import asyncio
import json
import re
import threading
import time as _time
from datetime import datetime, timezone
from fastmcp import Client

# --- Configuration ---
ENERGY_MCP_URL = "http://192.168.1.99:8411/sse"
RESOURCE_GRID = "sensor://metrics/grid_power"
# The server usually requires an initial read to enable push notifications
ARMING_RESOURCE = "sensor://metrics/available_surplus"
FETCH_READ_TIMEOUT_SEC = 5.0

class GridPowerStream(Task):

    def __init__(self, store, subscription):
        super().__init__(store, subscription)

        # Initialize shared flags and metrics.
        # No background processing starts here; triggered via on_activate().
        self._stop = False
        self._threads_started = False
        self._pump_status = "init"
        self._fetch_status = "init"
        self._notification_count = 0
        self._last_notification_ts = 0.0

        # Cross-thread communication for the Fetch Thread
        self._loop = None
        self._client = None
        self._ready = threading.Event()

        # OS Thread handles
        self._pump_thread = None
        self._fetch_thread = None

    # ------------------------------------------------------------------
    # Lifecycle Hooks
    # ------------------------------------------------------------------

    def on_activate(self):
        """Executed once when the task is loaded by the framework."""
        self._ensure_threads()
        print("GridPowerStream activated.")

    def on_deactivate(self):
        """Executed once during teardown. Signals threads to exit gracefully."""
        self._stop = True
        self._ready.clear()

        # Reset flag to allow safe reactivation if the object persists
        self._threads_started = False

        # Graceful join of daemon threads
        for t in (self._pump_thread, self._fetch_thread):
            if t and t.is_alive():
                t.join(timeout=3.0)

        print("GridPowerStream deactivated.")

    # ------------------------------------------------------------------
    # Async Background Loops
    # ------------------------------------------------------------------

    async def _pump_async(self):
        """Handles incoming SSE push events from the MCP server."""
        async def handler(msg):
            try:
                root = getattr(msg, "root", msg)
                if getattr(root, "method", None) != "notifications/resources/updated":
                    return

                params = getattr(root, "params", None)
                uri = str(getattr(params, "uri", "")) if params else ""
                if not uri:
                    return

                # Update telemetry for health monitoring
                self._last_notification_ts = _time.time()
                self._notification_count += 1

                # Notify framework to trigger matching @when decorators
                try:
                    self.subscription.notify(uri)
                except Exception as e:
                    print(f"subscription.notify failed: {e}")
            except Exception as e:
                print(f"Notification handler error: {e}")

        while not self._stop:
            try:
                async with Client(ENERGY_MCP_URL, message_handler=handler) as c:
                    await c.ping()

                    # CRITICAL: Perform initial reads to "arm" the subscription.
                    # The server starts pushing updates only after a client
                    # shows interest via a read request.
                    await c.read_resource(RESOURCE_GRID)
                    if ARMING_RESOURCE != RESOURCE_GRID:
                        await c.read_resource(ARMING_RESOURCE)

                    self._pump_status = "armed"
                    while not self._stop:
                        await asyncio.sleep(1)
            except Exception as e:
                self._pump_status = f"err: {e}"
                await asyncio.sleep(1)

    async def _fetch_async(self):
        """Maintains a warm client session for on-demand synchronous reads."""
        self._loop = asyncio.get_running_loop()
        while not self._stop:
            try:
                async with Client(ENERGY_MCP_URL) as c:
                    await c.ping()
                    self._client = c
                    self._ready.set()
                    self._fetch_status = "ready"

                    try:
                        while not self._stop:
                            await asyncio.sleep(1)
                    finally:
                        self._ready.clear()
                        self._client = None
            except Exception as e:
                self._fetch_status = f"err: {e}"
                self._ready.clear()
                self._client = None
                await asyncio.sleep(1)
        self._loop = None

    def _ensure_threads(self):
        """Idempotent starter for daemon threads."""
        if self._threads_started:
            return

        self._stop = False
        self._threads_started = True

        def _runner(func, status_attr):
            try:
                asyncio.run(func())
            except Exception as e:
                setattr(self, status_attr, f"crashed: {e}")
                print(f"Thread {status_attr} crashed: {e}")

        self._pump_thread = threading.Thread(target=_runner, args=(self._pump_async, '_pump_status'), name="grid_pump", daemon=True)
        self._fetch_thread = threading.Thread(target=_runner, args=(self._fetch_async, '_fetch_status'), name="grid_fetch", daemon=True)

        self._pump_thread.start()
        self._fetch_thread.start()

    # ------------------------------------------------------------------
    # Trigger Entry Point
    # ------------------------------------------------------------------

    @when("Rule loaded")
    @when("Item sensor://metrics/grid_power changed")
    @when("Time cron */1 * * * *")
    def on_execute(self) -> str:
        self._ensure_threads()

        watts, info = None, "not ready"

        # 1. Attempt ad-hoc read using the warm fetch client
        if self._ready.wait(timeout=3.0) and self._client and self._loop:
            try:
                # Bridge sync call to async thread
                fut = asyncio.run_coroutine_threadsafe(self._client.read_resource(RESOURCE_GRID), self._loop)
                res = fut.result(timeout=FETCH_READ_TIMEOUT_SEC)
                # Extract text blocks from MCP response
                text = "".join(getattr(b, "text", "") for b in res if getattr(b, "text", ""))

                # Parse numeric value (e.g., "202 W")
                m = re.search(r"(-?\d+)\s*W", text)
                if m:
                    watts = int(m.group(1))
                    info = "ok"
                else:
                    info = "unparsable"
            except Exception as e:
                info = f"err: {e}"

        # 2. Update persistent state and calculate delta
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        prev = json.loads(self.store.get("last") or '{"count": 0}')

        delta = (watts - prev["watts"]) if watts is not None and prev.get("watts") is not None else None

        new_state = {
            "watts": watts,
            "ts": now_iso,
            "count": prev.get("count", 0) + 1,
            "last_delta": delta,
        }
        self.store.put("last", json.dumps(new_state))

        # 3. Format telemetry and health output
        flow = "UNKNOWN" if watts is None else ("IMPORT" if watts > 0 else ("EXPORT" if watts < 0 else "BALANCED"))
        age = f"{(_time.time() - self._last_notification_ts):.1f}s" if self._last_notification_ts else "n/a"

        pump_alive = self._pump_thread.is_alive() if self._pump_thread else False
        fetch_alive = self._fetch_thread.is_alive() if self._fetch_thread else False

        return (
            f"grid={watts}W [{flow}] delta={delta} updates={new_state['count']} info={info} | "
            f"pump={self._pump_status} alive={pump_alive} | "
            f"fetch={self._fetch_status} alive={fetch_alive} | "
            f"notif={self._notification_count} (last {age})"
        )