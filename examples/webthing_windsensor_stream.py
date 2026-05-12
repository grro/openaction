"""
============================================================================
 Task: windsensor_ws_stream
============================================================================

PURPOSE
-------
Triggers on every WebThing push of the 'windspeed' property from the wind
sensor. The smoothed averages are deliberately ignored.

on_execute does NOT use the pushed value — it fetches the current
windspeed fresh via HTTP each time it fires.
============================================================================
"""

import asyncio
import json
import threading
from datetime import datetime, timezone
from typing import Optional

import requests
import websockets

# --- Configuration Constants ---
WS_URL = "ws://10.1.33.99:9860/"
HTTP_URL = "http://10.1.33.99:9860/properties/windspeed"
HTTP_TIMEOUT_SEC = 3.0
RECONNECT_DELAY_SEC = 3.0


class WindsensorWsStream(Task):
    """
    Listens to WebSocket property changes and fetches the latest windspeed via HTTP.
    """

    def __init__(self, store, subscription):
        super().__init__(store, subscription)
        self._stop = False
        self._thread: Optional[threading.Thread] = None
        self._ws_status = "init"
        self._notify_count = 0
        self._session: Optional[requests.Session] = None

    # ------------------------------------------------------------------ Lifecycle

    def on_activate(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({"Connection": "keep-alive"})
        self._stop = False
        self._start_ws_thread()
        print(f"[{self.__class__.__name__}] Activated.")

    def on_deactivate(self) -> None:
        self._stop = True
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)

        if self._session is not None:
            self._session.close()
            self._session = None

        print(f"[{self.__class__.__name__}] Deactivated.")

    # ------------------------------------------------------------------ WS Background Thread

    def _start_ws_thread(self) -> None:
        """Starts the WebSocket background thread if it is not currently running."""
        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(
            target=lambda: asyncio.run(self._ws_loop()),
            name="windsensor_ws",
            daemon=True
        )
        self._thread.start()

    async def _ws_loop(self) -> None:
        """Async loop to maintain the WebSocket connection."""
        while not self._stop:
            try:
                async with websockets.connect(WS_URL, subprotocols=["webthing"], ping_interval=20) as ws:
                    self._ws_status = "connected"
                    async for raw in ws:
                        if self._stop:
                            break
                        self._handle_frame(raw)
            except Exception as e:
                self._ws_status = f"err: {e}"
                await asyncio.sleep(RECONNECT_DELAY_SEC)

    def _handle_frame(self, raw: str) -> None:
        """
        Parses incoming WS frames and notifies the system only if the
        exact 'windspeed' property is updated.
        """
        try:
            msg = json.loads(raw)
            # Safely check if this is a propertyStatus containing 'windspeed'
            if msg.get("messageType") == "propertyStatus" and "windspeed" in msg.get("data", {}):
                self._notify_count += 1
                self.subscription.notify("thing://wind/windspeed")
        except Exception:
            # Ignore invalid JSON or unexpected formats silently
            pass

    # ------------------------------------------------------------------ Execution

    @when("Rule loaded")
    @when("Item thing://wind/windspeed changed")
    @when("Time cron */1 * * * *")
    def on_execute(self) -> str:
        # Self-healing: Ensure the WS thread is still running
        self._start_ws_thread()

        # Defensive fallback for the HTTP session
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({"Connection": "keep-alive"})

        # 1. Fetch current data via HTTP
        try:
            resp = self._session.get(HTTP_URL, timeout=HTTP_TIMEOUT_SEC)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"HTTP error fetching windspeed: {e}") from e
        except ValueError as e:
            raise ValueError(f"Invalid JSON received from wind sensor: {e}") from e

        # 2. Extract windspeed safely
        value = data.get("windspeed") if isinstance(data, dict) else data

        if not isinstance(value, (int, float)):
            raise ValueError(f"Cannot parse numeric windspeed from response: {data!r}")

        # 3. Format Output
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        alive = bool(self._thread and self._thread.is_alive())

        return (
            f"[{now_iso}] windspeed = {value:.1f} km/h | "
            f"ws={self._ws_status} thread_alive={alive} | "
            f"notifications={self._notify_count}"
        )