"""
============================================================================
 Task: ShellyPowerPoller
============================================================================

PURPOSE
-------
Polls the Shelly Pro 1PM 'PvAll' device periodically and reports the
current power draw in Watts.

Device Details:
- Model:    Shelly Pro 1PM (Gen 2)
- ID:       shellypro1pm-30c6f78101f8
- Endpoint: /rpc/Shelly.GetStatus
- Field:    switch:0.apower

SESSION MANAGEMENT
------------------
To optimize performance and reduce per-request overhead, this task utilizes
HTTP Keep-Alive via `requests.Session`. The session is initialized during the
activation phase (`on_activate`) and reused across sequential execution ticks,
keeping the underlying TCP connection alive. It is safely torn down during
`on_deactivate`.
============================================================================
"""

from datetime import datetime, timezone
from typing import Optional

import requests

# System-injected dependencies (Implicitly available in the runtime environment)
# from core import Task, when


class ShellyPowerPoller(Task):
    """
    Periodic task to monitor and report power consumption from a Shelly device.
    """

    # --- Configuration Constants ---
    STATUS_URL: str = "http://10.1.33.53/rpc/Shelly.GetStatus"
    HTTP_TIMEOUT_SEC: float = 5.0

    def __init__(self, store, subscription):
        super().__init__(store, subscription)
        self._session: Optional[requests.Session] = None

    # ------------------------------------------------------------------ Lifecycle

    def _create_session(self) -> requests.Session:
        """Helper to instantiate and configure the HTTP session."""
        session = requests.Session()
        session.headers.update({"Connection": "keep-alive"})
        return session

    def on_activate(self) -> None:
        """Triggered when the task is loaded into the active registry."""
        self._session = self._create_session()
        print(f"[{self.__class__.__name__}] Activated: Persistent HTTP session created.")

    def on_deactivate(self) -> None:
        """Triggered when the task is unloaded or the system shuts down."""
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
        print(f"[{self.__class__.__name__}] Deactivated: HTTP session closed.")

    # ------------------------------------------------------------------ Execution

    @when("Rule loaded")
    @when("Time cron */1 * * * *")
    def on_execute(self) -> str:
        """
        Executes the polling logic every minute and on task initialization.

        Raises:
            requests.RequestException: On network timeouts or HTTP errors.
            ValueError: If the JSON payload cannot be parsed.
            KeyError: If the expected data fields are missing from the response.

        Returns:
            str: A formatted string containing the timestamp and power draw.
        """
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

        # Defensive fallback: Recreate session if missing
        if self._session is None:
            self._session = self._create_session()

        # 1. Fetch Data
        try:
            resp = self._session.get(self.STATUS_URL, timeout=self.HTTP_TIMEOUT_SEC)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            # Raising the exception allows the task runner to register the failure
            # and trigger any automatic retry/backoff mechanisms.
            raise RuntimeError(f"Network error fetching Shelly status: {e}") from e
        except ValueError as e:
            raise ValueError(f"Invalid JSON received from device: {e}") from e

        # 2. Extract & Validate Status
        switch = data.get("switch:0")
        if not isinstance(switch, dict):
            raise KeyError("The key 'switch:0' is missing or invalid in the device response.")

        watts = switch.get("apower")
        if not isinstance(watts, (int, float)):
            raise KeyError("The key 'apower' is missing or non-numeric in the device response.")

        # 3. Format Output
        return f"[{now_iso}] power = {watts:.1f} W"