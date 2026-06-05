import asyncio
import logging
import random
import re
import time as _time
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from astral import LocationInfo
from astral.sun import sun
from fastmcp import Client

logger = logging.getLogger(__name__)

TZ = ZoneInfo("Europe/Berlin")

# --- Anonymized Location & URLs ---
LOCATION = LocationInfo("<CITY>", "<COUNTRY>", "<TIMEZONE>", 0.0000, 0.0000)

SHUTTER_MCP_URL = "http://<SHUTTER_IP>:<PORT>/sse"
PRESENCE_MCP_URL = "http://<PRESENCE_IP>:<PORT>/sse"
SHUTTER_NAME = "office"

CONN_LIFETIME_S = 33 * 60  # renew persistent clients after 33 min

OPEN_EARLIEST, OPEN_LATEST = time(5, 45), time(8, 0)
CLOSE_EARLIEST, CLOSE_LATEST = time(16, 0), time(22, 0)
OPEN_POS, CLOSE_POS = 0, 100

TOLERANCE = 5

SECURITY_MIN_POS, SECURITY_MAX_POS = 50, 70
SECURITY_MIN_INTERVAL_S, SECURITY_MAX_INTERVAL_S = 25 * 60, 120 * 60
SECURITY_MIN_DELTA = 5

K_EXPECTED = "expected_position"
K_LAST_EVENT = "last_auto_event"
K_OVERRIDE = "override_until_event"
K_MODE = "current_mode"
K_NEXT_SEC_CHANGE = "next_security_change_iso"
K_PRE_SEC_POS = "pre_security_position"
K_PRE_SEC_LAST_EVENT = "pre_security_last_event"

OBSOLETE_KEYS = ("pre_away_position", "pre_away_last_event")

TOPIC_SHUTTER = "Shutter"
TOPIC_MODE = "Mode"
TOPIC_SECURITY = "Security"


class OfficeShutterSmartAutomation(BackgroundTask):
    """
    Office shutter with sun-position normal mode and security mode
    (presence simulation). Persistent MCP clients with limited
    lifetime (33 min): after expiration, shutter and presence clients
    are closed and rebuilt. Prevents silently died long-term sessions.
    Additional read error retry via _call_with_retry.
    """

    @property
    def _store(self):
        return self.environment.store

    @property
    def _eventlog(self):
        return self.environment.eventlog

    # --- Lifecycle ----------------------------------------------------------

    def on_activate(self):
        import threading
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._loop.run_forever, name="office-shutter-mcp-loop",
            daemon=True,
        )
        self._loop_thread.start()

        self._presence_client = None
        self._shutter_client = None
        self._conn_ts = 0.0  # monotonic time of the last (re-)connection
        self._client_lock = threading.Lock()
        self._connect_clients()

        for k in OBSOLETE_KEYS:
            try:
                if self._store.get(k) is not None:
                    self._store.delete(k)
            except Exception as e:
                logger.warning("Failed to remove obsolete key '%s': %s", k, e)

        try:
            if self._store.get(K_MODE) == "away":
                self._store.put(K_MODE, "security")
        except Exception as e:
            logger.warning("Mode migration failed: %s", e)

    def on_deactivate(self):
        try:
            self._disconnect_clients()
        except Exception as e:
            logger.warning("Error during client teardown: %s", e)
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._loop_thread.join(timeout=5)
        except Exception as e:
            logger.warning("Error stopping loop: %s", e)

    # --- Loop / connection management --------------------------------------

    def _run_coro(self, coro):
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=30)

    def _connect_clients(self):
        async def _open():
            shutter = Client(SHUTTER_MCP_URL)
            presence = Client(PRESENCE_MCP_URL)
            await shutter.__aenter__()
            await presence.__aenter__()
            return shutter, presence

        with self._client_lock:
            shutter, presence = self._run_coro(_open())
            self._shutter_client = shutter
            self._presence_client = presence
            self._conn_ts = _time.monotonic()

    def _disconnect_clients(self):
        async def _close(c):
            if c is None:
                return
            try:
                await c.__aexit__(None, None, None)
            except Exception as e:
                logger.warning("Client close failed: %s", e)

        with self._client_lock:
            sc, pc = self._shutter_client, self._presence_client
            self._shutter_client = None
            self._presence_client = None
            self._conn_ts = 0.0
        if sc is not None:
            try:
                self._run_coro(_close(sc))
            except Exception as e:
                logger.warning("Shutter close failed: %s", e)
        if pc is not None:
            try:
                self._run_coro(_close(pc))
            except Exception as e:
                logger.warning("Presence close failed: %s", e)

    def _conn_expired(self) -> bool:
        if self._shutter_client is None or self._presence_client is None:
            return True
        return (_time.monotonic() - self._conn_ts) >= CONN_LIFETIME_S

    def _ensure_fresh_clients(self):
        """Renews the clients if their lifetime has expired."""
        if self._conn_expired():
            logger.info("Connection lifetime exceeded - rebuilding clients.")
            try:
                self._disconnect_clients()
            except Exception:
                pass
            self._connect_clients()

    def _call_with_retry(self, factory_coro_fn):
        try:
            return self._run_coro(factory_coro_fn())
        except Exception as first_err:
            logger.warning(
                "MCP call failed (%s: %s) - reconnecting and retrying once.",
                type(first_err).__name__, first_err,
            )
            try:
                self._disconnect_clients()
            except Exception:
                pass
            self._connect_clients()
            return self._run_coro(factory_coro_fn())

    # --- MCP helpers --------------------------------------------------------

    def _shutter_status(self) -> str:
        async def _do():
            r = await self._shutter_client.call_tool("get_system_status", {})
            return r.content[0].text if r.content else ""
        return self._call_with_retry(_do)

    def _shutter_set(self, position: int) -> None:
        async def _do():
            await self._shutter_client.call_tool(
                "set_position",
                {"name": SHUTTER_NAME, "position": int(position)},
            )
        self._call_with_retry(_do)

    def _presence_text(self) -> str:
        async def _do():
            r = await self._presence_client.call_tool("presence_overview", {})
            return r.content[0].text if r.content else ""
        return self._call_with_retry(_do)

    # --- Parsing ------------------------------------------------------------

    @staticmethod
    def _parse_office_position(status_text: str) -> int | None:
        m = re.search(r"office:\s*(\d+)\s*%", status_text)
        return int(m.group(1)) if m else None

    @staticmethod
    def _is_anyone_home(presence_text: str) -> bool:
        m = re.search(r"-\s*any:\s*(PRESENT|AWAY)", presence_text, re.IGNORECASE)
        if not m:
            return True
        return m.group(1).upper() == "PRESENT"

    # --- Store / EventLog helpers ------------------------------------------

    def _get_int(self, key):
        raw = self._store.get(key)
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def _log(self, topic: str, text: str) -> None:
        try:
            self._eventlog.log_event(topic, text)
        except Exception as e:
            logger.warning("EventLog write failed (%s: %s): %s", type(e).__name__, e, text)

    # --- Solar timing -------------------------------------------------------

    def _get_solar_times(self, day):
        s = sun(LOCATION.observer, date=day, tzinfo=TZ)

        def clamp(dt, lo, hi):
            low = datetime.combine(day, lo, tzinfo=TZ)
            high = datetime.combine(day, hi, tzinfo=TZ)
            return max(low, min(dt, high))

        return (
            clamp(s["sunrise"], OPEN_EARLIEST, OPEN_LATEST),
            clamp(s["sunset"], CLOSE_EARLIEST, CLOSE_LATEST),
        )

    @staticmethod
    def _get_due_event(now, open_dt, close_dt, today):
        if open_dt <= now < close_dt:
            return f"{today}:open", OPEN_POS, "Opening"
        if now >= close_dt:
            return f"{today}:close", CLOSE_POS, "Closing"
        return None

    @staticmethod
    def _get_next_slot_id(now, open_dt, close_dt, today):
        if now < open_dt:
            return f"{today}:open"
        if now < close_dt:
            return f"{today}:close"
        return f"{today + timedelta(days=1)}:open"

    # --- Mode transitions --------------------------------------------------

    def _enter_security(self, actual: int, prev_mode):
        self._store.put(K_MODE, "security")
        self._store.delete(K_OVERRIDE)
        self._store.put(K_PRE_SEC_POS, str(actual))
        last_event = self._store.get(K_LAST_EVENT) or ""
        self._store.put(K_PRE_SEC_LAST_EVENT, last_event)
        self._log(
            TOPIC_MODE,
            f"Switching to anti-burglary mode (absence detected). "
            f"Previous mode: {prev_mode or 'none'}. "
            f"Saved position {actual}%.",
        )
        return False

    def _exit_security(self, actual: int, prev_mode) -> bool:
        self._store.put(K_MODE, "normal")
        self._store.delete(K_NEXT_SEC_CHANGE)

        pre_pos = self._get_int(K_PRE_SEC_POS)
        pre_last_event = self._store.get(K_PRE_SEC_LAST_EVENT)
        current_last_event = self._store.get(K_LAST_EVENT) or ""

        self._store.delete(K_PRE_SEC_POS)
        self._store.delete(K_PRE_SEC_LAST_EVENT)

        def _finalize(reason: str):
            self._store.put(K_EXPECTED, str(actual))
            self._log(
                TOPIC_MODE,
                f"Switching to normal mode (presence detected). "
                f"Previous mode: {prev_mode or 'none'}. {reason}",
            )

        if pre_pos is None:
            _finalize("No restore (no snapshot).")
            return False

        if (pre_last_event or "") != current_last_event:
            _finalize(f"No restore - slot '{current_last_event}' has overridden.")
            return False

        now = datetime.now(TZ)
        today = now.date()
        open_dt, close_dt = self._get_solar_times(today)
        due = self._get_due_event(now, open_dt, close_dt, today)
        if due is not None:
            slot_id, target_pos, label = due
            if current_last_event != slot_id:
                _finalize(
                    f"No restore - {label} slot '{slot_id}' "
                    f"immediately due (target {target_pos}%)."
                )
                return False

        if abs(actual - pre_pos) <= TOLERANCE:
            _finalize(f"Position already at {actual}% (target {pre_pos}%).")
            return False

        try:
            self._shutter_set(pre_pos)
            self._store.put(K_EXPECTED, str(pre_pos))
            self._log(
                TOPIC_MODE,
                f"Switching to normal mode (presence detected). "
                f"Previous mode: {prev_mode or 'none'}. "
                f"Restore: {actual}% -> {pre_pos}%.",
            )
            return True
        except Exception as e:
            _finalize(
                f"Restore to {pre_pos}% failed: {type(e).__name__}: {e}."
            )
            return False

    # --- Mode logic ---------------------------------------------------------

    def on_execute(self) -> str:
        # Check client lifetime, renew if necessary.
        self._ensure_fresh_clients()

        try:
            presence_text = self._presence_text()
            status_text = self._shutter_status()
        except Exception as e:
            return f"MCP read failed: {type(e).__name__}: {e}"

        actual = self._parse_office_position(status_text)
        if actual is None:
            return f"Cannot parse shutter status: {status_text!r}"

        anyone_home = self._is_anyone_home(presence_text)
        prev_mode = self._store.get(K_MODE)

        if anyone_home:
            if prev_mode != "normal":
                moved = self._exit_security(actual, prev_mode)
                if moved:
                    return (
                        "[transition] Restore move in progress; "
                        "skipping normal-mode logic for this tick."
                    )
            return self._run_normal_mode(actual)

        if prev_mode != "security":
            self._enter_security(actual, prev_mode)
        return self._run_security_mode(actual)

    # --- Normal mode --------------------------------