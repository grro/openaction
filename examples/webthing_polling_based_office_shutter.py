import httpx
import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from astral import LocationInfo
from astral.sun import sun

SHUTTER_URL = "http://192.168.1.99:8320/0/properties/position"
TZ = ZoneInfo("Europe/Berlin")
LOCATION = LocationInfo("Neustadt", "DE", "Europe/Berlin", 49.3508, 8.1395)

OPEN_EARLIEST, OPEN_LATEST = time(6, 0), time(9, 0)
CLOSE_EARLIEST, CLOSE_LATEST = time(16, 0), time(22, 0)
OPEN_POS, CLOSE_POS = 0, 100
TOLERANCE = 5

K_EXPECTED = "expected_position"
K_LAST_EVENT = "last_auto_event"
K_OVERRIDE = "override_until_event"

logger = logging.getLogger(__name__)


class OfficeShutterSunAutomation(BackgroundTask):
    """
    Automates office shutters based on solar times (sunrise/sunset) with
    clamping logic and manual override detection.

    Store contract: ScopedStore only exposes put/get/delete/keys with str values.
    All numeric state is serialized as str and parsed via _get_int().
    """

    def __init__(self, store):
        super().__init__(store)
        self._client = None

    def on_activate(self):
        self._ensure_client()

    def on_deactivate(self):
        if self._client:
            self._client.close()
            self._client = None

    def _ensure_client(self):
        """Lazy init guard: ensures the httpx client exists even if on_activate
        was skipped (e.g. in some test/eval contexts)."""
        if self._client is None:
            self._client = httpx.Client(timeout=4.0)

    def _get_int(self, key):
        raw = self.store.get(key)
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def on_execute(self):
        self._ensure_client()
        now = datetime.now(TZ)
        today = now.date()

        open_dt, close_dt = self._get_solar_times(today)

        try:
            actual = self._get_actual_position()
        except Exception as e:
            return f"Sensor Error: {e}"

        expected = self._get_int(K_EXPECTED)
        override_slot = self.store.get(K_OVERRIDE)

        # Manual override detection
        if expected is not None and abs(actual - expected) > TOLERANCE:
            next_slot = self._get_next_slot_id(now, open_dt, close_dt, today)
            if override_slot != next_slot:
                self.store.put(K_OVERRIDE, next_slot)
                self.store.put(K_EXPECTED, str(actual))
                return f"Manual move detected ({actual}%). Overriding until {next_slot}."

        due = self._get_due_event(now, open_dt, close_dt, today)
        if not due:
            return f"Idle. Position: {actual}%. Next event pending."

        slot_id, target_pos, label = due

        if self.store.get(K_LAST_EVENT) == slot_id:
            return f"Slot {slot_id} already handled."

        if override_slot == slot_id:
            self.store.delete(K_OVERRIDE)
            self.store.put(K_LAST_EVENT, slot_id)
            self.store.put(K_EXPECTED, str(actual))
            return f"Override expired at {slot_id}. Resuming auto next cycle."

        if override_slot:
            return f"Auto-event {label} skipped due to active override."

        try:
            self._set_position(target_pos)
            self.store.put(K_EXPECTED, str(target_pos))
            self.store.put(K_LAST_EVENT, slot_id)
            return f"Success: Moved to {label} ({target_pos}%)"
        except Exception as e:
            return f"Movement failed: {e}"

    def _get_solar_times(self, day):
        s = sun(LOCATION.observer, date=day, tzinfo=TZ)

        def clamp(dt, start, end):
            low = datetime.combine(day, start, tzinfo=TZ)
            high = datetime.combine(day, end, tzinfo=TZ)
            return max(low, min(dt, high))

        return clamp(s["sunrise"], OPEN_EARLIEST, OPEN_LATEST), \
            clamp(s["sunset"], CLOSE_EARLIEST, CLOSE_LATEST)

    def _get_due_event(self, now, open_dt, close_dt, today):
        if open_dt <= now < close_dt:
            return f"{today}:open", OPEN_POS, "Opening"
        if now >= close_dt:
            return f"{today}:close", CLOSE_POS, "Closing"
        return None

    def _get_next_slot_id(self, now, open_dt, close_dt, today):
        if now < open_dt: return f"{today}:open"
        if now < close_dt: return f"{today}:close"
        return f"{today + timedelta(days=1)}:open"

    def _get_actual_position(self):
        r = self._client.get(SHUTTER_URL)
        r.raise_for_status()
        return int(r.json()["position"])

    def _set_position(self, pos):
        r = self._client.put(SHUTTER_URL, json={"position": int(pos)})
        r.raise_for_status()
