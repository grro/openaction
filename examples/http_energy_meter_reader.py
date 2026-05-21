
import httpx
import logging
from datetime import datetime
from zoneinfo import ZoneInfo


logger = logging.getLogger(__name__)

# --- Configuration ----------------------------------------------------------
SHELLY_BASE = "http://10.1.33.54"
STATUS_ENDPOINT = "/rpc/Shelly.GetStatus"
TZ = ZoneInfo("Europe/Berlin")
HTTP_TIMEOUT = 4.0

# Persistent store keys
K_LAST_POWER = "last_total_act_power_w"
K_LAST_ENERGY = "last_total_act_energy_wh"
K_LAST_TS = "last_read_ts"


class ShellyEnergyMeterReader(BackgroundTask):
    """Periodic reader for the Shelly Pro 3EM three-phase energy meter.

    Cron ('* * * * *') triggers on_execute every minute. The httpx client
    session is built once in on_activate and reused for every call —
    avoiding per-call connection setup and benefiting from HTTP keep-alive.
    """

    def __init__(self, store: "Store") -> None:
        super().__init__(store)
        self._client: httpx.Client | None = None

    # --- Lifecycle ----------------------------------------------------------

    def on_activate(self) -> None:
        # One persistent client with keep-alive for the lifetime of the task.
        self._client = httpx.Client(
            base_url=SHELLY_BASE,
            timeout=HTTP_TIMEOUT,
            headers={"Accept": "application/json"},
        )

    def on_deactivate(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def _ensure_client(self) -> httpx.Client:
        # Safety net if on_activate was skipped (e.g. in some test contexts).
        if self._client is None:
            self._client = httpx.Client(
                base_url=SHELLY_BASE,
                timeout=HTTP_TIMEOUT,
                headers={"Accept": "application/json"},
            )
        return self._client

    # --- Business logic -----------------------------------------------------

    def on_execute(self) -> str:
        client = self._ensure_client()
        try:
            r = client.get(STATUS_ENDPOINT)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.exception("shelly read failed")
            return f"Shelly read failed: {type(e).__name__}: {e}"

        em = data.get("em:0") or {}
        emdata = data.get("emdata:0") or {}

        total_power = em.get("total_act_power")
        a_power = em.get("a_act_power")
        b_power = em.get("b_act_power")
        c_power = em.get("c_act_power")
        total_current = em.get("total_current")
        total_energy = emdata.get("total_act")          # cumulative Wh consumed
        total_energy_ret = emdata.get("total_act_ret")  # cumulative Wh exported

        if total_power is None:
            return f"Shelly response missing 'em:0.total_act_power': {data!r}"

        now = datetime.now(TZ).isoformat(timespec="seconds")

        # Persist latest reading for downstream tasks / debugging.
        self.store.put(K_LAST_POWER, f"{total_power:.3f}")
        if total_energy is not None:
            self.store.put(K_LAST_ENERGY, f"{total_energy:.2f}")
        self.store.put(K_LAST_TS, now)

        parts = [f"Shelly Pro 3EM @ {now}"]
        parts.append(f"  Total: {total_power:+.1f} W")
        if a_power is not None and b_power is not None and c_power is not None:
            parts.append(
                f"  Phases: A={a_power:+.1f} W  B={b_power:+.1f} W  C={c_power:+.1f} W"
            )
        if total_current is not None:
            parts.append(f"  Total current: {total_current:.3f} A")
        if total_energy is not None:
            parts.append(
                f"  Cumulative: {total_energy/1000:.2f} kWh consumed"
                + (f", {total_energy_ret/1000:.2f} kWh exported" if total_energy_ret is not None else "")
            )
        return "\n".join(parts)
