import asyncio
import logging
import random
import re
import json
import threading
import time as _time
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from fastmcp import Client

logger = logging.getLogger(__name__)

TZ = ZoneInfo("Europe/Berlin")
UTC = timezone.utc

# --- Anonymized URLs ---
LIGHT_MCP_URL = "http://<LIGHT_IP>:<PORT>/sse"
BRIGHTNESS_MCP_URL = "http://<BRIGHTNESS_IP>:<PORT>/sse"
PRESENCE_MCP_URL = "http://<PRESENCE_IP>:<PORT>/sse"
MOTION_MCP_URL = "http://<MOTION_IP>:<PORT>/sse"

MOTION_PINS = ("MotionLivingroom", "MotionCorridor")
MOTION_TIMEOUT_S = 8

BRIGHTNESS_PIN = "LightOutdoor"  # gpios input: true=bright, false=dark
BRIGHTNESS_TIMEOUT_S = 8
CONN_LIFETIME_S = 33 * 60

EVENING_START = time(15, 0)
EVENING_END = time(23, 50)
MOTION_CUTOFF = time(22, 30)
MOTION_TIMEOUT_MIN = 17

BRIGHT_OFF_DELAY_S = 10 * 60

SIM_START = time(18, 0)
SIM_END = time(23, 30)
SIM_ON_MIN_S, SIM_ON_MAX_S = 20 * 60, 90 * 60
SIM_OFF_MIN_S, SIM_OFF_MAX_S = 15 * 60, 60 * 60

K_LAST_DARK = "last_time_dark_iso"
K_SIM_STATE = "sim_state"
K_SIM_NEXT_CHANGE = "sim_next_change_iso"
K_LAST_LOGGED = "last_logged_state"

OBSOLETE_KEYS = ("last_motion_iso",)

TOPIC_LIGHT = "Light"
TOPIC_SIM = "Simulation"


class LightLivingroomAutomation(BackgroundTask):
    """
    Living room light - Clone of the OpenHAB light_livingroom rule.

    Brightness: brightnessOutside MCP provides the 'gpios' tool (not get_state);
    Returns {"inputs": {"LightOutdoor": bool}}. true=bright, false=dark.
    On read/connection error, conservative fallback is_bright=True (light stays off).

    Trigger: Minute cron AND motion push notification (message_handler without
    business logic, worker thread). on_execute is reentrancy-protected. Motion
    'last movement' via resource last_change, polling as fallback.

    EventLog relief: physical switching every minute (corrects external
    control), logging only on actual state change (K_LAST_LOGGED).

    Present & dark: before 22:30 permanently on; from 22:30 on if motion
    <17 min, otherwise off (without data: off). Absent & dark: Simulation
    (random cycles 18:00-23:30). Bright: off after 10 min. Outside 15:00-23:50: off.
    """

    @property
    def _store(self):
        return self.environment.store

    @property
    def _eventlog(self):
        return self.environment.eventlog

    # --- Lifecycle ----------------------------------------------------------

    def on_activate(self):
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._loop.run_forever, name="light-livingroom-loop", daemon=True
        )
        self._loop_thread.start()

        self._light_client = None
        self._presence_client = None
        self._stable_conn_ts = 0.0

        self._brightness_client = None
        self._brightness_conn_ts = 0.0

        self._motion_client = None
        self._motion_conn_ts = 0.0

        self._client_lock = threading.Lock()
        self._exec_lock = threading.Lock()
        self._shutting_down = False

        self._connect_stable_clients()
        self._connect_brightness()
        self._connect_motion()

        for k in OBSOLETE_KEYS:
            try:
                if self._store.get(k) is not None:
                    self._store.delete(k)
            except Exception as e:
                logger.warning("obsolete key cleanup failed '%s': %s", k, e)

    def on_deactivate(self):
        self._shutting_down = True
        for closer in (self._disconnect_stable_clients, self._close_brightness, self._close_motion):
            try:
                closer()
            except Exception as e:
                logger.warning("Teardown error: %s", e)
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._loop_thread.join(timeout=5)
        except Exception as e:
            logger.warning("Loop stop error: %s", e)

    def _run_coro(self, coro):
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=30)

    # --- Stable Clients -----------------------------------------------------

    def _connect_stable_clients(self):
        async def _open():
            light = Client(LIGHT_MCP_URL)
            presence = Client(PRESENCE_MCP_URL)
            await light.__aenter__()
            await presence.__aenter__()
            return light, presence

        with self._client_lock:
            light, presence = self._run_coro(_open())
            self._light_client = light
            self._presence_client = presence
            self._stable_conn_ts = _time.monotonic()

    def _disconnect_stable_clients(self):
        async def _close(c):
            if c is None:
                return
            try:
                await c.__aexit__(None, None, None)
            except Exception as e:
                logger.warning("Close failed: %s", e)

        with self._client_lock:
            clients = (self._light_client, self._presence_client)
            self._light_client = None
            self._presence_client = None
            self._stable_conn_ts = 0.0
        for c in clients:
            if c is not None:
                try:
                    self._run_coro(_close(c))
                except Exception as e:
                    logger.warning("Close error: %s", e)

    def _stable_expired(self) -> bool:
        if self._light_client is None or self._presence_client is None:
            return True
        return (_time.monotonic() - self._stable_conn_ts) >= CONN_LIFETIME_S

    def _ensure_fresh_stable(self):
        if self._stable_expired():
            try:
                self._disconnect_stable_clients()
            except Exception:
                pass
            self._connect_stable_clients()

    def _call_stable_with_retry(self, factory_coro_fn):
        try:
            return self._run_coro(factory_coro_fn())
        except Exception as first_err:
            logger.warning("Stable MCP call failed (%s) - reconnect+retry.", first_err)
            try:
                self._disconnect_stable_clients()
            except Exception:
                pass
            self._connect_stable_clients()
            return self._run_coro(factory_coro_fn())

    # --- brightnessOutside --------------------------------------------------

    def _connect_brightness(self):
        async def _open():
            async with asyncio.timeout(BRIGHTNESS_TIMEOUT_S):
                c = Client(BRIGHTNESS_MCP_URL)
                await c.__aenter__()
                return c
        try:
            c = self._run_coro(_open())
            with self._client_lock:
                self._brightness_client = c
                self._brightness_conn_ts = _time.monotonic()
            return True
        except Exception as e:
            logger.warning("brightness connect failed: %s", e)
            with self._client_lock:
                self._brightness_client = None
                self._brightness_conn_ts = 0.0
            return False

    def _close_brightness(self):
        async def _close(c):
            if c is None:
                return
            try:
                await c.__aexit__(None, None, None)
            except Exception as e:
                logger.warning("brightness close failed: %s", e)
        with self._client_lock:
            c = self._brightness_client
            self._brightness_client = None
            self._brightness_conn_ts = 0.0
        if c is not None:
            try:
                self._run_coro(_close(c))
            except Exception as e:
                logger.warning("brightness close error: %s", e)

    def _brightness_expired(self) -> bool:
        if self._brightness_client is None:
            return True
        return (_time.monotonic() - self._brightness_conn_ts) >= CONN_LIFETIME_S

    # --- Motion (persistent + lean handler) ---------------------------------

    def _motion_message_handler_factory(self):
        def _is_motion_update(message) -> bool:
            try:
                root = getattr(message, "root", message)
                method = getattr(root, "method", None)
                if method != "notifications/resources/updated":
                    return False
                params = getattr(root, "params", None)
                uri = str(getattr(params, "uri", "")) if params else ""
                return any(pin in uri for pin in MOTION_PINS)
            except Exception:
                return False

        async def handler(message):
            if self._shutting_down:
                return
            if _is_motion_update(message):
                t = threading.Thread(target=self._triggered_execute, daemon=True)
                t.start()
        return handler

    def _triggered_execute(self):
        if self._shutting_down:
            return
        try:
            result = self.on_execute()
            logger.info("motion-triggered on_execute: %s", result)
        except Exception as e:
            logger.warning("triggered on_execute failed: %s", e)

    async def _motion_initial_read(self, c):
        for pin in MOTION_PINS:
            try:
                await c.read_resource(f"sensor://gpio/{pin}")
            except Exception as e:
                logger.warning("motion initial read %s failed: %s", pin, e)

    def _connect_motion(self):
        if MOTION_MCP_URL is None:
            return False
        handler = self._motion_message_handler_factory()

        async def _open():
            async with asyncio.timeout(MOTION_TIMEOUT_S):
                c = Client(MOTION_MCP_URL, message_handler=handler)
                await c.__aenter__()
                return c
        try:
            c = self._run_coro(_open())
            with self._client_lock:
                self._motion_client = c
                self._motion_conn_ts = _time.monotonic()
            try:
                self._run_coro(self._motion_initial_read(c))
            except Exception as e:
                logger.warning("motion initial read failed: %s", e)
            return True
        except Exception as e:
            logger.warning("motion connect failed: %s", e)
            with self._client_lock:
                self._motion_client = None
                self._motion_conn_ts = 0.0
            return False

    def _close_motion(self):
        async def _close(c):
            if c is None:
                return
            try:
                await c.__aexit__(None, None, None)
            except Exception as e:
                logger.warning("motion close failed: %s", e)
        with self._client_lock:
            c = self._motion_client
            self._motion_client = None
            self._motion_conn_ts = 0.0
        if c is not None:
            try:
                self._run_coro(_close(c))
            except Exception as e:
                logger.warning("motion close error: %s", e)

    def _motion_expired(self) -> bool:
        if self._motion_client is None:
            return True
        return (_time.monotonic() - self._motion_conn_ts) >= CONN_LIFETIME_S

    def _ensure_fresh_motion(self):
        if self._motion_expired():
            self._close_motion()
            self._connect_motion()

    def _read_motion_last_change(self):
        async def _read_with(c):
            newest = None
            for pin in MOTION_PINS:
                try:
                    r = await c.read_resource(f"sensor://gpio/{pin}")
                    text = None
                    for item in r:
                        text = getattr(item, "text", None)
                        if text:
                            break
                    if not text:
                        continue
                    data = json.loads(text)
                    lc = self._parse_iso_utc(data.get("last_change"))
                    if lc is not None and (newest is None or lc > newest):
                        newest = lc
                except Exception as e:
                    logger.warning("motion read pin %s failed: %s", pin, e)
            return newest

        if self._motion_client is not None:
            try:
                async def _do():
                    async with asyncio.timeout(MOTION_TIMEOUT_S):
                        return await _read_with(self._motion_client)
                newest = self._run_coro(_do())
                if newest is not None:
                    return newest
            except Exception as e:
                logger.warning("motion persistent read failed (%s) - fallback poll.", e)
                self._close_motion()

        try:
            async def _do_fallback():
                async with asyncio.timeout(MOTION_TIMEOUT_S):
                    async with Client(MOTION_MCP_URL) as c:
                        return await _read_with(c)
            return self._run_coro(_do_fallback())
        except Exception as e:
            logger.warning("motion fallback poll failed: %s", e)
            return None

    def _motion_elapsed_minutes(self):
        if MOTION_MCP_URL is None:
            return None
        self._ensure_fresh_motion()
        newest = self._read_motion_last_change()
        if newest is None:
            return None
        return (datetime.now(UTC) - newest).total_seconds() / 60.0

    # --- Reads (stable) -----------------------------------------------------

    def _light_get(self) -> bool | None:
        async def _do():
            r = await self._light_client.call_tool("get_switch_state", {})
            return r.content[0].text if r.content else ""
        txt = self._call_stable_with_retry(_do)
        if txt is None:
            return None
        low = txt.lower()
        if "off" in low:
            return False
        if "on" in low or "true" in low:
            return True
        if "false" in low:
            return False
        return None

    def _light_set(self, on: bool) -> None:
        async def _do():
            await self._light_client.call_tool("set_switch_state", {"on": bool(on)})
        self._call_stable_with_retry(_do)

    def _is_anyone_home(self) -> bool:
        async def _do():
            r = await self._presence_client.call_tool("presence_overview", {})
            return r.content[0].text if r.content else ""
        txt = self._call_stable_with_retry(_do)
        m = re.search(r"-\s*any:\s*(PRESENT|AWAY)", txt or "", re.IGNORECASE)
        if not m:
            return True
        return m.group(1).upper() == "PRESENT"

    def _is_bright(self) -> bool:
        """
        true=bright. Uses the gpios tool: {"inputs": {"LightOutdoor": bool}}.
        Persistent client with 33-min lifetime + retry. On error conservative
        True (light stays off instead of turning on falsely).
        """
        if self._brightness_expired():
            self._close_brightness()
            self._connect_brightness()

        def _attempt():
            async def _do():
                async with asyncio.timeout(BRIGHTNESS_TIMEOUT_S):
                    r = await self._brightness_client.call_tool("gpios", {})
                    return r.content[0].text if r.content else ""
            return self._run_coro(_do())

        txt = None
        if self._brightness_client is not None:
            try:
                txt = _attempt()
            except Exception as e:
                logger.warning("brightness read failed (%s) - reconnect+retry.", e)
                self._close_brightness()
                if self._connect_brightness():
                    try:
                        txt = _attempt()
                    except Exception as e2:
                        logger.warning("brightness retry failed: %s", e2)
                        txt = None
        else:
            if self._connect_brightness():
                try:
                    txt = _attempt()
                except Exception as e:
                    logger.warning("brightness read after connect failed: %s", e)
                    txt = None

        if not txt:
            return True
        try:
            data = json.loads(txt)
            val = data.get("inputs", {}).get(BRIGHTNESS_PIN, None)
            if val is None:
                return True
            return bool(val)
        except Exception as e:
            logger.warning("brightness parse failed (%s): %r", e, txt[:200])
            return True

    # --- Helpers ------------------------------------------------------------

    @staticmethod
    def _parse_iso_utc(s):
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s)
        except (ValueError, TypeError):
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt

    def _log(self, topic: str, text: str) -> None:
        try:
            self._eventlog.log_event(topic, text)
        except Exception as e:
            logger.warning("EventLog failed (%s): %s", e, text)

    def _clear_sim_state(self):
        for k in (K_SIM_STATE, K_SIM_NEXT_CHANGE):
            try:
                if self._store.get(k) is not None:
                    self._store.delete(k)
            except Exception:
                pass

    # --- Main ---------------------------------------------------------------

    def on_execute(self) -> str:
        if not self._exec_lock.acquire(blocking=False):
            return "[skip] on_execute already running (concurrent trigger)."
        try:
            return self._on_execute_locked()
        finally:
            self._exec_lock.release()

    def _on_execute_locked(self) -> str:
        self._ensure_fresh_stable()

        now = datetime.now(TZ)
        t = now.time()

        try:
            anyone_home = self._is_anyone_home()
        except Exception as e:
            return f"presence read failed: {type(e).__name__}: {e}"

        is_bright = self._is_bright()

        in_main_window = EVENING_START < t < EVENING_END

        if not in_main_window:
            self._clear_sim_state()
            return self._ensure_off("light period expired")

        if is_bright:
            self._clear_sim_state()
            last_dark_raw = self._store.get(K_LAST_DARK)
            if last_dark_raw:
                try:
                    last_dark = datetime.fromisoformat(last_dark_raw)
                    if (now - last_dark).total_seconds() > BRIGHT_OFF_DELAY_S:
                        return self._ensure_off("bright outside")
                    secs = int((now - last_dark).total_seconds())
                    return f"[light] bright, waiting delay ({secs}s/{BRIGHT_OFF_DELAY_S}s)."
                except ValueError:
                    pass
            return self._ensure_off("bright outside (no last_dark)")

        # --- DARK ---
        self._store.put(K_LAST_DARK, now.isoformat())

        if not anyone_home:
            return self._run_simulation(now)

        if t < MOTION_CUTOFF:
            self._clear_sim_state()
            return self._ensure_on("dark & onsite (before 22:30)")

        self._clear_sim_state()
        elapsed_motion = self._motion_elapsed_minutes()
        if elapsed_motion is None:
            return self._ensure_off("after 22:30, no motion data")
        if elapsed_motion < MOTION_TIMEOUT_MIN:
            return self._ensure_on(f"after 22:30, motion {elapsed_motion:.1f} min ago")
        return self._ensure_off(
            f"after 22:30, last motion {elapsed_motion:.1f} min ago (>{MOTION_TIMEOUT_MIN})"
        )

    # --- Actions (with EventLog reduction) ----------------------------------

    def _apply_state(self, target_on: bool, reason: str, sim: bool = False) -> str:
        want = "on" if target_on else "off"
        cur = self._light_get()
        physically_changed = False

        if cur is not target_on:
            try:
                self._light_set(target_on)
                physically_changed = True
            except Exception as e:
                return f"[light] {'ON' if target_on else 'OFF'} failed: {type(e).__name__}: {e}"

        last_logged = self._store.get(K_LAST_LOGGED)
        if last_logged != want:
            verb = "ON" if target_on else "OFF"
            topic = TOPIC_SIM if sim else TOPIC_LIGHT
            self._log(topic, f"{verb}: {reason}.")
            self._store.put(K_LAST_LOGGED, want)

        if physically_changed:
            return f"[light] set {'ON' if target_on else 'OFF'} ({reason})."
        return f"[light] already {'ON' if target_on else 'OFF'} ({reason})."

    def _ensure_on(self, reason: str) -> str:
        return self._apply_state(True, reason)

    def _ensure_off(self, reason: str) -> str:
        return self._apply_state(False, reason)

    def _run_simulation(self, now: datetime) -> str:
        t = now.time()
        if not (SIM_START <= t < SIM_END):
            self._clear_sim_state()
            return self._ensure_off("simulation window closed (outside 18:00-23:30)")

        next_iso = self._store.get(K_SIM_NEXT_CHANGE)
        sim_state = self._store.get(K_SIM_STATE)

        if next_iso:
            try:
                next_change = datetime.fromisoformat(next_iso)
            except ValueError:
                next_change = None
        else:
            next_change = None

        if sim_state is None or next_change is None or now >= next_change:
            new_state = "off" if sim_state == "on" else "on"
            target_on = (new_state == "on")
            if target_on:
                dur = random.randint(SIM_ON_MIN_S, SIM_ON_MAX_S)
            else:
                dur = random.randint(SIM_OFF_MIN_S, SIM_OFF_MAX_S)
            new_next = now + timedelta(seconds=dur)
            self._store.put(K_SIM_STATE, new_state)
            self._store.put(K_SIM_NEXT_CHANGE, new_next.isoformat())
            verb = "ON" if target_on else "OFF"
            res = self._apply_state(
                target_on,
                f"Simulation {verb} for {dur // 60} min (until {new_next.strftime('%H:%M')})",
                sim=True,
            )
            return f"[sim] {verb} for {dur // 60} min (next {new_next.strftime('%H:%M:%S')}). {res}"

        target_on = (sim_state == "on")
        self._apply_state(target_on, "simulation hold", sim=True)
        remaining = int((next_change - now).total_seconds())
        return (
            f"[sim] holding {sim_state.upper()}, "
            f"next change in {remaining // 60} min "
            f"({next_change.strftime('%H:%M:%S')})."
        )