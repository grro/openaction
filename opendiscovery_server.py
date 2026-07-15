import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from threading import Thread, Event
from time import sleep

from service_registry import ServiceRegistry, Service
from typing import Dict, List, Set, Any
from zeroconf import Zeroconf, ServiceBrowser, ServiceListener, ZeroconfServiceTypes, BadTypeInNameException

from mcp_server_base import McpServer
from simple_store import ScopedStore, SimpleStore


logger = logging.getLogger(__name__)



# Key under which the serialized service registry is stored in SimpleStore.
_STORE_KEY = "services"


@dataclass(frozen=True)
class MDNSService:
    """Snapshot of a single mDNS service as observed during a scan."""
    name: str
    discovered_at: str   # ISO-8601 timestamp, kept as string to stay JSON-serializable.
    host: str
    path: str
    port: int

    @property
    def last_seen(self) -> datetime:
        """Parsed ``discovered_at`` as a :class:`datetime` for age calculations."""
        return datetime.fromisoformat(self.discovered_at)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MDNSService":
        return cls(**data)


class MDNSRegistry:
    """
    Background mDNS browser that maintains a persistent set of discovered
    services.

    On construction, any services persisted by a previous run are restored
    from the supplied :class:`SimpleStore`. After :meth:`start`, a daemon
    thread re-scans the local network every ``SCAN_INTERVAL_SECONDS``
    seconds, refreshes the in-memory registry, persists it back to the
    store and evicts services that have not been seen for x days.
    """

    def __init__(self, store: SimpleStore, own_service_name: str = ""):
        """
        Args:
            store: Backing store used to persist the discovered services
                across restarts.
            own_service_name: Fully qualified mDNS name (e.g.
                ``"OpenAction._mcp._tcp.local."``) of this process,
                so we don't include ourselves in the discovered set.
                Pass an empty string to disable the self-filter.
        """
        self._is_running = False
        self._stop_event = Event()
        self._store = ScopedStore(store, "__mdns_registry__")
        self._own_service_name = own_service_name
        self._services: Dict[str, MDNSService] = {}
        self._restore_from_store()

    def _restore_from_store(self) -> None:
        """Load previously discovered services from the persistent store."""
        raw = self._store.get(_STORE_KEY, "{}")
        try:
            for name, service in json.loads(raw).items():
                self._services[name] = MDNSService.from_dict(service)
                logger.info(f"mDNS: Restoring previously discovered service '{name}'")
        except Exception as e:
            logger.warning(f"Failed to restore mDNS services from store: {e}")

    @property
    def services(self) -> List[MDNSService]:
        """Snapshot list of all currently known services."""
        return list(self._services.values())

    @property
    def names(self) -> Set[str]:
        """Set of fully qualified mDNS names currently known."""
        return set(self._services.keys())

    def start(self) -> None:
        """Start the background scan loop (no-op if already running)."""
        if not self._is_running:
            self._is_running = True
            self._stop_event.clear()
            Thread(target=self._loop, daemon=True, name="MDNSRegistry").start()

    def stop(self) -> None:
        """Request a graceful shutdown of the background scan loop."""
        self._is_running = False
        self._stop_event.set()

    def _loop(self) -> None:
        """Repeated scan / persist / cleanup cycle, interruptible via stop()."""
        while self._is_running:
            try:
                self._refresh(timeout_seconds=2.0)
                self._persist()
                self._clean_up(time_out_days=8)
            except Exception as e:
                logger.warning(f"Error in MDNSServiceRegistry loop: {e}")

            # `wait()` returns True iff stop() was called.
            if self._stop_event.wait(timeout=60):
                break

    def _persist(self) -> None:
        """Write the current registry contents back to the persistent store."""
        srvs = {service.name: service.to_dict() for service in self._services.values()}
        self._store.put(_STORE_KEY, json.dumps(srvs))

    def _clean_up(self, time_out_days: int) -> None:
        """Drop services that have not been seen within ``time_out_days`` days."""
        max_age = timedelta(days=time_out_days)
        now = datetime.now()
        expired = [name for name, svc in self._services.items() if (now - svc.last_seen) > max_age]
        for name in expired:
            del self._services[name]

    def _refresh(self, timeout_seconds: float):
        discovered = self.scan(timeout_seconds)
        for service in discovered.values():
            if service.name != self._own_service_name:
                if service.name not in self._services:
                    if '._FC9F5ED42C8A._tcp.local.' not in service.name: # ignore Nearby Share protocol
                        self._services[service.name] = service
                        logger.info(f"mDNS: Discovered '{service.name}' at {service.host}:{service.port}{service.path}")
        return discovered



    @staticmethod
    def scan(timeout_seconds: float) -> Dict[str, MDNSService]:
        """
        Perform one passive mDNS scan and return everything discovered.

        Note that the returned dict only contains services that responded
        *during this scan*; previously known services that did not
        respond are kept in :attr:`_services` and aged out by
        :meth:`_clean_up`.
        """
        discovered: Dict[str, MDNSService] = {}

        class _AnyListener(ServiceListener):
            def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
                self._process(zc, type_, name)

            def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
                self._process(zc, type_, name)

            def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
                pass

            def _process(self, zc: Zeroconf, type_: str, name: str) -> None:
                info = zc.get_service_info(type_, name)
                if not info or not info.parsed_addresses() or info.port is None:
                    return

                host = info.parsed_addresses()[0]
                path_raw = info.properties.get(b"path", b"/").decode("utf-8", errors="ignore")
                path = path_raw if path_raw.startswith("/") else f"/{path_raw}"

                discovered[name] = MDNSService(
                    name=name,
                    discovered_at=datetime.now().isoformat(),
                    host=host,
                    path=path,
                    port=info.port,
                )

        zc = Zeroconf()
        browsers: List[ServiceBrowser] = []
        try:
            all_types = ZeroconfServiceTypes.find(zc=zc)
            if all_types:
                listener = _AnyListener()
                for service_type in all_types:
                    if not service_type or not service_type.endswith(".local."):
                        logger.debug(f"Skipping malformed mDNS service type: {service_type!r}")
                        continue
                    try:
                        browsers.append(ServiceBrowser(zc, service_type, listener))
                    except BadTypeInNameException as e:
                        logger.debug(f"Skipping invalid mDNS service type {service_type!r}: {e}")

                # Allow services time to respond, but wake early on shutdown.
                sleep(timeout_seconds)

            return discovered

        except Exception as e:
            logger.error(f"mDNS scan failed: {e}", exc_info=True)
            return {}
        finally:
            for browser in browsers:
                try:
                    browser.cancel()
                except Exception:
                    pass
            zc.close()


class OpenDiscoveryServer(McpServer):
    """MCP server exposing both manually configured and mDNS-discovered services."""

    # mDNS service name used to identify this server itself, so it can be filtered out.
    _OWN_MDNS_NAME = "OpenAction._mcp._tcp.local."

    def __init__(self, name: str, port: int, dir: str, configs: Dict[str, Service], host: str = "0.0.0.0"):
        super().__init__(name, port, host)
        self.store = SimpleStore(name="state", directory=dir)
        self.manual_registry = ServiceRegistry(configs)
        self.mdns_registry = MDNSRegistry(self.store, own_service_name=self._OWN_MDNS_NAME)
        self.mdns_registry.start()

        @self.mcp.tool()
        def list_available_services() -> str:
            """
            Lists all manually configured services and locally discovered mDNS services.
            Use this to identify available hardware, endpoints, or network APIs.
            """
            try:
                report: List[str] = [
                    "### 📡 Available Services",
                    "=========================\n",
                    "#### 🛠️ Manually Configured Services",
                ]

                # --- 1. Manually Configured Services ---
                manual_services = getattr(self.manual_registry, "services", []) or []
                if not manual_services:
                    report.append("*No manually configured services found.*\n")
                else:
                    for svc in manual_services:
                        svc_name = getattr(svc, "name", "Unknown")
                        svc_type = str(getattr(svc, "type", "Unknown")).upper()
                        url = getattr(svc, "url", "Unknown URL")
                        report.append(f"• **{svc_name}** [{svc_type}]: `{url}`")
                    report.append("")  # Spacer

                # --- 2. mDNS Discovered Services ---
                report.append("#### 🔍 Discovered Local Services (mDNS)")
                mdns_services = self.mdns_registry.services
                if not mdns_services:
                    report.append("*No mDNS services currently discovered on the local network.*")
                else:
                    for svc in mdns_services:
                        time_str = svc.last_seen.strftime("%Y-%m-%d %H:%M:%S")
                        report.append(
                            f"• **{svc.name}** (`{svc.host}:{svc.port}{svc.path}`) "
                            f"| *last seen: {time_str}*"
                        )

                return "\n".join(report)

            except Exception as e:
                logger.error(f"Failed to list available services: {e}", exc_info=True)
                return f"Error: Could not retrieve services: {type(e).__name__} - {str(e)}"

    def stop(self):
        """Stop background registries before tearing down the MCP server."""
        super().stop()
