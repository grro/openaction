import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from threading import Thread
from time import sleep
from config import ServiceRegistry, Service
from typing import Dict, List, Set, Any
from zeroconf import Zeroconf, ServiceBrowser, ServiceListener, ZeroconfServiceTypes

from mcp_server import McpServer
from store_impl import ScopedStore, SimpleStore




logger = logging.getLogger(__name__)





@dataclass(frozen=True)
class MDNSService:
    name: str
    discovered_at: str
    host: str
    path: str
    port: int

    @property
    def last_seen(self) -> datetime:
        return datetime.fromisoformat(self.discovered_at)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MDNSService":
        return cls(**data)



class MDNSRegistry:
    def __init__(self, store: SimpleStore):
        self.is_running = False
        self._store = ScopedStore(store, "__mdns_registry__")
        self._services = dict()
        try:
            for name, service in json.loads(self._store.get("services", dict())).items():
                self._services[name] = MDNSService.from_dict(service)
                logger.info(f"mDNS: Restoring previously discovered service '{service}'")
        except Exception as e:
            logger.warning(f"Failed to restore mDNS services from store: {e}")


    @property
    def services(self) -> List[MDNSService]:
        return list(self._services.values())

    @property
    def names(self) -> Set[str]:
        return set(self._services.keys())

    def start(self):
        if not self.is_running:
            self.is_running = True
            Thread(target=self._loop, daemon=True).start()

    def stop(self):
        self.is_running = False

    def _loop(self):
        while self.is_running:
            try:
                discovered = self._scan(timeout_seconds=2.0)
                for service in discovered.values():
                    self._services[service.name] = service

                srvs = {service.name: service.to_dict() for service in self._services.values()}
                self._store.put("services", json.dumps(srvs))

                self._clean_up()
            except Exception as e:
                logger.warning(f"Error in MDNSServiceRegistry loop: {e}")
            sleep(60)


    def _clean_up(self, time_out_days = 30):
        now = datetime.now()
        expired = [name for name, service in self._services.items() if (now - service.last_seen).total_seconds() > (time_out_days * 24*60*60*60)]
        for name in expired:
            logger.info(f"mDNS: Removing expired service '{name}'")
            del self._services[name]


    def _scan(self, timeout_seconds: float) -> Dict[str, MDNSService]:
        discovered: Dict[str, MDNSService] = dict()

        class AnyListener(ServiceListener):

            def __init__(self, registry: MDNSRegistry):
                self.registry = registry

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

                if name != "OpenAction._mcp._tcp.local.":
                    discovered[name] = MDNSService(name=name, discovered_at=datetime.now().isoformat(), host=host, path=path, port=info.port)
                    if name not in self.registry.names:
                        logger.info(f"mDNS: Discovered '{name}' at {host}:{info.port}{path}")


        zc = Zeroconf()
        try:
            all_types = ZeroconfServiceTypes.find(zc=zc)
            if len(all_types) > 0:
                browsers = []
                for service_type in all_types:
                    browsers.append(ServiceBrowser(zc, service_type, AnyListener(self)))

                sleep(timeout_seconds)

                for browser in browsers:
                    browser.cancel()

            return discovered

        except Exception as e:
            logger.error(f"mDNS scan failed: {e}", exc_info=True)
            return []
        finally:
            zc.close()



class OpenDiscoveryServer(McpServer):

    def __init__(self, name: str, port: int, dir: str, configs: Dict[str, Service], host: str = "0.0.0.0"):
        super().__init__(name, port, host)
        self.store = SimpleStore(name="state", directory=dir)
        self.mdns_registry = MDNSRegistry(self.store)
        self.manual_registry = ServiceRegistry(configs)
        self.manual_registry.start()
        self.mdns_registry.start()


        @self.mcp.tool()
        def list_available_services() -> str:
            """
            Lists all manually configured services and locally discovered mDNS services.
            Use this to identify available hardware, endpoints, or network APIs.
            """
            try:
                report = [
                    "### 📡 Available Services",
                    "=========================\n"
                ]

                # --- 1. Manually Configured Services ---
                report.append("#### 🛠️ Manually Configured Services")
                manual_services = getattr(self.manual_registry, 'services', [])

                if not manual_services:
                    report.append("*No manually configured services found.*\n")
                else:
                    for svc in manual_services:
                        name = getattr(svc, 'name', 'Unknown')
                        svc_type = getattr(svc, 'type', 'Unknown').upper()
                        url = getattr(svc, 'url', 'Unknown URL')
                        report.append(f"• **{name}** [{svc_type}]: `{url}`")
                    report.append("\n") # Spacer

                # --- 2. mDNS Discovered Services ---
                report.append("#### 🔍 Discovered Local Services (mDNS)")
                mdns_services = getattr(self.mdns_registry, 'services', {})

                if not mdns_services:
                    report.append("*No mDNS services currently discovered on the local network.*")
                else:
                    # Normalize iteration if it's a dictionary
                    iterator = mdns_services.values() if isinstance(mdns_services, dict) else mdns_services

                    for svc in iterator:
                        name = svc.name
                        port = svc.port
                        server = svc.host

                        # Extract and format the last_seen timestamp
                        last_seen = svc.last_seen
                        time_str = last_seen.strftime("%Y-%m-%d %H:%M:%S")

                        # Extract and safely decode byte-encoded properties common in Zeroconf
                        props = getattr(svc, 'properties', {})
                        props_str = ""
                        if props:
                            safe_props = {}
                            for k, v in props.items():
                                safe_k = k.decode('utf-8') if isinstance(k, bytes) else str(k)
                                safe_v = v.decode('utf-8') if isinstance(v, bytes) else str(v)
                                safe_props[safe_k] = safe_v
                            props_str = f"\n  - *Properties:* `{safe_props}`"

                        # Append the formatted service entry
                        report.append(f"• **{name}** (`{server}:{port}`) | *last seen: {time_str}*{props_str}")

                return "\n".join(report)

            except Exception as e:
                logger.error(f"Failed to list available services: {e}", exc_info=True)
                return f"Error: Could not retrieve services: {type(e).__name__} - {str(e)}"


    def stop(self):
        self.manual_registry.stop()
        self.mdns_registry.stop()
        super().stop()

