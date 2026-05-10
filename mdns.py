import json
import logging
import socket
from dataclasses import dataclass, asdict
from datetime import datetime
from threading import Thread
from typing import Dict, Optional, List, Set, Any
from time import sleep
from zeroconf import IPVersion, ServiceInfo, Zeroconf, ServiceBrowser, ServiceListener, ZeroconfServiceTypes

from api.store import Store
from store_impl import ScopedStore, SimpleStore

logger = logging.getLogger(__name__)



class MDNS:
    def __init__(self):
        self.registered: Dict[str, ServiceInfo] = dict()
        self.zc = Zeroconf(ip_version=IPVersion.V4Only)
        self.service_type = "_mcp._tcp.local."
        self.hostname = socket.gethostname()

        # Determine local IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            self.local_ip = s.getsockname()[0]
        except Exception:
            self.local_ip = "127.0.0.1"
        finally:
            s.close()

    def register_mdns(self, name: str, port: int):
        """Registers a service via mDNS."""
        try:
            service_name = f"{name}.{self.service_type}"
            service_info = ServiceInfo(
                type_=self.service_type,
                name=service_name,
                addresses=[socket.inet_aton(self.local_ip)],
                port=port,
                properties={
                    "version": "1.0",
                    "path": "/sse",
                    "server_type": "fastmcp"
                },
                server=f"{self.hostname}.local.",
            )

            logger.info(f"mDNS: Registering {service_name} at {self.local_ip}:{port}")
            self.zc.register_service(service_info)
            self.registered[name] = service_info
        except Exception as e:
            logger.error(f"mDNS Registration failed: {e}")

    def unregister_mdns(self, name: str):
        """Unregisters a specific service without closing the Zeroconf instance."""
        service_info = self.registered.pop(name, None)
        if service_info:
            logger.info(f"mDNS: Unregistering service {name}...")
            self.zc.unregister_service(service_info)

    def shutdown(self):
        """Completely shut down mDNS and close the socket."""
        self.zc.unregister_all_services()
        self.zc.close()



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
            except Exception as e:
                logger.warning(f"Error in MDNSServiceRegistry loop: {e}")
            sleep(60)

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