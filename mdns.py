import logging
import socket
from dataclasses import dataclass
from typing import Dict, Optional, Set
from time import sleep
from urllib.parse import urlparse

from zeroconf import IPVersion, ServiceInfo, Zeroconf, ServiceBrowser, ServiceListener

logger = logging.getLogger(__name__)




class MDNS:
    def __init__(self):
        self.registered: Dict[str, ServiceInfo] = dict()
        self.zc = Zeroconf(ip_version=IPVersion.V4Only)
        self.service_type = "_mcp._tcp.local."
        self.hostname = socket.gethostname()
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            self.local_ip = s.getsockname()[0]
        finally:
            s.close()

    def register_mdns(self, name: str, port: int):
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

            logging.info(f"mDNS: Registering {service_name} at {self.local_ip}:{port}")
            self.zc.register_service(service_info)
            self.registered[name] = service_info
        except Exception as e:
            logging.error(f"mDNS Registration failed: {e}")

    def unregister_mdns(self, name: str):
        service_info = self.registered.get(name)
        if service_info is not None:
            logging.info("mDNS: Unregistering service...")
            self.zc.unregister_service(service_info)
            self.zc.close()




@dataclass(frozen=True)
class DiscoveredService:
    name: str
    url: str


class MDNSScanner:
    """
    A utility class for discovering Model Context Protocol (MCP) servers
    on the local network using mDNS (Zeroconf).
    """

    def scan(self, service_type: str, timeout_seconds: float = 1.5) -> Set[DiscoveredService]:
        """
        Performs a brief, synchronous scan for mDNS services matching the MCP type.
        """
        logger.debug(f"Starting mDNS scan for {timeout_seconds} seconds...")
        discovered: Set[DiscoveredService] = set()

        class MCPListener(ServiceListener):
            def __init__(self, parent_scanner):
                self.scanner = parent_scanner

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
                path_raw = info.properties.get(b"path", b"").decode("utf-8", errors="ignore")
                path = path_raw if path_raw.startswith("/") else f"/{path_raw}"

                service_name = name.replace(f".{type_}", "").strip()
                url = self._build_discovered_url(host, info.port, path)

                if self._is_valid_url(url):
                    discovered.add(DiscoveredService(service_name, url))
                else:
                    logger.warning(f"Skipping discovered service with invalid URL: {service_name} -> {url}")

            def _is_valid_url(self, url: str) -> bool:
                parsed = urlparse(url)
                return parsed.scheme in ("http", "https") and bool(parsed.netloc)

            def _build_discovered_url(self, host: str, port: int, path: str) -> str:
                safe_host = host
                if ":" in host and not host.startswith("["):
                    safe_host = f"[{host}]"
                normalized_path = path if path.startswith("/") else f"/{path}"
                return f"http://{safe_host}:{port}{normalized_path}"

        zc: Optional[Zeroconf] = None
        try:
            zc = Zeroconf()
            browser = ServiceBrowser(zc, service_type, MCPListener(self))
            sleep(timeout_seconds)
            browser.cancel()
            logger.debug(f"mDNS scan finished. Found {len(discovered)} services.")
            return discovered

        except Exception as e:
            logger.error(f"mDNS scan failed completely: {e}", exc_info=True)
            return {}
        finally:
            if zc is not None:
                zc.close()
