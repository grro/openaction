import logging
import requests
from time import sleep
from dataclasses import dataclass
from threading import Thread
from typing import Dict, Optional
from urllib.parse import urlparse, urlunparse
from zeroconf import ServiceBrowser, ServiceListener, Zeroconf

logger = logging.getLogger(__name__)


MCP_SSE = 'MCP_SSE'
SHELLY = 'SHELLY'


@dataclass(frozen=True)
class ServiceConfig:
    name: str
    type: str
    url: str
    auto_scanned: bool

    @staticmethod
    def read(conf: str):
        try:
            if conf is not None:
                id, url = conf.split('=', 1)
                type, name = id.split(':', 1)
                return ServiceConfig(name.strip(), type.strip().upper(), url.strip(), False)
        except Exception as e:
            logger.warning(f"Failed to parse service config entry '{conf}': {e}")
        return None



class Configs:

    @staticmethod
    def read(servers: str) -> Dict[str, ServiceConfig]:
        if not servers:
            return {}
        else:
            srvs = [ServiceConfig.read(entry) for entry in servers.split('&') if len(entry.strip()) > 0]
            configs = {srv.name: srv for srv in srvs if srv is not None}
            logger.info(f"Loaded {len(configs)} static service configurations.")
            return configs




class ServiceRegistry:

    def __init__(self, configs: Dict[str, ServiceConfig], autoscan: bool):
        self._is_running = True
        self.autoscan = autoscan
        self.registered_services = {name: config for name, config in configs.items()}
        self.reachable_services = set()
        self._listeners = set()

        logger.info(f"ServiceRegistry initialized. Autoscan is {'ON' if autoscan else 'OFF'}.")

        Thread(target=self.__loop, daemon=True).start()

    def add_listener(self, listener):
        self._listeners.add(listener)

    def remove_listener(self, listener):
        self._listeners.discard(listener)

    def _notify_listeners(self):
        logger.debug("Notifying listeners about registry updates.")
        for listener in set(self._listeners):
            listener()

    def stop(self):
        logger.info("Stopping ServiceRegistry background loop.")
        self._is_running = False

    def __loop(self):
        self._is_running = True
        while self._is_running:
            updates = False
            if self.autoscan:
                updates = updates or self._autoscan()
            updates = updates or self._update_reachability()

            if updates:
                self._notify_listeners()
            sleep(60)

    def _autoscan(self) -> bool:
        updates = False
        discovered = MDNSScanner().scan()
        for name, conf in discovered.items():
            if name not in self.registered_services and name.upper() != 'OPENACTION':
                logger.info(f"🔍 Auto-discovered new service: '{name}' [{conf.type}] at {conf.url}")
                self.registered_services[name] = conf
                updates = True
        return updates

    def _update_reachability(self) -> bool:
        updates = False
        for name, config in list(self.registered_services.items()):
            if self._is_reachable(config.url):
                if name not in self.reachable_services:
                    logger.info(f"🟢 Service '{name}' is now ONLINE and reachable.")
                    self.reachable_services.add(name)
                    updates = True
            else:
                if name in self.reachable_services:
                    logger.warning(f"🔴 Service '{name}' is now OFFLINE (unreachable).")
                    self.reachable_services.discard(name)
                    updates = True
        return updates

    def _is_reachable(self, url: str, timeout: float = 2.0) -> bool:
        try:
            # We use GET with stream=True so we don't download the body (crucial for SSE).
            # Even if it returns 405 (Method Not Allowed), it means the server is up.
            response = requests.get(url, timeout=timeout, stream=True)
            return response.status_code < 500
        except Exception as e:
            logger.debug(f"Reachability check failed for {url}: {type(e).__name__}")
        return False






class MDNSScanner:
    """
    A utility class for discovering Model Context Protocol (MCP) servers
    on the local network using mDNS (Zeroconf).
    """

    def scan(self, timeout_seconds: float = 1.5) -> Dict[str, ServiceConfig]:
        """
        Performs a brief, synchronous scan for mDNS services matching the MCP type.
        """
        logger.debug(f"Starting mDNS scan for {timeout_seconds} seconds...")
        service_type = "_mcp._tcp.local."
        discovered: Dict[str, ServiceConfig] = {}

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
                path_raw = info.properties.get(b"path", b"/sse").decode("utf-8", errors="ignore")
                path = path_raw if path_raw.startswith("/") else f"/{path_raw}"

                service_name = name.replace(f".{type_}", "").strip()
                url = self._build_discovered_url(host, info.port, path)

                if self._is_valid_mcp_url(url):
                    discovered[service_name] = ServiceConfig(name, MCP_SSE, url, True)
                else:
                    logger.warning(f"Skipping discovered service with invalid URL: {service_name} -> {url}")

            def _is_valid_mcp_url(self, url: str) -> bool:
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