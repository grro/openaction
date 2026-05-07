import logging
import requests
from abc import ABC, abstractmethod
from time import sleep
from dataclasses import dataclass, field
from threading import Thread
from typing import Dict, Set, List
from mdns import MDNSScanner


logger = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
class ServiceConfig:
    name: str
    type: str
    url: str = field(compare=False)
    auto_scanned: bool = field(compare=False)

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




class Scanner(ABC):

    @abstractmethod
    def scan(self) -> Set[ServiceConfig]:
        pass



class ServiceRegistry:

    def __init__(self, manual_configs: Dict[str, ServiceConfig], scanner: List[Scanner]):
        self._is_running = True
        self._listeners = set()
        self.scanner: Dict[Scanner, Dict[str, ServiceConfig]] = {s: dict() for s in scanner}
        self.reachable_services = set()
        self.manual_configs = manual_configs

        if len(manual_configs) > 0:
            logger.info("manual configs")
            for name, config in self.manual_configs.items():
                logger.info(f"  - {name} [{config.type}]: {config.url}")

        Thread(target=self.__loop, daemon=True).start()


    @property
    def registered_services(self) -> Dict[str, ServiceConfig]:
        srvs = dict(self.manual_configs)
        for scanner, detected in self.scanner.items():
            for name, config in detected.items():
                if name not in srvs.keys():
                    srvs[name] = config
        return srvs

    @property
    def autoscan(self) -> bool:
        return len(self.scanner) > 0

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
        logger.info("start scanning for services and connectivity ...")
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
        for scanner, detected in self.scanner.items():
            for config in scanner.scan():
                if config.name not in detected.keys():
                    logger.info(f"Auto-discovered service: '{config.name}' [{ config.type.upper()}] at {config.url}")
                    detected[config.name] = config
                    updates = True
        return updates

    def _update_reachability(self) -> bool:
        updates = False
        for name, config in list(self.registered_services.items()):
            if self._is_reachable(config.url):
                if name not in self.reachable_services:
                    logger.info(f"🟢 Service '{name}' [{config.type}] is ONLINE and reachable.")
                    self.reachable_services.add(name)
                    updates = True
            else:
                if name in self.reachable_services:
                    logger.warning(f"🔴 Service '{name}' [{config.type}] is OFFLINE (unreachable).")
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

