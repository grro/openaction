import logging
from dataclasses import dataclass, field
from typing import Dict, Set, List


logger = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
class Service:
    name: str
    type: str
    url: str = field(compare=False)

    @staticmethod
    def read(conf: str):
        try:
            if conf is not None:
                id, url = conf.split('=', 1)
                type, name = id.split(':', 1)
                return Service(name.strip(), type.strip().upper(), url.strip())
        except Exception as e:
            logger.warning(f"Failed to parse service config entry '{conf}': {e}")
        return None


class Configs:

    @staticmethod
    def read(servers: str) -> Dict[str, Service]:
        if not servers:
            return {}
        else:
            srvs = [Service.read(entry) for entry in servers.split('&') if len(entry.strip()) > 0]
            configs = {srv.name: srv for srv in srvs if srv is not None}
            logger.info(f"Loaded {len(configs)} static service configurations.")
            return configs




class ServiceRegistry:

    def __init__(self, services: Dict[str, Service]):
        self._services = services
        for service in services.values():
            logger.info(f"Registered service: {service.name} (type={service.type}, url={service.url})")


    @property
    def services(self) -> List[Service]:
        return list(self._services.values())

    @property
    def names(self) -> Set[str]:
        return set(self._services.keys())

    def start(self):
        pass

    def stop(self):
        pass