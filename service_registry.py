import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


logger = logging.getLogger(__name__)


# Separator between individual service entries in the raw config string.
_ENTRY_SEPARATOR = "&"
# Separator between the "<type>:<name>" identifier and the URL of a single entry.
_ID_URL_SEPARATOR = "="
# Separator between the type and the name inside a service identifier.
_TYPE_NAME_SEPARATOR = ":"


@dataclass(frozen=True, order=True)
class Service:
    """
    Immutable description of an externally configured service.

    A service is identified by a unique ``name`` and categorised by ``type``
    (e.g. ``"HTTP"``, ``"MQTT"``). The ``url`` is informational only and is
    excluded from equality/ordering so that two configurations pointing to
    the same logical service compare equal.
    """

    name: str
    type: str
    url: str = field(compare=False)

    @staticmethod
    def parse(entry: str) -> Optional["Service"]:
        """
        Parse a single config entry of the form ``"<type>:<name>=<url>"``.

        Returns ``None`` (and logs a warning) if the entry is malformed or empty,
        so that one bad entry does not break parsing of the whole config string.
        """
        if not entry:
            return None
        try:
            identifier, url = entry.split(_ID_URL_SEPARATOR, 1)
            type_, name = identifier.split(_TYPE_NAME_SEPARATOR, 1)
            return Service(name.strip(), type_.strip().upper(), url.strip())
        except ValueError as e:
            logger.warning(f"Failed to parse service config entry '{entry}': {e}")
            return None


class Configs:
    """Helper to parse the ``&``-separated service configuration string."""

    @staticmethod
    def read(servers: str) -> Dict[str, Service]:
        """
        Parse a config string like ``"http:api=http://x&mqtt:broker=tcp://y"``
        into a ``{name: Service}`` mapping. Empty/invalid entries are skipped.
        """
        if not servers:
            return {}

        services = (Service.parse(entry.strip()) for entry in servers.split(_ENTRY_SEPARATOR))
        configs = {svc.name: svc for svc in services if svc is not None}
        logger.info(f"Loaded {len(configs)} static service configurations.")
        return configs


class ServiceRegistry:
    """Read-only registry exposing the statically configured services by name."""

    def __init__(self, services: Dict[str, Service]):
        self._services = services
        for service in services.values():
            logger.info(f"Registered service: {service.name} (type={service.type}, url={service.url})")

    @property
    def services(self) -> List[Service]:
        """All registered services as a list (order not guaranteed)."""
        return list(self._services.values())

    @property
    def names(self) -> Set[str]:
        """Set of all registered service names."""
        return set(self._services.keys())