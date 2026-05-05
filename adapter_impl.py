import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any, Set
from api.adapter import AdapterRegistry

logger = logging.getLogger(__name__)


class Registry(ABC):
    """
    Interface for a specific adapter type registry.
    Responsible for managing instances of one specific category.
    """
    @abstractmethod
    def get_adapter(self, name: Optional[str] = None) -> Optional[Any]:
        """Returns the adapter instance associated with the given name."""
        pass


class AdapterManager(AdapterRegistry):
    """
    Orchestrates multiple registries, each handling a different adapter_type.
    """
    def __init__(self, registries: Dict[str, Registry]):
        self._registries = registries or {}

    def get_supported_adapter_types(self) -> Set[str]:
        """Returns all available adapter categories (e.g., 'http_adapter')."""
        return set(self._registries.keys())

    def get_adapter(self, adapter_type: str, name: Optional[str] = None) -> Any:
        """
        Retrieves an adapter with layered error handling.
        """
        # 1. Validation: Does a registry for this specific type exist?
        registry = self._registries.get(adapter_type)

        if not registry:
            logger.error(
                f"Retrieval failed: No registry found for type '{adapter_type}'. "
                f"Available types: {list(self._registries.keys())}"
            )
            return None

        try:
            # 2. Delegation: Forward the request to the specialized registry
            adapter = registry.get_adapter(name)

            if adapter is None:
                identifier = f"'{name}'" if name else "Default"
                logger.warning(
                    f"Registry '{adapter_type}' could not find an adapter for {identifier}."
                )

            return adapter

        except Exception as e:
            # 3. Safeguard: Protect against implementation errors in the concrete registry
            logger.exception(
                f"Unexpected error while retrieving adapter '{name}' "
                f"from registry '{adapter_type}': {str(e)}"
            )
            return None