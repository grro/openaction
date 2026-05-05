from abc import ABC, abstractmethod
from typing import Any, Optional, Set



class AdapterRegistry(ABC):
    """
    Interface for a centralized registry that manages various adapter instances.

    It allows for the retrieval of specific adapters based on their functional
    category and a unique identifier.
    """

    @abstractmethod
    def get_supported_adapter_types(self) -> Set[str]:
        """
        Returns a set of all adapter categories currently supported by this registry.

        Returns:
            A set of strings representing categories like 'store_adapter', 'http_adapter', etc.
        """
        pass


    @abstractmethod
    def get_adapter(self, adapter_type: str, name: Optional[str] = None) -> Optional[Any]:
        """
        Retrieves a registered adapter instance based on its category and identifier.

        Args:
            adapter_type: The functional category of the adapter
                         (e.g., 'store_adapter', 'http_adapter').
            name: The unique identifier for the specific implementation.
                  If None, the registry returns the default adapter for this type.

        Returns:
            The adapter instance if found, or None if no matching adapter
            or default implementation exists.
        """
        pass
