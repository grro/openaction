from abc import ABC, abstractmethod
from typing import Any

class StoreService(ABC):
    """
    Abstract base class defining the contract for a key-value storage service.
    """

    @abstractmethod
    def put(self, key: str, value: str, ttl_sec: int | None = None) -> None:
        """
        Store a value in the storage with the specified key.

        Args:
            key (str): The unique identifier for the stored value.
            value (Any): The data to be stored.
            ttl_sec (int | None, optional): Time-To-Live in seconds. If provided,
                                            the key-value pair will expire and be
                                            removed after this duration. Defaults to None.
        """
        pass

    @abstractmethod
    def get(self, key: str, default_value: str = None) -> str:
        """
        Retrieve a value from the storage using its key.

        Args:
            key (str): The unique identifier of the value to retrieve.
            default_value (Any, optional): The value to return if the key is not found
                                           in the storage. Defaults to None.

        Returns:
            Any: The stored value if the key exists, otherwise the default_value.
        """
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        """
        Remove a key-value pair from the storage.

        Args:
            key (str): The unique identifier of the value to be deleted.
        """
        pass