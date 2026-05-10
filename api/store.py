from abc import ABC, abstractmethod


class Store(ABC):
    """
    Persistent key-value storage interface for task state.

    The `Store` is designed for long-term data persistence. Data saved here
    survives task re-executions, system restarts, and environment reloads.
    It is ideal for maintaining state across time (e.g., tracking the last
    execution time, caching stringified JSON, or maintaining counters).

    Store vs. Session:
    ------------------
    *   **Persistence:** `Store` is strictly persistent (survives restarts).
        `Session` is volatile and transient (destroyed between instantiations).
    *   **Data Types:** `Store` strictly accepts strings (complex objects
        must be serialized, e.g., via `json.dumps`). `Session` accepts live
        Python objects (like `requests.Session()` or MCP clients).
    """

    @abstractmethod
    def put(self, key: str, value: str, ttl_sec: int | None = None) -> None:
        """
        Store a string value in the persistent storage with the specified key.

        Constraints:
            The total amount of stored data per task should not exceed 4 KB.
            Tasks are responsible for their own data management—ensure stale
            or temporary data is deleted or utilizes the TTL parameter so no
            orphaned data is left behind.

        Args:
            key (str): The unique identifier for the stored value.
            value (str): The string data to be stored.
            ttl_sec (int | None, optional): Time-To-Live in seconds. If provided,
                the key-value pair will automatically expire and be removed
                after this duration. Defaults to None.
        """
        pass

    @abstractmethod
    def get(self, key: str, default_value: str = None) -> str:
        """
        Retrieve a value from the persistent storage using its key.

        Args:
            key (str): The unique identifier of the value to retrieve.
            default_value (str, optional): The value to return if the key is
                not found or has expired. Defaults to None.

        Returns:
            str: The stored string value if the key exists, otherwise the default_value.
        """
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        """
        Remove a key-value pair from the persistent storage.

        Args:
            key (str): The unique identifier of the value to be deleted.
        """
        pass

    @abstractmethod
    def keys(self) -> list[str]:
        """
        Retrieve a list of all keys currently active in the storage.

        Returns:
            list[str]: A list of active keys belonging to this task.
        """
        pass