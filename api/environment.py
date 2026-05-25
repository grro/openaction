from abc import ABC, abstractmethod


class Environment(ABC):
    """
    Provides the execution context and access to core system services.

    This abstract base class acts as a central registry or dependency injection
    container. It is passed to various components (such as background tasks or rules)
    to give them a standardized, unified way to interact with system-level
    features like persistent storage and event logging.
    """

    @property
    @abstractmethod
    def store(self) -> 'Store':
        """
        Access the system's persistent key-value store.

        Used by components to save and retrieve state across executions,
        ensuring idempotency and persistent configuration.

        Returns:
            Store: The interface for persistent data storage.
        """
        pass

    @property
    @abstractmethod
    def eventlog(self) -> 'EventLog':
        """
        Access the high-priority daily event logger.

        Used by components to record significant occurrences, alerts, or
        critical state changes that should be visible to the user.

        Returns:
            EventLog: The interface for recording system events.
        """
        pass