from abc import ABC, abstractmethod



class Task(ABC):
    """
    Abstract Base Task Class defining the lifecycle and execution contract for all
    automation tasks. Every user-defined script must implement this interface
    to be compatible with the TaskRunner.
    """

    def __init__(self, store):
        """
        Initializes the task with a persistent storage backend.

        Args:
            store (Store): A key-value store provided by the host environment
                           to persist data across task executions and restarts.
                           The store is individual for each task instance (not shared).
        """
        self.store = store

    @abstractmethod
    def on_destroy(self):
        """
        Lifecycle hook: Executed once during task removal or system shutdown.

        Use this method for teardown logic, such as closing network connections,
        persisting final buffer states, or logging a graceful exit message.
        """
        pass

    @abstractmethod
    def on_execute(self) -> str:
        """
        Main execution logic: Called on a schedule (cron) or event-driven trigger. This method
        contains the core procedural logic.

        To trigger the method execution, it must be decorated with on or more `@when` statements.
        Supported triggers are:
         * `@when("Rule loaded")`: Triggers the execution when the rule has been (re)loaded.
         * `@when("Time cron <cron expression>")`: Triggers the execution based on a
           cron expression (e.g., `@when("Time cron 55 55 5 * * ?")`).

        Returns:
            str: A summary of the execution outcome (e.g., "Lights turned off").
                 This string is captured and stored in the task's execution history.
        """
        pass