from abc import ABC, abstractmethod
from typing import Any

class Task(ABC):
    """
    Abstract Base Task Class defining the lifecycle and execution contract for all
    automation tasks. Every user-defined script must implement this interface
    to be compatible with the TaskRunner.
    """

    def __init__(self, store: 'Store', subscription: 'Subscription') -> None: # type: ignore
        """
        Initializes the task with a persistent storage backend and event handler.

        Args:
            store (Store): A key-value store provided by the host environment
                to persist data across task executions and restarts.
                The store is individual for each task instance (not shared).
            subscription (Subscription): A task-specific event handler, which will be
                called with the path of the changed value when a subscribed value changes.
                This will be used to trigger task execution based on changes to specific
                data points (e.g., sensor readings, device states) when the proper
                '@when' decorator is set on the on_execute method.
        """
        self.store = store
        self.subscription = subscription

    @abstractmethod
    def on_destroy(self) -> None:
        """
        Lifecycle hook: Executed once during task removal or system shutdown.

        Use this method for teardown logic, such as closing network connections,
        persisting final buffer states, or logging a graceful exit message.
        """
        pass

    @abstractmethod
    def on_execute(self) -> str:
        """
        Main execution logic: Called on a schedule (cron) or event-driven trigger.
        This method contains the core procedural logic.

        To trigger the method execution, it must be decorated with one or more `@when` statements.
        Supported triggers are:
         * `@when("Rule loaded")`: Triggers the execution when the rule has been (re)loaded.
         * `@when("Item <path> changed")`: Triggers the execution when a change event has
            occurred (e.g., `@when("Item sensor://metrics/grid_power changed")`).
         * `@when("Time cron <cron expression>")`: Triggers the execution based on a
            cron expression with 5 fields (e.g., `@when("Time cron 0 15 10 * *")`).

        Returns:
            str: A summary of the execution outcome (e.g., "Lights turned off").
                 This string is captured and stored in the task's execution history.
        """
        pass