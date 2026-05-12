from abc import ABC, abstractmethod

class Task(ABC):
    """
    Abstract Base Task Class defining the lifecycle and execution contract for all
    automation tasks. Every user-defined script must implement this interface
    to be compatible with the TaskRunner.
    """

    def __init__(self, store: 'Store', subscription: 'Subscription') -> None: # type: ignore
        """
        Initializes the task with a persistent storage backend and event handler.
        Please consider that no processing/threads should be started inside the __init__ method.
        Processing/Thread can be started inside the on_activate method.

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
    def on_activate(self) -> None:
        """
        Lifecycle hook: Executed once when the task is loaded and ready.

        Use this method to start background processing, initialize continuous
        polling loops, or establish initial states without blocking the
        class instantiation in __init__.
        """
        pass

    @abstractmethod
    def on_deactivate(self) -> None:
        """
        Lifecycle hook: Executed once during task removal or system shutdown.

        Use this method for teardown logic, such as closing network connections,
        persisting final buffer states, stopping background threads, or logging
        a graceful exit message.
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
         * `@when("Time cron <cron expression>")`: Triggers the execution based on a
            cron expression with 5 fields (e.g., `@when("Time cron */5 * * * *")`).
         * `@when("Item <path> changed")`: Triggers the execution when a change event has
            occurred (e.g., `@when("Item sensor://metrics/grid_power changed")`).
            If the underlying service provides a push or notification channel, this trigger
            is typically used to achieve near real-time processing. Often, a cron trigger
            (e.g., every 1 minute) is used alongside it as a fallback.

        Returns:
            str: A summary of the execution outcome (e.g., "Lights turned off").
                 This string is captured and stored in the task's execution history.
        """
        pass