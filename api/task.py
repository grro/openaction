from abc import ABC, abstractmethod
from typing import List




class AdhocTask(ABC):
    """
    Abstract Base Class defining an ad hoc task.

    These tasks are triggered manually. Typically, ad hoc tasks are used to implement
    persistent "Do-That" actions by consuming parameters to reach a target state.

    Examples incude:
        - Set roller shutter to position X (where X is provided as a parameter).
        - Switch a light on/off (where the target state is provided as a parameter).
    """

    def __init__(self, store: 'Store') -> None:  # type: ignore
        self.store = store


    @abstractmethod
    def on_execute_with_params(self, params: List[str]) -> str:
        """
        Executes the ad hoc task with the provided parameters.

        In contrast to the `on_execute` method of a `BackgroundTask`, this method
        accepts parameters to dictate the specific action to be taken. Additionally,
        because ad hoc tasks are triggered manually on demand, the `@when` decorator
        does not make sense here and is not supported.

        Args:
            params (List[str]): A list of string parameters required to execute the task.

        Returns:
            str: A summary of the execution outcome.
        """
        pass




class BackgroundTask(ABC):
    """
    Abstract Base Class defining the lifecycle and execution contract for repeating,
    automated background tasks.

    These tasks are triggered by system events or scheduled (cron) intervals.
    Every user-defined script must implement this interface to be compatible
    with the TaskRunner.

    Typically, background tasks are used to implement persistent "If-This-Then-That"
    rules, allowing the system to automatically control devices or trigger actions
    based on environmental changes.

    Examples include:
        - Roller shutter rules based on the time of day.
        - Lighting rules based on time and outside brightness.
        - Energy management rules that control heating rods depending on excess power grid capacity.

    Lifecycle Management:
        Use the lifecycle methods to manage internally used session-related instances,
        such as network clients (e.g., httpx.Client). Initializing clients in
        `on_activate` and closing them in `on_deactivate` avoids creating new network
        sessions for each execution call, significantly improving performance and stability.
    """

    def __init__(self, store: 'Store') -> None: # type: ignore
        """
        Initializes the task with a persistent storage backend and event handler.

        Note:
            No processing or background threads should be started inside the
            `__init__` method. Heavy initialization or thread creation must
            be deferred to the `on_activate` method.

        Args:
            store (Store): A key-value store provided by the host environment
                to persist data across task executions and restarts. The store
                is isolated for each task instance (not shared).
        """
        self.store = store


    @abstractmethod
    def on_activate(self) -> None:
        """
        Lifecycle hook: Executed once when the task is loaded and ready.

        Use this method to start background processing, initialize continuous
        polling loops, or establish initial states without blocking the
        class instantiation in `__init__`.
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

        This method contains the core procedural logic. To trigger its execution,
        it must be decorated with one or more `@when` statements.

        Supported triggers:
            * `@when("Rule loaded")`: Triggers the execution when the rule has been (re)loaded.
            * `@when("Time cron <cron expression>")`: Triggers the execution based on a
              cron expression with 5 fields  <m h d M w> or 6 fields format <s m h d M w>
              (e.g., `@when("Time cron */5 * * * * *")`).

        Returns:
            str: A summary of the execution outcome (e.g., "Lights turned off").
                 This string is captured and stored in the task's execution history.
        """
        pass