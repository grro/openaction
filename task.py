import concurrent
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from adapter_impl import AdapterManager
from api.adapter import AdapterRegistry
from store_impl import ScopedStore, Store, SimpleStore

logger = logging.getLogger(__name__)


TaskExecute = Callable[[Store, AdapterRegistry], str]


def when(target: str):
    """Decorator to define a task's trigger (cron, property change, etc.) and execution entry point."""
    def decorator(func):
        if not hasattr(func, "__openaction_cron__"):
            func.__openaction_cron__ = None
        if not hasattr(func, "__openaction_rule_loaded__"):
            func.__openaction_rule_loaded__ = False

        # Support 'Time cron' OpenHab-style string and convert to linux 5-part if it's longer
        if target.upper().startswith("TIME CRON "):
            parts = target[10:].strip().split()
            if len(parts) >= 6:
                # Assuming Quatz "Sec Min Hour DoM Month DoW [Year]" -> "Min Hour DoM Month DoW"
                # Note: This is an approximation.
                try:
                    minutes = parts[1]
                    hours = parts[2]
                    dom = parts[3]
                    month = parts[4]
                    dow = parts[5].replace('?', '*')
                    func.__openaction_cron__ = f"{minutes} {hours} {dom} {month} {dow}"
                except IndexError:
                    func.__openaction_cron__ = target[10:].strip()
            else:
                func.__openaction_cron__ = target[10:].strip()

        elif target.upper().startswith("RULE LOADED"):
            func.__openaction_rule_loaded__ = True

        elif target.startswith("Item ") or target.startswith("Property ") or target.startswith("System ") or target.startswith("Rule "):
            pass

        return func

    return decorator



@dataclass
class TaskResult:
    date: datetime
    result: str | None
    error: Exception | None
    elapsed: timedelta | None


class Task(ABC):

    @abstractmethod
    def execute(self, store: Store, registry: AdapterRegistry) -> str:
        pass


class CronTask(Task):

    @abstractmethod
    def cron_expression(self) -> str:
        pass


class TaskAdapter:

    RUNNING = "running"
    IDLING = "idling"

    """
    Adapter for executing AI-generated tasks.
    Manages execution state, persistent storage scoping, and thread-safe execution.
    """

    def __init__(self,
                 name: str,
                 code: str,
                 description: str,
                 props: Dict[str, Any],
                 cron_expression: str,
                 load_on_start: bool,
                 meth_to_execute: Callable[[Store, AdapterRegistry], str],
                 execute_name: str,
                 executor: ThreadPoolExecutor,
                 store: Store,
                 adapter_manager: AdapterManager):
        self.name = name
        self.description = description
        self.props = props
        self.code = code
        self._scoped_store = store
        self._adapter_manager = adapter_manager
        self._meth_to_execute = meth_to_execute
        self.function_name = execute_name
        self.load_on_start = load_on_start
        self.cron_expression = cron_expression
        self._executor = executor
        self.last_executions: List[TaskResult] = list()
        self.default_timeout_sec = 30
        self.state = self.IDLING

    def data(self) -> Dict[str, str]:
        """Returns the current persistent state stored for this specific task."""
        return {key: self._scoped_store.get(key) for key in self._scoped_store.keys()}

    def reset(self) -> None:
        """Clears all persistent state for this task."""
        for key in self._scoped_store.keys():
            self._scoped_store.delete(key)

    @property
    def is_test_task(self) -> bool:
        return self.props.get("is_test", False)

    def is_still_valid(self) -> bool:
        """Checks if the task's TTL has expired based on the 'valid_to' property."""
        if "valid_to" in self.props:
            valid_to = self.props["valid_to"]
            if datetime.now() < datetime.fromisoformat(valid_to):
                return True
            else:
                return False
        return True

    def last_attempt_at(self) -> Optional[datetime]:
        """Returns the timestamp of the most recent execution attempt."""
        if not self.last_executions:
            return None
        return self.last_executions[-1].date

    def last_failure_age(self) -> Optional[timedelta]:
        """Returns the duration since the last failed execution."""
        if not self.last_executions or self.last_executions[-1].error is None:
            return None
        return datetime.now() - self.last_executions[-1].date

    def safe_run(self):
        """Executes the task and ensures any unhandled exceptions are logged."""
        try:
            self.run()
        except Exception as e:
            logger.error(f"Execution failed for task '{self}': {e}")

    def run(self) -> str:
        """
        Executes the task logic within a dedicated thread.
        Handles locking, timeouts, and result logging.
        """

        # Acquire lock without blocking to prevent overlapping executions of the same task
        if self.state == self.RUNNING:
            msg = f"Task '{self.name}' is already running. Skipping this execution cycle."
            logger.debug(msg)
            return msg

        # Retrieve custom timeout from properties or use the system default
        timeout_sec = self.props.get("timeout", self.default_timeout_sec)

        start = datetime.now()
        try:
            self.state = self.RUNNING
            # Submit the task method to the isolated thread pool
            logger.info("Executing task '%s'", self.name)
            future = self._executor.submit(self._meth_to_execute,self._scoped_store, self._adapter_manager)

            # Wait for completion or timeout
            result = future.result(timeout=timeout_sec)
            elapsed = datetime.now() - start
            self.last_executions.append(TaskResult(datetime.now(), result, None, elapsed))
            return result

        except concurrent.futures.TimeoutError as te:
            # Handle cases where the script hangs or takes too long
            error_msg = f"Task '{self.name}' timed out after {timeout_sec} seconds."
            elapsed = datetime.now() - start
            self.last_executions.append(TaskResult(datetime.now(), None, Exception(error_msg), elapsed))
            logger.warning(f"Execution failed (TimeoutError; timeout {timeout_sec} seconds) for task '{self.name}': {str(te)}")
            return error_msg

        except Exception as e:
            # Log the crash and re-raise to allow the scheduler to handle the failure
            elapsed = datetime.now() - start
            self.last_executions.append(TaskResult(datetime.now(), None, e, elapsed))
            logger.warning(f"Execution failed for task '{self.name}': {type(e).__name__}: {str(e)}")

        finally:
            self.state = self.IDLING
            # Always release the lock and maintain a sliding window of the last 10 results
            if len(self.last_executions) > 10:
                del self.last_executions[0]


class TaskFactory:

    def __init__(self, store: SimpleStore, adapter_manager: AdapterManager):
        self._store = store
        self._adapter_manager = adapter_manager

    def create(self,
               name: str,
               code: str,
               description: str,
               props: Dict[str, Any],
               cron_expression: str,
               load_on_start: bool,
               execute: Callable[[Store, AdapterRegistry], str],
               execute_name: str,
               executor: ThreadPoolExecutor):
        return TaskAdapter(name,
                           code,
                           description,
                           props,
                           cron_expression,
                           load_on_start,
                           execute,
                           execute_name,
                           executor,
                           ScopedStore(self._store, name),
                           self._adapter_manager)
