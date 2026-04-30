import concurrent
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from api.mcp_service import MCPClientRegistry
from api.http_service import HttpClient
from api.store_service import StoreService
from http_client import AutoRecreateHttpClient
from mcp_client import McpRegistry
from store import ScopedStore, Store

logger = logging.getLogger(__name__)


@dataclass
class TaskResult:
    date: datetime
    result: str | None
    error: Exception | None
    elapsed: timedelta | None


class Task(ABC):

    @abstractmethod
    def execute(self, store_service: StoreService, mcp_registry: MCPClientRegistry, http_client: HttpClient) -> str:
        pass


class CronTask(Task):

    @abstractmethod
    def cron_expression(self) -> str:
        pass


class TaskAdapter(ABC):

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
                 meth_to_execute: Callable[[StoreService, MCPClientRegistry, HttpClient], str],
                 store: StoreService,
                 mcp_registry: McpRegistry,
                 http_client: HttpClient):
        self.name = name
        self.description = description
        self.props = props
        self.code = code
        self._scoped_store = store
        self._mcp_registry = mcp_registry
        self._http_client = http_client
        self._meth_to_execute = meth_to_execute
        self.last_executions: List[TaskResult] = list()
        self.default_timeout_sec = 30
        self.state = self.IDLING

        # Dedicated executor to isolate task execution from the main scheduler thread
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"Task-{self.name}")

    def data(self) -> Dict[str, str]:
        """Returns the current persistent state stored for this specific task."""
        return {key: self._scoped_store.get(key) for key in self._scoped_store.keys()}

    def reset(self) -> None:
        """Clears all persistent state for this task."""
        for key in self._scoped_store.keys():
            self._scoped_store.delete(key)

    def is_still_valid(self) -> bool:
        """Checks if the task's TTL has expired based on the 'valid_to' property."""
        if "valid_to" in self.props:
            valid_to = self.props["valid_to"]
            # BUGFIX: Wenn jetzt VOR valid_to ist, ist der Task noch gültig (True)
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
            future = self._executor.submit(self._meth_to_execute,self._scoped_store,self._mcp_registry,self._http_client)

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
            logger.error(f"Execution failed (TimeoutError; timeout {timeout_sec} seconds) for task '{self.name}': {str(te)}")
            return error_msg

        except Exception as e:
            # Log the crash and re-raise to allow the scheduler to handle the failure
            elapsed = datetime.now() - start
            self.last_executions.append(TaskResult(datetime.now(), None, e, elapsed))
            logger.error(f"Execution failed for task '{self.name}': {str(e)}")

        finally:
            self.state = self.IDLING
            # Always release the lock and maintain a sliding window of the last 10 results
            if len(self.last_executions) > 10:
                del self.last_executions[0]

    def __del__(self):
        """Ensure the thread pool is shut down when the task adapter is destroyed."""
        self._executor.shutdown(wait=False)


class CronTaskAdapter(TaskAdapter):

    def __init__(self,
                 name: str,
                 code: str,
                 description: str,
                 props: Dict[str, Any], cron_getter: Callable[[], str],
                 execute: Callable[[StoreService, MCPClientRegistry, HttpClient], str],
                 store: StoreService,
                 mcp_registry: McpRegistry,
                 http_client: HttpClient):
        super().__init__(name, code, description, props, execute, store, mcp_registry, http_client)
        self.cron_expression = cron_getter()



class TaskFactory:

    def __init__(self, store: Store, mcp_registry: McpRegistry):
        self._store = store
        self._mcp_registry = mcp_registry

    def create(self,
               name: str,
               code: str,
               description: str,
               props: Dict[str, Any],
               cron_getter: Callable[[], str],
               execute: Callable[[StoreService, MCPClientRegistry, HttpClient], str]):
        return CronTaskAdapter(name,
                               code,
                               description,
                               props,
                               cron_getter,
                               execute,
                               ScopedStore(self._store, name),
                               self._mcp_registry.clone(),
                               AutoRecreateHttpClient())