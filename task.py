import concurrent
import logging
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from api.mcp_service import MCPClientRegistry
from api.http_service import HttpClient
from api.store_service import StoreService
from store import ScopedStore, Store



@dataclass
class TaskResult:
    date: datetime
    result: str | None
    error: Exception | None




class Task(ABC):

    @abstractmethod
    def execute(self, store_service: StoreService, mcp_registry: MCPClientRegistry, http_client: HttpClient) -> str:
        pass



class CronTask(Task):

    @abstractmethod
    def cron_expression(self) -> str:
        pass



class TaskAdapter(ABC):


    def __init__(self, name: str, code: str, description: str, props: Dict[str, Any], meth_to_execute: Callable[[StoreService, MCPClientRegistry, HttpClient], str], store: Store):
        self.name = name
        self.description = description
        self.props = props
        self.code = code
        self.__meth_to_execute = meth_to_execute
        self.last_executions: List[TaskResult] = list()
        self.scoped_store = ScopedStore(store, self.name)
        self.default_timeout_sec = 30
        self._execution_lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"Task-{self.name}")

    def data(self) -> Dict[str, str]:
        return {key: self.scoped_store.get(key) for key in self.scoped_store.keys()}

    def is_still_valid(self) -> bool:
        if "valid_to" in self.props:
            valid_to = self.props["valid_to"]
            if datetime.now() < datetime.fromisoformat(valid_to):
                return False
        return True

    def time_last_run(self)  -> Optional[datetime]:
        if len(self.last_executions) == 0:
            return None
        else:
            return self.last_executions[-1].date

    def time_since_last_error(self)  -> Optional[timedelta]:
        if len(self.last_executions) == 0 or self.last_executions[-1].error is None:
            return None
        return datetime.now() - self.last_executions[-1].date

    def run(self, store_service: StoreService, mcp_registry: MCPClientRegistry, http_client: HttpClient) -> str:
        """Execute the task function with the given store and MCP context."""

        if not self._execution_lock.acquire(blocking=False):
            msg = f"Task '{self.name}' is already running. Skipping this execution cycle."
            logging.warning(msg)
            return msg

        # Timeout aus den Props lesen, falls die KI einen spezifischen Wert gesetzt hat
        timeout_sec = self.props.get("timeout", self.default_timeout_sec)

        try:
            future = self._executor.submit(self.__meth_to_execute,self.scoped_store, mcp_registry, http_client)
            result = future.result(timeout=timeout_sec)
            self.last_executions.append(TaskResult(datetime.now(), result, None))
            return result
        except concurrent.futures.TimeoutError:
            # Spezielle Behandlung für Zeitüberschreitungen
            error_msg = f"Task '{self.name}' timed out after {timeout_sec} seconds."
            self.last_executions.append(TaskResult(datetime.now(), None, Exception(error_msg)))
            logging.error(error_msg)
            return error_msg
        except Exception as e:
            self.last_executions.append(TaskResult(datetime.now(), None, e))
            raise e
        finally:
            self._execution_lock.release()
            if len(self.last_executions) > 10:
                del self.last_executions[0]

    def __del__(self):
        self._executor.shutdown(wait=False)


class CronTaskAdapter(TaskAdapter):


    def __init__(self, name: str, code: str, description: str, props: Dict[str, Any], cron_getter: Callable[[], str],
                 execute: Callable[[StoreService, MCPClientRegistry, HttpClient], str], store: StoreService):
        self.cron_expression = cron_getter()
        super().__init__(name, code, description, props, execute, store)

    @staticmethod
    def create(name: str, code: str, description: str, props: Dict[str, Any], cron_getter: Callable[[], str], execute: Callable[[StoreService, MCPClientRegistry, HttpClient], str], store: StoreService):
        return CronTaskAdapter(name, code, description, props, cron_getter, execute, store)

