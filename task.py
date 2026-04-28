from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from api.mcp_service import MCPClientRegistry
from api.http_service import HttpClient
from api.store_service import StoreService
from store_service import ScopedStore



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


    def __init__(self, name: str, code: str, description: str, props: Dict[str, Any], meth_to_execute: Callable[[StoreService, MCPClientRegistry, HttpClient], str]):
        self.name = name
        self.description = description
        self.props = props
        self.code = code
        self.__meth_to_execute = meth_to_execute
        self.last_executions: List[TaskResult] = list()

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

        try:
            scoped_store = ScopedStore(store_service, self.name)
            result = self.__meth_to_execute(scoped_store, mcp_registry, http_client)
            self.last_executions.append(TaskResult(datetime.now(), result, None))
            return result
        except Exception as e:
            self.last_executions.append(TaskResult(datetime.now(), None, e))
            raise e
        finally:
            if len(self.last_executions) > 10:
                del self.last_executions[0]



class CronTaskAdapter(TaskAdapter):


    def __init__(self, name: str, code: str, description: str, props: Dict[str, Any], cron_getter: Callable[[], str],
                 execute: Callable[[StoreService, MCPClientRegistry, HttpClient], str]):
        self.cron_expression = cron_getter()
        super().__init__(name, code, description, props, execute)

    @staticmethod
    def create(name: str, code: str, description: str, props: Dict[str, Any], cron_getter: Callable[[], str], execute: Callable[[StoreService, MCPClientRegistry, HttpClient], str]):
        return CronTaskAdapter(name, code, description, props, cron_getter, execute)

