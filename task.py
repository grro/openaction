from abc import abstractmethod, ABC
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from store_service import Store, ScopedStore



@dataclass
class TaskResult:
    date: datetime
    result: str
    error: Exception



class Task(ABC):

    def __init__(self, name: str, code: str, description: str, props: Dict[str, Any]):
        self.name = name
        self.description = description
        self.props = props
        self.code = code
        self.last_executions: List[TaskResult] = list()


    def time_last_run(self)  -> Optional[datetime]:
        if len(self.last_executions) == 0:
            return None
        else:
            return self.last_executions[-1].date

    def time_since_last_error(self)  -> Optional[timedelta]:
        if len(self.last_executions) == 0 or self.last_executions[-1].error is None:
            return None
        return datetime.now() - self.last_executions[-1].date

    @staticmethod
    def create(name: str, code: str, description: str, props: Dict[str, Any], cron_getter: Callable[[], str], execute: Callable[[Store, Mapping[str, Any]], None]) -> "Task":
        return FunctionTaskAdapter(name, code, description, props, cron_getter, execute)

    @property
    @abstractmethod
    def cron_expression(self) -> str:
        pass

    @abstractmethod
    def run(self, store: Store, mcp: Mapping[str, Any]) -> str:
        pass


class FunctionTaskAdapter(Task):

    def __init__(self, name: str, code: str, description: str, props: Dict[str, Any], cron_getter: Callable[[], str], execute: Callable[[Store, Mapping[str, Any]], None]):
        self.__cron_expression = cron_getter()
        self.__execute = execute
        super().__init__(name, code, description, props)

    @property
    def cron_expression(self) -> str:
        return self.__cron_expression

    def run(self, store: Store, mcp: Mapping[str, Any]) -> str:
        """Execute the task function with the given store and MCP context."""

        try:
            scoped_store: Dict[str, Any] = ScopedStore(store, self.name)
            result = self.__execute(scoped_store, mcp or {})
            self.last_executions.append(TaskResult(datetime.now(), result, None))
            return result
        except Exception as e:
            self.last_executions.append(TaskResult(datetime.now(), None, e))
            raise e
        finally:
            if len(self.last_executions) > 10:
                del self.last_executions[0]

