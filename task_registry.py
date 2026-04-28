import logging
import threading
from collections.abc import Callable
from time import sleep
from typing import Any, cast
from api.mcp_service import MCPClientRegistry
from api.http_service import HttpClient
from api.store_service import StoreService
from code_repository import CodeRepository
from task import TaskAdapter, CronTaskAdapter


TaskExecute = Callable[[StoreService, MCPClientRegistry, HttpClient], str]


class TaskRegistry:
    """Registry that periodically scans and loads tasks from a CodeRepository."""

    def __init__(self, code_registry: CodeRepository):
        """Initialize scanner with a task registry.

        Args:
            code_registry: Registry containing registered task code files.
        """
        self.__is_running = False
        self._code_registry = code_registry
        self.tasks: dict[str, TaskAdapter] = {}



    def start(self):
        """Start the background scanning thread."""
        self.__is_running = True

        existing_tasks = self._code_registry.list()
        if existing_tasks:
            logging.info(f"TaskRegistry started. Registered tasks: {', '.join(existing_tasks)}")
        else:
            logging.info("TaskRegistry started. No registered tasks found.")

        threading.Thread(target=self._loop, daemon=True).start()
        return self

    def stop(self):
        """Stop the background scanning thread gracefully."""
        self.__is_running = False

    def reload(self) -> None:
        """Manually trigger a scan of the registry."""
        self._scan()

    def _loop(self) -> None:
        """Background loop that scans for tasks periodically."""
        # Wait returns True if the flag is set, False if the timeout occurs.
        # We loop as long as the stop event is NOT set.
        while self.__is_running:
            try:
                self._scan()
            except Exception as e:
                logging.exception(f"Unexpected error in periodic scan: {e}")
            try:
                self._clean_up()
            except Exception as e:
                logging.exception(f"Unexpected error in periodic clean up: {e}")
            sleep(60)

    def _scan(self) -> None:
        """Scan registry for registered tasks and load source-code task functions."""
        current_tasks: dict[str, TaskAdapter] = {}

        for task_name in self._code_registry.list():
            try:
                task_code, task_desc, task_props = self._code_registry.get(task_name)
            except Exception as e:
                logging.error(f"Failed to retrieve task '{task_name}' from repository: {e}")
                continue

            # Check if we already have this task loaded and its code hasn't changed
            existing_task = self.tasks.get(task_name)
            if existing_task and existing_task.code == task_code:
                # Keep the existing task to avoid memory leaks and unneeded execution
                current_tasks[task_name] = existing_task
                continue

            # Load or reload the task via exec()
            task = self._load_task(task_name, task_code, task_desc, task_props)
            if task:
                current_tasks[task_name] = task

        # Update the tasks dictionary (this also naturally drops tasks that were deleted from the repository)
        self.tasks = current_tasks


    def _load_task(self, task_name: str, task_code: str, task_description: str, task_props: dict[str, Any]) -> TaskAdapter | None:
        """Load and instantiate a task from raw code strings.

        Args:
            task_name: Name of the task.
            task_code: The raw Python source code.
            task_description: The description string.
            task_props: Dictionary of task properties.
        """
        try:
            # Execute task code in an isolated namespace.
            namespace: dict[str, object] = {"__name__": task_name}
            exec(task_code, namespace)

            cron_getter = namespace.get("cron")
            execute = namespace.get("execute")

            if not callable(cron_getter):
                raise ValueError("Missing required function 'cron() -> str'")

            if not callable(execute):
                raise ValueError("Missing required function 'execute(store_service, mcp_registry, shelly_registry) -> str'")

            typed_cron_getter = cast(Callable[[], str], cron_getter)
            typed_execute = cast(TaskExecute, execute)

            return CronTaskAdapter.create(task_name, task_code, task_description, task_props, typed_cron_getter, typed_execute)

        except Exception as e:
            logging.warning(f"Warning: Could not load task '{task_name}' from registry: {e}")
            return None

    def _clean_up(self):
        for task in list(self.tasks.values()):
            if not task.is_still_valid():
                self._code_registry.deregister(task.name, reason='ttl reached')
                self.reload()
                logging.info(f"Task '{task.name}' has expired and was removed from the registry.")
