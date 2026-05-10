import logging
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from time import sleep
from typing import List, Optional

from code_repository import CodeRepository
from store_impl import SimpleStore
from task_adapter import TaskAdapter, TaskAdapterFactory

logger = logging.getLogger(__name__)




class TaskAdapterRepository:
    """Repository that periodically scaerver disconnected. For troubleshooting guidance,ns and loads tasks from a CodeRepository."""

    def __init__(self, code_dir: str, task_factory: TaskAdapterFactory, store: SimpleStore):
        self.__is_running = False
        self._code_registry = CodeRepository(codedir=code_dir)
        self._task_factory = task_factory
        self._store = store
        self.tasks: dict[str, TaskAdapter] = {}
        self._executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="Taskexecutor")
        logger.info("TaskRepository initialized (Taskexecutor started)")

    def register(self, name: str, code: str, description: str, ttl: Optional[int], is_test: bool) -> None:

        if name.startswith("test_"):
            raise ValueError("Task name cannot contain dots ('.')")

        for char in [',', '.', ' ', '/', '\\', ':', '*', '?', '"', '<', '>', '|']:
            if char in name:
                raise ValueError("Task name cannot contain '" + char + "'")

        task = self._task_factory.create(name, code, description, ttl, is_test)

        image = self._code_registry.create_image(name)
        image.write_data(task.code, task.props)

        self._add_task(name, task, reason="newly registered")

    def _add_task(self, name: str, task: TaskAdapter, reason: str):
        if task is None:
            logger.warning(f"Failed to add task '{name}'. Ni after registration.")
        else:
            is_new = name not in self.tasks.keys()
            is_updated = not is_new and self.tasks[name].created_at != task.created_at

            if is_new or is_updated:
                self.tasks[name] = task

                task.activate()
                if task.run_on_start:
                    if is_new:
                        logger.info(f"Task '{name}' added to registry with load on start (Reason: {reason})")
                    else:
                        logger.info(f"Task '{name}' re-added to registry with load on start (Reason: {reason})")
                    task.safe_run("run_on_start")
                else:
                    if is_new:
                        logger.info(f"Task '{name}' added to registry (Reason: {reason})")
                    else:
                        logger.info(f"Task '{name}' re-added to registry (Reason: {reason})")

                #if len(task.props_observed) > 0:
                 #   for prop_observed in task.props_observed:
                  #      self._subscription_service.subscribe(prop_observed.service, prop_observed.prop, prop_observed.min_interval_sec, task)

    def deregister(self, name: str, reason: str) -> None:
        task = self.tasks.pop(name, None)
        if task:
            task.reset()
            logger.info(f"Task '{name}' has been deregistered (Reason: {reason})")
        self._code_registry.delete_image(name)

    def start(self):
        """Start the background scanning thread."""
        self.__is_running = True
        threading.Thread(target=self._loop, daemon=True).start()
        return self

    def stop(self):
        """Stop the background scanning thread gracefully."""
        self.__is_running = False

    def _loop(self) -> None:
        """Background loop that scans for tasks periodically."""

        sleep(10)

        # Wait returns True if the flag is set, False if the timeout occurs.
        # We loop as long as the stop event is NOT set.
        while self.__is_running:
            try:
                self._scan()
            except Exception as e:
                logger.exception(f"Unexpected error in periodic scan: {e}")
            try:
                self._clean_up()
            except Exception as e:
                logger.exception(f"Unexpected error in periodic clean up: {e}")
            sleep(60)


        self._executor.shutdown()
        logger.info("TaskRepository stopped and Taskexecutor shut down.")


    def _scan(self) -> None:
        load_task_names = set()
        for image in self._code_registry.list_images():
            code, props = image.read()
            task = self._task_factory.restore(image.unit_name, code, props)
            load_task_names.add(task.name)

            if task.name in self.tasks.keys():
                if task.created_at > self.tasks.get(task.name).created_at:
                    self._add_task(task.name, task, reason="loaded from code repo")
            else:
                self._add_task(task.name, task, reason="loaded from code repo")

        for name in list(self.tasks.keys()):
            if name not in load_task_names:
                self.deregister(name, reason="no longer exist in code repo")


    def _clean_up(self):
        for image in self._code_registry.list_images():
            code, props = image.read()
            task = self._task_factory.restore(image.unit_name, code, props)
            if task.is_expired():
                self.deregister(task.name, reason="TTL expired")
