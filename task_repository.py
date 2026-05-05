import logging
import re
import threading
import json
import zipfile
import inspect
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import Any, cast, Optional, List
from task import TaskAdapter, TaskFactory, TaskExecute, when

logger = logging.getLogger(__name__)



class CodeRepository:
    """Repository for managing task code and descriptions stored as files."""

    def __init__(self, codedir: str | Path):
        """Initialize the registry with a code directory.

        Args:
            codedir: Directory where task files and descriptions will be stored.
        """
        self._codedir = Path(codedir)
        # Ensure the directory exists
        self._codedir.mkdir(parents=True, exist_ok=True)

    def _get_paths(self, name: str) -> tuple[Path, Path, Path]:
        """Helper to generate standard file paths for a given task name."""
        return (
            self._codedir / f"{name}.py",
            self._codedir / f"{name}.desc",
            self._codedir / f"{name}.props"
        )

    def register(self, name: str, task_code: str, description: str, ttl:int, is_test: bool) -> None:
        """Register a new task by storing its code and description.

        Args:
            name: Task name (used as filename prefix).
            task_code: Python code for the task.
            description: Task description.
            ttl: time to live in sec

        Raises:
            ValueError: If task name is empty or contains invalid characters.
        """
        # Cleaner validation using regex (allows alphanumeric, underscores, and hyphens)
        if not name or not re.match(r"^[\w\-]+$", name):
            raise ValueError("Task name must be alphanumeric (with _ or - allowed)")

        if self.exists(name):
            logger.info("Overwriting existing task '%s'", name)

        code_file, desc_file, props_file = self._get_paths(name)

        # Write files
        code_file.write_text(task_code, encoding="utf-8")
        desc_file.write_text(description, encoding="utf-8")

        props = {}
        if ttl is not None:
            props["valid_to"] = (datetime.now() + timedelta(seconds=ttl)).isoformat()
        if is_test:
            props["is_test"] = True
        props_file.write_text(json.dumps({}), encoding="utf-8")


    def deregister(self, name: str, reason: str) -> None:
        """Remove a task and its description.

        Args:
            name: Task name to remove.

        Raises:
            FileNotFoundError: If task is not registered.
        """
        if not self.exists(name):
            raise FileNotFoundError(f"Task '{name}' is not registered")

        # Unlink removes the file. missing_ok=True prevents crashes if a file was manually deleted
        for file_path in self._get_paths(name):
            file_path.unlink(missing_ok=True)

    def read(self, name: str) -> tuple[str, str, dict[str, Any]]:
        """Retrieve task code and description.

        Args:
            name: Task name to retrieve.

        Returns:
            Tuple of (task_code, description, props_dict).

        Raises:
            FileNotFoundError: If task is not registered.
        """
        if not self.exists(name):
            raise FileNotFoundError(f"Task '{name}' is not registered")

        code_file, desc_file, props_file = self._get_paths(name)

        task_code = code_file.read_text(encoding="utf-8")
        description = desc_file.read_text(encoding="utf-8")

        # Safely load JSON, fallback to empty dict if properties file is corrupted
        try:
            if props_file.exists():
                props = json.loads(props_file.read_text(encoding="utf-8"))
            else:
                props = {}
        except json.JSONDecodeError:
            props = {}

        return task_code, description, props

    def list(self) -> list[str]:
        """List all registered task names.

        Returns:
            List of task names (without .py or .desc extension).
        """
        tasks = {path.stem for path in self._codedir.glob("*.py")}
        return sorted(list(tasks))

    def exists(self, name: str) -> bool:
        """Check if a task is registered.
        Requires both the .py and .desc files to exist to be considered valid.
        """
        code_file, desc_file, _ = self._get_paths(name)
        return code_file.exists()

    def list_backup(self) -> List[str]:
        """List all backup files in the code directory.

        Returns:
            List of backup filenames (sorted, newest first).
        """
        try:
            backups = [
                # Changed search pattern to match "repository_backup_*.zip"
                path.name for path in self._codedir.glob("repository_backup_*.zip")
                if path.is_file()
            ]
            return sorted(backups, reverse=True)
        except Exception as e:
            logger.warning(f"Error listing backups: {e}")
            return []

    def backup(self) -> Optional[str]:
        """Create a zip backup of all registered task files inside the code directory.

        Returns:
            The absolute path to the created backup zip file, or None if it failed.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Changed filename prefix to "repository_backup_"
        zip_filename = f"repository_backup_{timestamp}.zip"
        zip_filepath = self._codedir / zip_filename

        try:
            with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zip_file:
                if self._codedir.exists():
                    allowed_suffixes = {".py", ".desc", ".props"}

                    for file_path in self._codedir.iterdir():
                        if file_path.is_file() and file_path.suffix in allowed_suffixes:
                            zip_file.write(file_path, arcname=file_path.name)

            backup_path = str(zip_filepath.resolve())
            logger.info(f"Backup successfully created at: {backup_path}")
            return backup_path

        except Exception as e:
            logger.error(f"Error creating backup: {e}")
            # Clean up the corrupted zip file if it failed halfway through
            zip_filepath.unlink(missing_ok=True)
            return None


class TaskRepository:
    """Repository that periodically scans and loads tasks from a CodeRepository."""

    def __init__(self, code_dir: str, task_factory: TaskFactory):
        self.__is_running = False
        self._code_registry = CodeRepository(codedir=code_dir)
        self._task_factory = task_factory
        self.tasks: dict[str, TaskAdapter] = {}
        self._executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="Taskexecutor")
        logger.info("TaskRepository initialized (Taskexecutor started)")

    def register(self, name: str, task_code: str, description: str, ttl:int, is_test: bool) -> None:
        self._code_registry.register(name, task_code, description, ttl, is_test)
        self._scan()
        if name not in self.tasks:
            raise ValueError(f"Failed to register task '{name}': Please check your script syntax, required functions, and logs. It was rejected by the registry.")

    def deregister(self, name: str, reason: str) -> None:
        task = self.tasks.get(name, None)
        if task is not None:
            task.reset()
        self._code_registry.deregister(name, reason)
        self._scan()

    def backup(self) -> Optional[str]:
        return self._code_registry.backup()

    def list_backup(self) -> List[str]:
        return self._code_registry.list_backup()

    def start(self):
        """Start the background scanning thread."""
        self.__is_running = True

        existing_tasks = self._code_registry.list()
        if existing_tasks:
            logger.info(f"TaskRegistry started. Registered tasks: {', '.join(existing_tasks)}")
        else:
            logger.info("TaskRegistry started. No registered tasks found.")

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

        stored_task_names = self._code_registry.list()

        removed_task_names = set(self.tasks.keys()) - set(stored_task_names)
        current_tasks = {name: task for name, task in self.tasks.items() if name not in removed_task_names}

        new_task_names = set(stored_task_names) - set(self.tasks.keys())
        for name in new_task_names:
            try:
                task_code, task_desc, task_props = self._code_registry.read(name)
            except Exception as e:
                logger.error(f"Failed to retrieve task '{name}' from code repository: {e}")
                continue
            task = self._load_task(name, task_code, task_desc, task_props)
            if task is not None:
                current_tasks[name] = task
                logger.info(f"Task '{name}' with function '{task.function_name}' found in code repository.")

        # make updated tasks list visible
        self.tasks = current_tasks

        for name in removed_task_names:
            logger.info(f"Task '{name}' was removed from the registry")

        for name in new_task_names:
            task = current_tasks[name]
            if task.load_on_start:
                logger.info(f"Task '{name}' added to registry (is marked to load on start. Triggering initial execution)")
                task.safe_run()
            else:
                logger.info(f"Task '{name}' added to registry")


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
            namespace: dict[str, object] = {"__name__": task_name, "when": when}
            exec(task_code, namespace)

            target_func = None
            cron_expr = None
            load_on_start = False

            for obj in namespace.values():
                if callable(obj) and (hasattr(obj, "__openaction_rule_loaded__") or hasattr(obj, "__openaction_cron__")):
                    target_func = obj
                    cron_expr = getattr(obj, "__openaction_cron__", None)
                    load_on_start = getattr(obj, "__openaction_rule_loaded__", False)
                    break

            if target_func is None:
                target_func = namespace.get("execute")

                if not callable(target_func):
                    raise ValueError("Missing required function 'execute(store, registry) -> str' or @when annotation")


            try:
                sig_str = str(inspect.signature(target_func))
                target_func_name = getattr(target_func, "__name__", "execute") + sig_str
            except Exception:
                target_func_name = getattr(target_func, "__name__", str(target_func))

            def wrapped_execute(store, registry):
                sig = inspect.signature(target_func)
                params = list(sig.parameters.keys())

                injectable = {
                    "store": store,
                    "registry": registry
                }

                kwargs = {}
                for p in params:
                    if p in injectable:
                        kwargs[p] = injectable[p]

                if not kwargs and len(params) == 2:
                    return target_func(store, registry)

                return target_func(**kwargs)

            typed_execute = cast(TaskExecute, wrapped_execute)

            return self._task_factory.create(task_name, task_code, task_description, task_props, cron_expr, load_on_start, typed_execute, target_func_name, self._executor)
        except Exception as e:
            logger.warning(f"Warning: Could not load task '{task_name}' from registry: {e}")
            return None

    def _clean_up(self):
        for task in list(self.tasks.values()):
            if not task.is_still_valid():
                self._code_registry.deregister(task.name, reason='ttl reached')
                self._scan()
                logger.info(f"Task '{task.name}' has expired and was removed from the registry")
