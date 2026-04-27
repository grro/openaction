import os
import logging
import zipfile
import json
from datetime import datetime
from collections.abc import Callable, Mapping
from threading import Thread
from time import sleep
from typing import Any, Set, cast, Dict, Optional

from task import Task
from pathlib import Path



class CodeRegistry:
    """Registry for managing task code and descriptions stored as files."""

    def __init__(self, codedir: str):
        """Initialize the registry with a code directory.

        Args:
            codedir: Directory where task files and descriptions will be stored.
        """
        self.__codedir = codedir
        # Ensure the directory exists
        Path(self.__codedir).mkdir(parents=True, exist_ok=True)

    def register(self, name: str, task_code: str, description: str, is_test_task: bool) -> None:
        """Register a new task by storing its code and description.

        Args:
            name: Task name (used as filename prefix).
            task_code: Python code for the task.
            description: Task description.

        Raises:
            ValueError: If task name is empty or contains invalid characters.
        """
        if not name or not name.replace("_", "").replace("-", "").isalnum():
            raise ValueError("Task name must be alphanumeric (with _ or - allowed)")

        code_file = os.path.join(self.__codedir, f"{name}.py")
        desc_file = os.path.join(self.__codedir, f"{name}.desc")
        props_file = os.path.join(self.__codedir, f"{name}.props")

        # Write task code to Python file
        with open(code_file, "w", encoding="utf-8") as f:
            f.write(task_code)

        # Write description to description file
        with open(desc_file, "w", encoding="utf-8") as f:
            f.write(description)

        # Write props file
        with open(props_file, "w", encoding="utf-8") as f:
            f.write(json.dumps({"is_test_task": is_test_task}))


    def deregister(self, name: str) -> None:
        """Remove a task and its description.

        Args:
            name: Task name to remove.

        Raises:
            FileNotFoundError: If task is not registered.
        """
        code_file = os.path.join(self.__codedir, f"{name}.py")
        desc_file = os.path.join(self.__codedir, f"{name}.desc")
        props_file = os.path.join(self.__codedir, f"{name}.props")

        # Check if files exist
        if not os.path.exists(code_file) or not os.path.exists(desc_file):
            raise FileNotFoundError(f"Task '{name}' is not registered")

        # Remove all files
        os.remove(code_file)
        os.remove(desc_file)
        os.remove(props_file)


    def get(self, name: str) -> tuple[str, str, Dict[str, Any]]:
        """Retrieve task code and description.

        Args:
            name: Task name to retrieve.

        Returns:
            Tuple of (task_code, description).

        Raises:
            FileNotFoundError: If task is not registered.
        """
        code_file = os.path.join(self.__codedir, f"{name}.py")
        desc_file = os.path.join(self.__codedir, f"{name}.desc")
        props_file = os.path.join(self.__codedir, f"{name}.props")

        if not os.path.exists(code_file) or not os.path.exists(desc_file):
            raise FileNotFoundError(f"Task '{name}' is not registered")

        with open(code_file, "r", encoding="utf-8") as f:
            task_code = f.read()

        with open(desc_file, "r", encoding="utf-8") as f:
            description = f.read()

        with open(props_file, "r", encoding="utf-8") as f:
            props = json.load(f)

        return task_code, description, props

    def list(self) -> list[str]:
        """List all registered task names.

        Returns:
            List of task names (without .py or .desc extension).
        """
        tasks = set()
        for filename in os.listdir(self.__codedir):
            if filename.endswith(".py"):
                task_name = filename[:-3]  # Remove .py extension
                tasks.add(task_name)
        return sorted(list(tasks))


    def exists(self, name: str) -> bool:
        """Check if a task is registered.

        Args:
            name: Task name to check.

        Returns:
            True if task exists, False otherwise.
        """
        code_file = os.path.join(self.__codedir, f"{name}.py")
        desc_file = os.path.join(self.__codedir, f"{name}.desc")
        return os.path.exists(code_file) and os.path.exists(desc_file)


    def backup(self) -> str | None:
        """Create a zip backup of all registered task files inside the code directory.

        Returns:
            The absolute path to the created backup zip file, or None if it failed.
        """
        # Generate timestamp for the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_filename = f"registry_backup_{timestamp}.zip"

        # Combine the directory path with the filename
        zip_filepath = os.path.join(self.__codedir, zip_filename)

        try:
            # Open ZipFile in write mode with compression at the target path
            with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zip_file:

                # Check if the directory exists and has files
                if os.path.exists(self.__codedir):
                    for filename in os.listdir(self.__codedir):
                        # Only backup .py and .desc files (this safely ignores older .zip backups!)
                        if filename.endswith(".py") or filename.endswith(".desc"):
                            file_path = os.path.join(self.__codedir, filename)
                            # arcname=filename ensures the zip doesn't contain the full folder path
                            zip_file.write(file_path, arcname=filename)

            backup_path = os.path.abspath(zip_filepath)
            logging.info(f"Backup successfully created at: {backup_path}")
            return backup_path

        except Exception as e:
            logging.error(f"Error creating backup: {e}")
            return None

class TaskRegistry:

    def __init__(self, code_registry: CodeRegistry):
        """Initialize scanner with a task registry.

        Args:
            code_registry: Registry containing registered task code files.
        """
        self.is_running = False
        self.__code_registry = code_registry
        self.tasks: Dict[str, Task] = dict()

    def stop(self):
        self.is_running = False
        return self

    def start(self):
        self.is_running = True
        # Log existing registered tasks at startup
        existing_tasks = self.__code_registry.list()
        if existing_tasks:
            logging.info(f"TaskRegistry started. Registered tasks: {', '.join(existing_tasks)}")
        else:
            logging.info("TaskRegistry started. No registered tasks found.")
        Thread(target=self.__loop, daemon=True).start()
        return self

    def reload(self):
        self.__scan()

    def __loop(self):
        while self.is_running:
            try:
                self.__scan()
            except Exception as e:
                print(f"Error in task registry: {e}")
            sleep(10)

    def __scan(self):
        """Scan registry for registered tasks and load source-code task functions."""
        new_tasks = {}
        for task_name in self.__code_registry.list():
            task = self.__task_from_registry(task_name)
            if task is not None:
                new_tasks[task_name] = task
        self.tasks =new_tasks

    def __task_from_registry(self, task_name: str) -> Optional[Task]:
        """Load and instantiate a task from the registry.

        Args:
            task_name: Name of the registered task to load.
        """
        try:
            # Retrieve task code from registry
            task_code, task_description, task_props = self.__code_registry.get(task_name)
            if task_name in self.tasks.keys():
                if task_code == self.tasks[task_name].code:
                    return None

            # Execute task code in an isolated namespace.
            namespace: dict[str, object] = {"__name__": task_name}
            exec(task_code, namespace)

            cron_getter = namespace.get("cron_cron")
            execute = namespace.get("execute")

            if not callable(cron_getter):
                raise ValueError("Missing required function 'cron_cron() -> str'")

            if not callable(execute):
                raise ValueError("Missing required function 'execute(store, mcp) -> None'")

            typed_cron_getter = cast(Callable[[], str], cron_getter)
            typed_execute = cast(Callable[[Mapping[str, Any], Mapping[str, Any]], None], execute)
            return Task.create(task_name, task_code, task_description, task_props, typed_cron_getter, typed_execute)
        except Exception as e:
            logging.warning(f"Warning: Could not load task '{task_name}' from registry: {e}")
