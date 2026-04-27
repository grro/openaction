import json
import logging
import re
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any


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

    def register(self, name: str, task_code: str, description: str, is_test_task: bool) -> None:
        """Register a new task by storing its code and description.

        Args:
            name: Task name (used as filename prefix).
            task_code: Python code for the task.
            description: Task description.
            is_test_task: Boolean indicating if this is a test task.

        Raises:
            ValueError: If task name is empty or contains invalid characters.
        """
        # Cleaner validation using regex (allows alphanumeric, underscores, and hyphens)
        if not name or not re.match(r"^[\w\-]+$", name):
            raise ValueError("Task name must be alphanumeric (with _ or - allowed)")

        code_file, desc_file, props_file = self._get_paths(name)

        # Write files
        code_file.write_text(task_code, encoding="utf-8")
        desc_file.write_text(description, encoding="utf-8")
        props_file.write_text(json.dumps({"is_test_task": is_test_task}), encoding="utf-8")

    def deregister(self, name: str) -> None:
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

    def get(self, name: str) -> tuple[str, str, dict[str, Any]]:
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
            props = json.loads(props_file.read_text(encoding="utf-8"))
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
        return code_file.exists() and desc_file.exists()

    def list_backup(self) -> list[str]:
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
            logging.warning(f"Error listing backups: {e}")
            return []

    def backup(self) -> str | None:
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
            logging.info(f"Backup successfully created at: {backup_path}")
            return backup_path

        except Exception as e:
            logging.error(f"Error creating backup: {e}")
            # Clean up the corrupted zip file if it failed halfway through
            zip_filepath.unlink(missing_ok=True)
            return None