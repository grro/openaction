import logging
import threading
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from threading import Event
from typing import Dict, Set

from code_repository import CodeRepository
from simple_store import SimpleStore
from managed_task import ManagedTask, ManagedTaskFactory

logger = logging.getLogger(__name__)


# Characters that are forbidden in task names (filesystem- and URL-unsafe).
_FORBIDDEN_NAME_CHARS = (',', '.', ' ', '/', '\\', ':', '*', '?', '"', '<', '>', '|')



class ManagedTaskRepository:
    """
    Registry that owns the live :class:`ManagedTask` instances and keeps
    them in sync with the on-disk :class:`CodeRepository`.

    Responsibilities:

      * **Registration**: :meth:`register` validates the task name,
        builds a fresh task via the :class:`ManagedTaskFactory`,
        persists its code/description/props through the
        :class:`CodeRepository`, and activates it.
      * **Deregistration**: :meth:`deregister` deactivates the task,
        clears its persistent state, removes it from the in-memory
        registry and deletes its on-disk image.
      * **Background sync**: After :meth:`start`, a daemon thread
        periodically calls :meth:`_scan` (load new / updated tasks,
        drop ones that disappeared from disk) and :meth:`_clean_up`
        (drop tasks whose TTL has expired).
    """

    def __init__(self, code_dir: str, task_factory: ManagedTaskFactory, store: SimpleStore, autobackup: bool = True):
        """
        Args:
            code_dir: Directory under which the :class:`CodeRepository`
                stores task source code, descriptions and props.
            task_factory: Factory used to instantiate
                :class:`ManagedTask` objects from either fresh input or
                restored on-disk images.
            store: Shared persistent key/value store (currently kept
                as a reference for future use by repository-level
                metadata).
        """
        self._is_running = False
        self._autobackup = autobackup
        self._stop_event = Event()
        self._code_repository = CodeRepository(codedir=code_dir)
        self._task_factory = task_factory
        self._store = store
        self.tasks: Dict[str, ManagedTask] = {}
        logger.info("TaskRepository initialized")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register(self, name: str, code: str, description: str, run_on_start: bool, cron: str) -> None:
        """
        Create, persist and activate a new task.

        Args:
            name: Unique task identifier. Must not start with the
                reserved ``"test_"`` prefix and must not contain any
                filesystem-unsafe characters.
            code: Python source code of the task.
            description: Human-readable summary.
            run_on_start: If ``True``, the task fires once immediately
                after activation in addition to its cron schedule.
            cron: Cron expression (may be empty for purely
                ``run_on_start`` / manual tasks).

        Raises:
            ValueError: If ``name`` violates the naming constraints.
        """
        self._validate_name(name)

        task = self._task_factory.create(name, False, False, code, run_on_start, cron, description, None)

        # Persist the code/props on disk so it survives restarts and can
        # be discovered by the background scan on other instances.
        image = self._code_repository.create_image(name)
        image.write_data(task.code, description, task.props)

        self._add_task(name, task, reason="newly registered")

    def deregister(self, name: str, reason: str) -> None:
        """
        Stop and remove a task by name (no-op if it is not registered).

        The on-disk image is always deleted, even if the task was not
        currently in memory, to keep storage and runtime state aligned.
        """
        task = self.tasks.pop(name, None)
        if task:
            task.deactivate()
            task.reset()
            logger.info(f"Task '{name}' has been deregistered (Reason: {reason})")
        self._code_repository.delete_image(name)

    def start(self) -> "ManagedTaskRepository":
        """Start the background sync thread. Returns ``self`` for chaining."""
        self._is_running = True
        self._stop_event.clear()
        threading.Thread(target=self._loop, daemon=True, name="TaskRepoSync").start()
        return self

    def stop(self) -> None:
        """Request a graceful shutdown of the background sync thread."""
        self._is_running = False
        # Wake the loop immediately so we don't have to wait out the current sleep.
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_name(name: str) -> None:
        """Reject reserved prefixes and any filesystem-unsafe character."""
        if name.startswith("test_"):
            raise ValueError("Task name must not start with the reserved 'test_' prefix.")
        for char in _FORBIDDEN_NAME_CHARS:
            if char in name:
                raise ValueError(f"Task name cannot contain '{char}'")

    def _add_task(self, name: str, task: ManagedTask, reason: str) -> None:
        """
        Insert or replace a task in the registry, then activate it.

        A task is replaced if one with the same name already exists but
        carries a different ``created_at`` timestamp (i.e. a newer
        revision was loaded from disk).
        """
        if task is None:
            logger.warning(f"Failed to add task '{name}'. None after registration.")
            return

        existing = self.tasks.get(name)
        is_new = existing is None
        is_updated = existing is not None and existing.created_at != task.created_at

        if not (is_new or is_updated):
            return

        # If we are replacing an existing task, stop the old one cleanly first
        # so it doesn't keep running in parallel with the new revision.
        if is_updated:
            existing.deactivate()

        self.tasks[name] = task

        action = "added" if is_new else "re-added"
        suffix = " with load on start" if task.run_on_start else ""
        logger.info(f"Task '{name}' {action} to registry{suffix} (Reason: {reason})")

        task.activate()

    def _loop(self) -> None:
        """Background loop: periodic scan + TTL clean-up, interruptible via stop()."""
        # Delay the first scan so the rest of the system has time to start up.
        if self._stop_event.wait(timeout=10):
            return

        while self._is_running:
            try:
                self._scan()
            except Exception as e:
                logger.exception(f"Unexpected error in periodic scan: {e}")
            try:
                self._clean_up()
            except Exception as e:
                logger.exception(f"Unexpected error in periodic clean up: {e}")

            if self._autobackup:
                try:
                    self._perform_autobackup()
                except Exception as e:
                    logger.exception(f"Unexpected error during code repository backup: {e}")

            # `wait()` returns True if stop() was called, in which case we exit.
            if self._stop_event.wait(timeout=60):
                break

        logger.info("TaskRepository stopped.")

    def _scan(self) -> None:
        """
        Reconcile the in-memory registry with the on-disk code images.

        For each image on disk: load it, add it if new, or replace the
        in-memory task if the on-disk revision is newer. Afterwards,
        deregister every in-memory task whose image no longer exists on
        disk.
        """
        seen_names: Set[str] = set()

        for image in self._code_repository.list_images():
            code, desc, props = image.read()
            task = self._task_factory.restore(image.unit_name, False, False, code, desc, props)
            seen_names.add(task.name)

            existing = self.tasks.get(task.name)
            if existing is None or task.created_at > existing.created_at:
                self._add_task(task.name, task, reason="loaded from code repo")

        # Drop tasks that vanished from disk while we were running.
        for name in list(self.tasks.keys()):
            if name not in seen_names:
                self.deregister(name, reason="no longer exists in code repo")

    def _clean_up(self) -> None:
        """Deregister every task whose TTL (``valid_to``) has expired."""
        for image in self._code_repository.list_images():
            code, desc, props = image.read()
            task = self._task_factory.restore(image.unit_name, False, False, code, desc, props)
            if task.is_expired():
                self.deregister(task.name, reason="TTL expired")

    def _perform_autobackup(self) -> None:
        latest_backup = datetime.strptime(self._store.get("__system_latest_backup", "1970-01-01"), "%Y-%m-%d")
        if datetime.now() > latest_backup + timedelta(hours=7):
            filepath = self._code_repository.backup(f"backup_{datetime.now().strftime('%Y%m%d')}.zip")
            self._store.put("__system_latest_backup", datetime.now().strftime("%Y-%m-%d"))

            self._process_monthly_backup(filepath)
            self._cleanup_old_daily_backups()

    def _process_monthly_backup(self, backup_p: Path) -> None:
        current_month_str = datetime.now().strftime("%Y%m")
        monthly_p = backup_p.parent / f"backup_monthly_{current_month_str}.zip"

        if not monthly_p.exists():
            temp_p = backup_p.parent / f"backup_monthly_tmp_{datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
            try:
                shutil.copy(backup_p, temp_p)
                temp_p.replace(monthly_p)
                logger.info(f"Created monthly backup: {monthly_p.name}")
            except Exception as e:
                logger.warning(f"Failed to create monthly backup {monthly_p.name}: {e}")
            finally:
                if temp_p.exists():
                    try:
                        temp_p.unlink()
                    except Exception:
                        pass

    def _cleanup_old_daily_backups(self) -> None:
        now = datetime.now()
        backup_files_list = self._code_repository.backupfiles()  # type: ignore
        for file_str in backup_files_list:
            p = Path(file_str)
            name_part = p.stem.replace("backup_", "")
            try:
                # Attempt to parse names like "backup_20260525"
                dt = datetime.strptime(name_part.split("_")[0], "%Y%m%d")
            except ValueError:
                continue

            if now - dt > timedelta(days=7):
                try:
                    p.unlink()
                    logger.info(f"Deleted old daily backup: {p.name}")
                except Exception as e:
                    logger.warning(f"Failed to delete old daily backup {p.name}: {e}")

