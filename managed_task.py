import logging
import io
import time
import sys
import textwrap
from threading import Thread, Event
from contextlib import redirect_stdout, redirect_stderr, contextmanager
from dataclasses import field, InitVar, dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable, Iterator, cast

from api.task import BackgroundTask, AdhocTask
from api.store import Store
from api.eventlog import EventLog
from api.environment import Environment
from simple_environment import EnvironmentImpl
from simple_store import SimpleStore, ScopedStore
from cron_expression import CronExpression


logger = logging.getLogger(__name__)





@dataclass
class TaskResult:
    """
    Immutable record describing the outcome of a single task execution.

    A ``TaskResult`` is created automatically by :class:`ManagedTask` for
    every ``on_execute`` / ``on_execute_with_params`` invocation. It
    captures:

      * which trigger caused the execution (e.g. ``"cron"``, ``"app"``,
        ``"run_on_start"`` or a user-defined label),
      * how long the execution took,
      * the return value (``result``) of the user's task code,
      * any captured stdout/stderr output (``output``),
      * and the error message (``error``) if the execution failed.

    The owning task's name is derived automatically from the supplied
    :class:`ManagedTask` instance (passed as an ``InitVar``, so it is not
    stored as a regular field).
    """

    # InitVar means 'task' is passed to __init__ but not stored as a class attribute.
    task: InitVar['ManagedTask']
    trigger: str
    elapsed: timedelta
    events: List[Event] = field(default_factory=list)

    result: str | None = None
    output: str | None = None
    error: str | None = None

    # These fields are populated automatically after initialization.
    name: str = field(init=False)
    date: datetime = field(default_factory=lambda: datetime.now(), init=False)

    def __post_init__(self, task: 'ManagedTask') -> None:
        self.name = task.name

    def is_success(self) -> bool:
        return self.error is None

    def __str__(self) -> str:
        status = "❌ FAILED" if self.error else "✅ SUCCESS"
        elapsed_sec = f"{self.elapsed.total_seconds():.3f}s"

        # Primary header line.
        parts = [f"{self.name} executed [{status}] | Trigger: '{self.trigger}' | Elapsed: {elapsed_sec}"]

        # Append titled sections; lists are rendered entry-by-entry for readability.
        def format_block(title: str, content: Any | None) -> None:
            if content is None:
                return

            parts.append(f"--- {title} ---")

            if isinstance(content, list):
                if not content:
                    parts.append("  | (none)")
                    return

                for item in content:
                    parts.append(textwrap.indent(str(item).strip(), "  | "))
                return

            rendered = str(content).strip()
            parts.append(textwrap.indent(rendered if rendered else "(empty)", "  | "))

        if self.error:
            format_block("Error", self.error)
        else:
            format_block("Success", self.result)

        format_block("Events", self.events)

        format_block("Output (std, error out)", self.output)

        return "\n".join(parts)


class ManagedTask:
    """
    Runtime container that owns the lifecycle of a single user-defined task.

    A ``ManagedTask`` is responsible for:

      1. **Instantiation**: Compiling the user's Python source (``code``)
         and locating exactly one class that inherits from either
         :class:`BackgroundTask` or :class:`AdhocTask`. A thin subclass
         (``_WrappedTask``) is generated on the fly so that every
         execution is funneled through :meth:`_execute_sync`, which
         captures output and records a :class:`TaskResult`.

      2. **Background loop** (only relevant for background tasks):
         :meth:`activate` starts a daemon thread that

           * calls ``on_activate()`` once,
           * optionally fires an initial ``run_on_start`` execution,
           * then loops, checking the configured cron expression and
             executing the task whenever it is due,
           * and finally calls ``on_deactivate()`` once
             :meth:`deactivate` is invoked.

      3. **Manual execution**: :meth:`execute_manually` allows the host
         to fire the task on demand (e.g. from an HTTP endpoint or a UI
         action), routing the call through the same wrapping logic so
         the result is consistently recorded.

      4. **State exposure**: Persistent per-task state is reachable via
         :meth:`data` / :meth:`reset`; the most recent
         :class:`TaskResult` instances are kept in
         :attr:`last_executions` (bounded by
         :data:`MAX_EXECUTION_HISTORY`).
    """

    def __init__(self,
                 store: SimpleStore,
                 name: str,
                 is_ephemeral: bool,
                 is_test: bool,
                 code: str,
                 desc: str,
                 props: Dict[str, Any]):
        """
        Args:
            store: Persistent, per-task key/value store (scoped by ``name``).
            name: Unique task identifier (used for logging, store scoping
                and as the basis for the wrapper class name).
            is_ephemeral: If ``True``, the task is not persisted across restarts.
            is_test: Marks the task as a test/debug task (informational).
            code: Python source code defining exactly one task class.
            desc: Human-readable description shown in logs and UIs.
            props: Free-form configuration dictionary. Recognised keys are
                ``"cron"``, ``"run_on_start"``, ``"timeout"``,
                ``"created_at"`` and ``"valid_to"``.
        """
        self.name = name
        self.description = desc
        self.props = props
        self.code = code
        self.is_ephemeral = is_ephemeral
        self.is_test = is_test
        self.last_executions: List[TaskResult] = list()
        self.environment = EnvironmentImpl(store, self.name)
        self.is_activated = False
        self.cron_expression: CronExpression = CronExpression(self.cron)
        self.last_cron_attempt_at: datetime = None
        self.last_cron_failure_at: datetime = None
        # Event used to interrupt the loop's `wait()` on deactivation.
        self._wakeup = Event()
        self._task_instance = self.instantiate()

    def instantiate(self):
        """
        Compile the user script, locate its task class and return a
        wrapped instance.

        The wrapper subclass overrides ``on_execute`` (background tasks)
        or ``on_execute_with_params`` (ad-hoc tasks) so that every
        invocation is routed through :meth:`_execute_sync`. This
        guarantees uniform output capture, error handling and history
        bookkeeping, regardless of whether the call originated from the
        cron loop, a manual trigger, or user code calling the method
        directly.

        Returns:
            An instance of the dynamically generated ``_WrappedTask`` class.

        Raises:
            ValueError: If the script defines zero or more than one task
                class.
            SyntaxError: If the script cannot be compiled. The error is
                logged with line-numbered context before being re-raised.
        """
        self_outer = self
        try:
            # Inject required dependencies directly into the script's namespace
            # so the user code does not need to import them explicitly.
            self._namespace = {
                "__name__": f"task_{self.name}",
                "Store": Store,
                "EventLog": EventLog,
                "Environment": Environment,
                "BackgroundTask": BackgroundTask,
                "AdhocTask": AdhocTask,
            }

            # Compile and execute the user's script.
            compiled_code = compile(self.code, f"task_{self.name}.py", 'exec')
            exec(compiled_code, self._namespace)

            # Discover exactly one user-provided Task subclass.
            user_class, is_background = self._discover_task_class()

            # Dynamically subclass the detected user class to wrap on_execute /
            # on_execute_with_params. The original implementation is reached via
            # `base_class.on_execute(...)` rather than `super()` to avoid
            # recursion through our own override.
            base_class = user_class

            if is_background:
                class _WrappedTask(user_class):  # type: ignore[misc, valid-type]
                    def on_execute(self_inner, *args, **kwargs):
                        return self_outer._execute_sync(
                            "app",
                            lambda: base_class.on_execute(self_inner, *args, **kwargs),
                        )

                    def on_execute_fw(self_inner, trigger: str, params: List[str], *args, **kwargs) -> TaskResult:
                        return self_outer._execute_sync(
                            trigger,
                            lambda: base_class.on_execute(self_inner, *args, **kwargs),
                        )
            else:
                class _WrappedTask(user_class):  # type: ignore[misc, valid-type]
                    # Ad-hoc tasks don't have a meaningful activation lifecycle,
                    # so we provide no-op defaults to keep the call sites uniform.
                    def on_activate(self) -> None:
                        pass

                    def on_deactivate(self) -> None:
                        pass

                    def on_execute_with_params(self_inner, params: List[str], *args, **kwargs):
                        return self_outer._execute_sync(
                            "app",
                            lambda: base_class.on_execute_with_params(self_inner, params, *args, **kwargs),
                        )

                    def on_execute_fw(self_inner, trigger: str, params: List[str], *args, **kwargs) -> TaskResult:
                        return self_outer._execute_sync(
                            trigger,
                            lambda: base_class.on_execute_with_params(self_inner, params, *args, **kwargs),
                        )

            # Preserve the @when decorator metadata from the original method on
            # the wrapper so external code introspecting triggers still works.
            original_method = getattr(user_class, "on_execute", None)
            if original_method is not None and hasattr(original_method, "__triggers__"):
                _WrappedTask.on_execute.__triggers__ = original_method.__triggers__

            _WrappedTask.__name__ = f"Wrapped{user_class.__name__}"
            _WrappedTask.__qualname__ = _WrappedTask.__name__

            return _WrappedTask(self.environment)

        except SyntaxError as e:
            self._log_syntax_error(e)
            raise

        except Exception as e:
            # Catch-all for any other unforeseen compilation, instantiation, or setup errors.
            logger.error(f"Unexpected error instantiating task '{self.name}': {e}", exc_info=True)
            raise

    def _discover_task_class(self) -> tuple[type, bool]:
        """
        Scan the executed script namespace for the single user task class.

        Returns:
            A tuple ``(user_class, is_background)`` where ``is_background``
            is ``True`` if the class inherits from :class:`BackgroundTask`,
            ``False`` if it inherits from :class:`AdhocTask`.

        Raises:
            ValueError: If no matching class, or more than one matching
                class, is found.
        """
        user_class: Optional[type] = None
        is_background = False

        for obj in self._namespace.values():
            if not isinstance(obj, type):
                continue

            # Detect background task subclasses (but skip the abstract base itself).
            if issubclass(obj, BackgroundTask) and obj is not BackgroundTask:
                if user_class is not None:
                    raise ValueError(
                        f"Multiple classes implementing the 'Task' interface found in "
                        f"the script for '{self.name}'. Please ensure only one class inherits from 'Task'."
                    )
                user_class = obj
                is_background = True

            # Detect ad-hoc task subclasses (but skip the abstract base itself).
            elif issubclass(obj, AdhocTask) and obj is not AdhocTask:
                if user_class is not None:
                    raise ValueError(
                        f"Multiple classes implementing the 'Task' interface found in "
                        f"the script for '{self.name}'. Please ensure only one class inherits from 'Task'."
                    )
                user_class = obj
                is_background = False

        if user_class is None:
            raise ValueError(
                f"No class implementing the 'Task' interface found in the script for '{self.name}'."
            )

        return user_class, is_background

    def _log_syntax_error(self, error: SyntaxError) -> None:
        """Render a :class:`SyntaxError` together with a line-numbered code listing."""
        lines = self.code.splitlines()
        # Fallback to line 0 if lineno is None (e.g., unexpected EOF).
        error_line = error.lineno if error.lineno is not None else 0

        numbered_code = []
        for i, line in enumerate(lines):
            line_num = i + 1
            # Add a visual pointer (>>) to the exact line that failed.
            marker = ">> " if line_num == error_line else "   "
            numbered_code.append(f"{marker}{line_num:03d} | {line}")

        formatted_code = "\n".join(numbered_code)
        logger.error(
            f"Syntax error compiling task '{self.name}' at line {error_line}: {error.msg}\n"
            f"--- Source Code ---\n"
            f"{formatted_code}\n"
            f"-------------------"
        )

    @property
    def _last_execution_state(self) -> str:
        if not self.last_executions:
            return "?"
        else:
            return "success" if self.last_executions[-1].is_success() else "failure"

    def activate(self) -> None:
        """
        Start the background loop in a daemon thread.

        Safe to call multiple times: subsequent calls while the task is
        already active are silently ignored. The wakeup event is cleared
        before starting so that an immediate :meth:`deactivate` afterwards
        will still take effect.
        """
        if not self.is_activated:
            self.is_activated = True
            self._wakeup.clear()
            Thread(target=self._loop, daemon=True, name=f"task-{self.name}").start()

    def _loop(self) -> None:
        """
        Main background loop.

        Runs ``on_activate``, optionally an initial ``run_on_start``
        execution, then ticks at :data:`LOOP_TICK_SECONDS` evaluating the
        cron expression. Terminates after ``on_deactivate`` once
        :attr:`is_activated` becomes ``False``.

        All user-thrown exceptions are logged but never propagated, so a
        misbehaving task cannot kill the loop.
        """
        try:
            self._task_instance.on_activate()
        except Exception as e:
            logger.error(f"Error activating task '{self.name}': {e}", exc_info=True)

        if self.run_on_start:
            try:
                self._task_instance.on_execute_fw("run_on_start", list())
            except Exception as e:
                logger.warning(f"Error in initial execution for task '{self.name}': {e}", exc_info=True)

        while self.is_activated:
            if self.cron != "":
                try:
                    if self.cron_expression.should_run(self.last_cron_attempt_at, self.last_cron_failure_at):
                        task_result = self._task_instance.on_execute_fw("cron", list())
                except Exception as e:
                    self.last_cron_failure_at = datetime.now()
                    logger.warning(f"Error in cron loop for task '{self.name}': {e}", exc_info=True)
                finally:
                    self.last_cron_attempt_at = datetime.now()

            # `wait` returns early if `deactivate()` sets the event, giving us
            # a responsive shutdown without busy-polling `is_activated`.
            self._wakeup.wait(timeout=4)

        try:
            self._task_instance.on_deactivate()
        except Exception as e:
            logger.error(f"Error deactivating task '{self.name}': {e}", exc_info=True)

    def deactivate(self) -> None:
        """Signal the background loop to stop and wake it up immediately."""
        self.is_activated = False
        self._wakeup.set()

    def __del__(self):
        # Best-effort cleanup; exceptions during interpreter shutdown are ignored.
        try:
            self.deactivate()
        except Exception:
            pass

    def execute_manually(self, trigger: str, params: List[str]) -> TaskResult:
        """
        Run the task once, synchronously, from an external caller.

        Args:
            trigger: Free-form label describing why the task was fired
                (recorded on the resulting :class:`TaskResult`).
            params: Parameter list forwarded to ad-hoc tasks. Ignored by
                background tasks.

        Returns:
            The :class:`TaskResult` produced by the execution.
        """
        return self._task_instance.on_execute_fw(trigger, params)

    @staticmethod
    @contextmanager
    def _root_logger_to_stdout_for_call() -> Iterator[None]:
        """Temporarily route root logger records to current stdout."""
        root_logger = logging.getLogger()

        # Store original state
        previous_handlers = root_logger.handlers[:]
        previous_level = root_logger.level
        previous_disabled = root_logger.disabled

        # Setup temporary stdout handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.NOTSET)

        # Safely clone the formatter from the primary handler if it exists
        if previous_handlers and previous_handlers[0].formatter:
            handler.setFormatter(previous_handlers[0].formatter)

        # Apply temporary state
        root_logger.handlers = [handler]
        root_logger.disabled = False

        try:
            yield
        finally:
            handler.flush()
            handler.close()
            # Restore original state
            root_logger.handlers = previous_handlers
            root_logger.level = previous_level
            root_logger.disabled = previous_disabled


    def _execute_sync(self, trigger: str, call: Callable[[], Any]) -> 'TaskResult':
        """
        Invoke ``call`` while capturing stdout/stderr and recording a
        TaskResult in last_executions.

        The captured output and the result/error are always logged at
        INFO level, regardless of success. Exceptions are re-raised after
        being recorded, so callers can react to failures.
        """
        eventlog_revision_before = self.environment.eventlog.revision
        execution_state_before = self._last_execution_state

        # perf_counter is strictly monotonic and immune to system clock updates
        start_time = time.perf_counter()
        output_buffer = io.StringIO()

        result = None
        error_msg = None

        try:
            # Capture stdout, stderr, and root logs
            #with redirect_stdout(output_buffer), redirect_stderr(output_buffer), self._root_logger_to_stdout_for_call():
            with redirect_stdout(output_buffer), redirect_stderr(output_buffer):
                result = call()
        except Exception as e:
            error_msg = str(e)
            raise  # Halts here, runs 'finally', then re-raises
        finally:
            # 1. Calculate duration once
            elapsed = timedelta(seconds=time.perf_counter() - start_time)

            # 2. Build kwargs cleanly
            kwargs = {"output": output_buffer.getvalue()}
            if error_msg is not None:
                kwargs["error"] = error_msg
            else:
                kwargs["result"] = result

            # 3. Instantiate TaskResult once
            events = self.environment.eventlog.events_since_revision(eventlog_revision_before)  # Force any pending events to be included in the revision
            task_result = TaskResult(self, trigger, elapsed, events,**kwargs)

            # 4. Update ring buffer history
            self.last_executions.append(task_result)
            if len(self.last_executions) > 10:
                del self.last_executions[0]

            # 5. Log if necessary
            if not task_result.is_success() or eventlog_revision_before != self.environment.eventlog.revision:
                logger.info(task_result)

        # Reached only if no exceptions occurred
        return task_result


    # ------------------------------------------------------------------
    # Persistent state helpers
    # ------------------------------------------------------------------

    def data(self) -> Dict[str, str]:
        """Return a snapshot of all persistent key/value pairs for this task."""
        return {key: self.environment.store.get(key) for key in self.environment.store.keys()}

    def reset(self) -> None:
        """Delete every persistent entry owned by this task."""
        for key in self.environment.store.keys():
            self.environment.store.delete(key)

    # ------------------------------------------------------------------
    # Read-only properties derived from the task's configuration / state
    # ------------------------------------------------------------------

    @property
    def is_background_task(self) -> bool:
        """``True`` if the underlying user class inherits from :class:`BackgroundTask`."""
        return isinstance(self._task_instance, BackgroundTask)

    @property
    def run_on_start(self) -> bool:
        """Whether the task should fire once immediately after activation."""
        return self.props.get("run_on_start", False)

    @property
    def cron(self) -> str:
        """Raw cron expression string from the task's properties (may be empty)."""
        return self.props.get("cron", "")

    @property
    def created_at(self) -> datetime:
        """Creation timestamp; falls back to ``now()`` if not set in props."""
        return datetime.fromisoformat(self.props.get("created_at", datetime.now().isoformat()))

    @property
    def valid_to(self) -> Optional[datetime]:
        """Expiration timestamp of the task's TTL, or a far-future sentinel if unset."""
        if "valid_to" in self.props:
            return datetime.fromisoformat(self.props["valid_to"])
        return datetime(2999, 1, 1)

    def is_expired(self) -> bool:
        """``True`` if ``now()`` is past :attr:`valid_to`."""
        return datetime.now() > self.valid_to

    def __str__(self) -> str:
        """Render a multi-line, human-readable summary of the task and its last run."""
        state = "ACTIVE" if self.is_activated else "INACTIVE"

        # 1. Header
        lines = [f"Task '{self.name}' | State: {state}"]

        # 2. Configuration Section
        triggers: List[str] = []
        if self.run_on_start:
            triggers.append("run_on_start")
        if self.cron:
            triggers.append(f"cron({self.cron})")
        trigger_str = " + ".join(triggers) if triggers else "manual_only"

        lines.append("--- Configuration ---")
        lines.append(f"  | Configured: {trigger_str}")

        # 3. Last Execution Section
        lines.append("--- Last Execution ---")
        if not self.last_executions:
            lines.append("  | Never executed")
        else:
            last_run = self.last_executions[-1]
            status = "❌ FAILED" if last_run.error else "✅ SUCCESS"
            run_date = last_run.date.strftime("%Y-%m-%d %H:%M:%S")
            trigger_used = getattr(last_run, 'trigger', 'unknown')

            lines.append(f"  | Status:     {status} ({last_run.elapsed.total_seconds():.3f}s) at {run_date}")
            lines.append(f"  | Fired By:   '{trigger_used}'")

            # Show up to 5 lines of error or output (whichever is present).
            detail_label, detail_text = ("Error", last_run.error) if last_run.error else ("Output", last_run.output)
            if detail_text:
                lines.append(f"  | {detail_label}:")
                detail_lines = str(detail_text).strip().splitlines()
                for line in detail_lines[:5]:
                    lines.append(f"  |   {line}")
                if len(detail_lines) > 5:
                    lines.append("  |   ... (truncated)")

        return "\n".join(lines)


class ManagedTaskFactory:
    """
    Convenience factory for constructing :class:`ManagedTask` instances.

    Two entry points are provided:

      * :meth:`create` -- for brand-new tasks. Builds a ``props``
        dictionary from explicit keyword arguments (cron expression,
        run-on-start flag, TTL, ...) and stamps it with the current
        creation time.

      * :meth:`restore` -- for tasks reloaded from persistent storage.
        The caller passes the already-stored ``props`` dictionary
        verbatim.
    """

    def __init__(self, store: SimpleStore, log_listener: Callable[[str], None]):
        self._store = store
        self._log_listener = log_listener

    def create(self,
               name: str,
               is_ephemeral: bool,
               is_test: bool,
               code: str,
               run_on_start: bool,
               cron: str,
               description: str,
               ttl: Optional[int] = None) -> ManagedTask:
        """Create a fresh :class:`ManagedTask` with freshly initialised properties."""
        props: Dict[str, Any] = {
            'created_at': datetime.now().isoformat(),
            'description': description,
            'run_on_start': run_on_start,
            'cron': cron,
            'ttl': -1 if ttl is None else ttl,
        }

        task = ManagedTask(
            self._store,
            name,
            is_ephemeral,
            is_test,
            code,
            description,
            props,
        )
        task.environment.eventlog.register_listener(self._log_listener)
        return task

    def restore(self,
                name: str,
                is_ephemeral: bool,
                is_test: bool,
                code: str,
                desc: str,
                props: Dict[str, Any]) -> ManagedTask:
        """Re-create a previously persisted :class:`ManagedTask` from raw props."""
        task = ManagedTask(
            self._store,
            name,
            is_ephemeral,
            is_test,
            code,
            desc,
            props,
        )
        task.environment.eventlog.register_listener(self._log_listener)
        return task
