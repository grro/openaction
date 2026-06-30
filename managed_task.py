import logging
import io
import time
import textwrap
from threading import Thread, Event
from contextlib import redirect_stdout, redirect_stderr
from dataclasses import field, InitVar, dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable

from api.task import BackgroundTask, AdhocTask
from api.store import Store
from api.eventlog import EventLog
from api.environment import Environment
from simple_environment import EnvironmentImpl
from simple_store import SimpleStore
from cron_expression import CronExpression


logger = logging.getLogger(__name__)


class TaskState(str, Enum):
    """Lifecycle phases of a :class:`ManagedTask`'s background loop.

    ``DEGRADED`` means the loop is running and ticking, but ``on_activate()``
    exceeded its timeout (or has not returned yet). The task still executes;
    user code is expected to (re)connect lazily inside ``on_execute``.
    """
    INSTANTIATED = "instantiated"
    ACTIVATING = "activating"
    ACTIVE = "active"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"





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
        # Lifecycle / liveness bookkeeping.
        self.state: TaskState = TaskState.INSTANTIATED
        self.activated_at: Optional[datetime] = None
        # Real liveness signal: refreshed on EVERY loop iteration, independent
        # of cron, so even event-only tasks (empty cron) prove the loop is alive.
        self.last_loop_tick_at: Optional[datetime] = None
        self.last_success_at: Optional[datetime] = None
        self.consecutive_failures: int = 0
        self._loop_thread: Optional[Thread] = None
        # Monotonically increasing activation id. Each loop captures its
        # generation; a fresh activate() bumps it so a stale loop exits instead
        # of running in parallel (guards the activate/deactivate restart race).
        self._generation: int = 0
        self._activate_timeout_s: float = float(self.props.get("activate_timeout", 30))
        self._deactivate_timeout_s: float = float(self.props.get("deactivate_timeout", 15))
        # Per-execution watchdog. 0 disables it (default), preserving the
        # original behaviour for legitimately long-running tasks. When > 0, an
        # on_execute exceeding it is abandoned so the loop keeps ticking.
        self._execute_timeout_s: float = float(self.props.get("execute_timeout", 0))
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
        already active are silently ignored. Each start bumps the generation
        counter and stores the thread handle so a later :meth:`await_stopped`
        can join it and a stale loop can detect it has been superseded.
        """
        if not self.is_activated:
            self.is_activated = True
            self._generation += 1
            self._wakeup.clear()
            self._loop_thread = Thread(target=self._loop, args=(self._generation,),
                                       daemon=True, name=f"task-{self.name}")
            self._loop_thread.start()

    def await_stopped(self, timeout: Optional[float] = None) -> bool:
        """
        Block until the background loop thread has actually terminated.

        Restart safety: a caller replacing a task must wait here between
        :meth:`deactivate` and a fresh :meth:`activate`. Otherwise ``activate()``
        could flip ``is_activated`` back to ``True`` before the old loop reads
        it, leaving two loop threads running in parallel.

        Returns ``True`` if the thread is stopped (or was never running),
        ``False`` if it is still alive after ``timeout`` seconds.
        """
        if timeout is None:
            timeout = self._deactivate_timeout_s
        t = self._loop_thread
        if t is None or not t.is_alive():
            return True
        t.join(timeout=timeout)
        return not t.is_alive()

    def _call_with_timeout(self, fn: Callable[[], Any], timeout: float, label: str) -> bool:
        """
        Run ``fn`` in a dedicated daemon thread, returning ``True`` if it
        finished within ``timeout`` seconds.

        ``timeout <= 0`` runs ``fn`` inline (no watchdog). On timeout the
        worker thread is *not* killed (Python cannot force-kill threads); it
        keeps running in the background while the caller regains control, so a
        single hung call can never freeze the loop. Exceptions raised by ``fn``
        are swallowed here (they are already logged/recorded downstream).
        """
        if timeout <= 0:
            fn()
            return True

        done = Event()

        def _runner() -> None:
            try:
                fn()
            except Exception:
                pass
            finally:
                done.set()

        Thread(target=_runner, name=f"{label}-{self.name}", daemon=True).start()
        return done.wait(timeout=timeout)

    def _run_on_activate_guarded(self) -> bool:
        """
        Call ``on_activate()`` in a dedicated daemon thread with a timeout.

        Returns ``True`` if it returned in time, ``False`` on timeout. On
        timeout the loop still proceeds (the hung ``on_activate`` thread keeps
        running; if it ever completes it corrects the state from ``DEGRADED``
        back to ``ACTIVE``). This structurally prevents a blocking
        ``on_activate`` from ever killing the loop.
        """
        def _activate() -> None:
            try:
                self._task_instance.on_activate()
                if self.state in (TaskState.ACTIVATING, TaskState.DEGRADED):
                    self.state = TaskState.ACTIVE
            except Exception as e:
                logger.error(f"Error activating task '{self.name}': {e}", exc_info=True)

        if not self._call_with_timeout(_activate, self._activate_timeout_s, "activate"):
            logger.error(
                f"on_activate() of '{self.name}' exceeded {self._activate_timeout_s:.0f}s; "
                f"continuing in DEGRADED state. on_activate should be non-blocking."
            )
            return False
        return True

    def _loop(self, generation: int) -> None:
        """
        Main background loop.

        Runs ``on_activate`` (guarded by a timeout), optionally an initial
        ``run_on_start`` execution, then ticks every few seconds evaluating the
        cron expression. Each cron execution is itself guarded by an optional
        per-execution watchdog. Terminates after ``on_deactivate`` once
        :attr:`is_activated` becomes ``False`` or a newer generation supersedes
        this loop.

        :attr:`last_loop_tick_at` is refreshed at the start of every iteration,
        independent of cron, and serves as the authoritative liveness signal.

        All user-thrown exceptions are logged but never propagated, so a
        misbehaving task cannot kill the loop.
        """
        self.state = TaskState.ACTIVATING
        if not self._run_on_activate_guarded():
            self.state = TaskState.DEGRADED
        elif self.state == TaskState.ACTIVATING:
            self.state = TaskState.ACTIVE
        self.activated_at = datetime.now()

        if self.run_on_start:
            try:
                self._task_instance.on_execute_fw("run_on_start", list())
            except Exception as e:
                logger.warning(f"Error in initial execution for task '{self.name}': {e}", exc_info=True)

        # Generation guard: exit immediately if a newer activate() superseded us.
        while self.is_activated and self._generation == generation:
            # Liveness signal, refreshed unconditionally each iteration.
            self.last_loop_tick_at = datetime.now()
            if self.cron != "":
                try:
                    if self.cron_expression.should_run(self.last_cron_attempt_at, self.last_cron_failure_at):
                        completed = self._call_with_timeout(
                            lambda: self._task_instance.on_execute_fw("cron", list()),
                            self._execute_timeout_s,
                            "execute",
                        )
                        if not completed:
                            self.last_cron_failure_at = datetime.now()
                            self.consecutive_failures += 1
                            logger.error(
                                f"on_execute of '{self.name}' exceeded "
                                f"{self._execute_timeout_s:.0f}s; abandoned, loop continues."
                            )
                except Exception as e:
                    self.last_cron_failure_at = datetime.now()
                    logger.warning(f"Error in cron loop for task '{self.name}': {e}", exc_info=True)
                finally:
                    self.last_cron_attempt_at = datetime.now()

            # `wait` returns early if `deactivate()` sets the event, giving us
            # a responsive shutdown without busy-polling `is_activated`.
            self._wakeup.wait(timeout=4)

        # A superseded loop must not run on_deactivate (the new loop owns the
        # task instance); only the still-current generation tears down.
        if self._generation != generation:
            return

        self.state = TaskState.STOPPING
        self._call_with_timeout(self._run_on_deactivate, self._deactivate_timeout_s, "deactivate")
        self.state = TaskState.STOPPED

    def _run_on_deactivate(self) -> None:
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

            # 5. Track success / failure streak for health reporting. On success
            #    clear the cron failure throttle so the next due tick runs
            #    promptly instead of waiting out the post-failure backoff.
            if task_result.is_success():
                self.last_success_at = task_result.date
                self.consecutive_failures = 0
                self.last_cron_failure_at = None
            else:
                self.consecutive_failures += 1

            # 6. Log if necessary
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

    # ------------------------------------------------------------------
    # Health / liveness
    # ------------------------------------------------------------------

    def is_healthy(self, max_tick_age_s: float = 150.0) -> bool:
        """
        ``True`` if the background loop is alive and ticking.

        A deactivated task is never healthy. During start-up (no tick yet) the
        task is considered healthy only while still ``ACTIVATING``. Otherwise
        the loop must have ticked within ``max_tick_age_s`` seconds. The
        threshold deliberately exceeds a pathologically long ``on_execute`` so
        a task currently stuck inside its own execution (and protected by the
        per-execution watchdog) is not prematurely flagged.
        """
        if not self.is_activated:
            return False
        if self.last_loop_tick_at is None:
            return self.state == TaskState.ACTIVATING
        return (datetime.now() - self.last_loop_tick_at).total_seconds() <= max_tick_age_s

    def health(self, max_tick_age_s: float = 150.0) -> Dict[str, Any]:
        """Return a JSON-serialisable health snapshot for monitoring."""
        now = datetime.now()

        def age(dt: Optional[datetime]) -> Optional[float]:
            return None if dt is None else (now - dt).total_seconds()

        last = self.last_executions[-1] if self.last_executions else None
        return {
            "name": self.name,
            "state": self.state.value,
            "is_activated": self.is_activated,
            "healthy": self.is_healthy(max_tick_age_s),
            "loop_tick_age_s": age(self.last_loop_tick_at),
            "cron": self.cron,
            "last_exec_at": last.date.isoformat() if last else None,
            "last_exec_trigger": getattr(last, "trigger", None) if last else None,
            "last_exec_ok": last.is_success() if last else None,
            "last_success_age_s": age(self.last_success_at),
            "consecutive_failures": self.consecutive_failures,
        }

    def __str__(self) -> str:
        """Render a multi-line, human-readable summary of the task and its last run."""
        state = self.state.value.upper() if self.is_activated else "INACTIVE"

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
