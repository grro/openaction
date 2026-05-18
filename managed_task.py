import logging
import io
import textwrap
from threading import Thread, Event
from contextlib import redirect_stdout, redirect_stderr
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import field, InitVar, dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


from api.store import Store
from api.task import BackgroundTask, AdhocTask
from cron import CronExpression
from store_impl import SimpleStore, ScopedStore


logger = logging.getLogger(__name__)



@dataclass
class TaskResult:
    # InitVar means 'task' is passed to __init__ but not stored as a class attribute
    task: InitVar['ManagedTask']
    trigger: str
    elapsed: timedelta

    # Modern Python (3.10+) type hinting for optional variables
    result: str | None = None
    output: str | None = None
    error: str | None = None

    # These fields are populated automatically after initialization
    name: str = field(init=False)
    date: datetime = field(default_factory=lambda: datetime.now(), init=False)

    def __post_init__(self, task: 'ManagedTask') -> None:
        self.name = task.name

    def __str__(self) -> str:
        status = "❌ FAILED" if self.error else "✅ SUCCESS"
        elapsed_sec = f"{self.elapsed.total_seconds():.3f}s"

        # Primary header line
        parts = [f"{self.name} executed [{status}] | Trigger: '{self.trigger}' | Elapsed: {elapsed_sec}"]

        # Helper function to format blocks
        def format_block(title: str, content: str | None) -> None:
            if content is not None:
                parts.append(f"--- {title} ---")
                parts.append(textwrap.indent(str(content).strip(), "  | "))

        if self.error:
            format_block("Error", self.error)
        else:
            format_block("Success", self.result)

        format_block("Output", self.output)

        return "\n".join(parts)


class ManagedTask:

    def __init__(self,
                 executor: ThreadPoolExecutor,
                 store: Store,
                 name: str,
                 is_ephemeral: bool,
                 is_test: bool,
                 code: str,
                 desc: str,
                 props: Dict[str, Any]):
        self._scoped_store = store
        self._executor = executor
        self.name = name
        self.description = desc
        self.props = props
        self.code = code
        self.is_ephemeral = is_ephemeral
        self.is_test = is_test
        self.last_executions: List[TaskResult] = list()
        self.default_timeout_sec = 30
        self.is_activated = False
        self.cron_expression: CronExpression = CronExpression(self.cron)
        self._wakeup = Event()
        self._task_instance = self.instantiate()


    def instantiate(self):
        self_outer = self
        try:
            # Inject required dependencies directly into the script's namespace
            self._namespace = {
                "__name__": f"task_{self.name}",
                "Store": Store,
                "BackgroundTask": BackgroundTask,
                "AdhocTask": AdhocTask,
            }

            # Compile and execute the user's script
            compiled_code = compile(self.code, f"task_{self.name}.py", 'exec')
            exec(compiled_code, self._namespace)

            user_class = None
            is_background = False
            for obj in self._namespace.values():

                # Check if it's a background task
                if isinstance(obj, type) and issubclass(obj, BackgroundTask) and obj is not BackgroundTask:
                    if user_class is None:
                        user_class = obj
                        is_background = True
                    else:
                        raise ValueError(
                            f"Multiple classes implementing the 'Task' interface found in "
                            f"the script for '{self.name}'. Please ensure only one class inherits from 'Task'."
                        )

                # Check if it's a ad hoc task
                if isinstance(obj, type) and issubclass(obj, AdhocTask) and obj is not AdhocTask:
                    if user_class is None:
                        user_class = obj
                    else:
                        raise ValueError(
                            f"Multiple classes implementing the 'Task' interface found in "
                            f"the script for '{self.name}'. Please ensure only one class inherits from 'Task'."
                        )

            if not user_class:
                raise ValueError(f"No class implementing the 'Task' interface found in the script for '{self.name}'.")

            # Dynamically subclass the detected user class to wrap on_execute / on_execute_with_params
            # with print statements before/after, while still calling the original implementation
            # (looked up on user_class to bypass our own override).
            base_class = user_class

            if is_background:
                class _WrappedTask(user_class):  # type: ignore[misc, valid-type]
                    def on_execute(self_inner, *args, **kwargs):
                        return self_outer._execute_sync("app", lambda: base_class.on_execute(self_inner, *args, **kwargs))
                    def on_execute_fw(self_inner, trigger: str, params: List[str], *args, **kwargs):
                        return self_outer._execute_sync(trigger, lambda: base_class.on_execute(self_inner, *args, **kwargs))
            else:
                class _WrappedTask(user_class):  # type: ignore[misc, valid-type]
                    def on_activate(self) -> None:
                        pass
                    def on_deactivate(self) -> None:
                        pass
                    def on_execute_with_params(self_inner, params: List[str], *args, **kwargs):
                        return self_outer._execute_sync("app", lambda: base_class.on_execute_with_params(self_inner, params, *args, **kwargs))
                    def on_execute_fw(self_inner, trigger: str, params: List[str], *args, **kwargs):
                        return self_outer._execute_sync(trigger, lambda: base_class.on_execute_with_params(self_inner, params, *args, **kwargs))

            # Preserve the @when decorator metadata from the original method on the wrapper
            original_method = getattr(user_class, "on_execute", None)
            if original_method is not None and hasattr(original_method, "__triggers__"):
                _WrappedTask.on_execute.__triggers__ = original_method.__triggers__

            _WrappedTask.__name__ = f"Wrapped{user_class.__name__}"
            _WrappedTask.__qualname__ = _WrappedTask.__name__

            return _WrappedTask(self._scoped_store)

        except SyntaxError as e:
            # 1. Format the code with line numbers and point to the error line
            lines = self.code.splitlines()
            numbered_code = []

            # Fallback to line 0 if lineno is None (e.g., unexpected EOF)
            error_line = e.lineno if e.lineno is not None else 0

            for i, line in enumerate(lines):
                line_num = i + 1
                # Add a visual pointer (>>) to the exact line that failed
                marker = ">> " if line_num == error_line else "   "
                numbered_code.append(f"{marker}{line_num:03d} | {line}")

            formatted_code = "\n".join(numbered_code)

            # 2. Log a highly structured and readable error block
            logger.error(
                f"Syntax error compiling task '{self.name}' at line {error_line}: {e.msg}\n"
                f"--- Source Code ---\n"
                f"{formatted_code}\n"
                f"-------------------"
            )
            raise

        except Exception as e:
            # Catch-all for any other unforeseen compilation, instantiation, or setup errors
            logger.error(f"Unexpected error instantiating task '{self.name}': {e}", exc_info=True)
            raise


    def activate(self):
        if not self.is_activated:
            self.is_activated = True
            self._wakeup.clear()
            Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        try:
            self._task_instance.on_activate()
        except Exception as e:
            logger.error(f"Error activating task '{self.name}': {e}", exc_info=True)

        if self.run_on_start:
            try:
                self._task_instance.on_execute_fw("run_one_start", list())
            except Exception as e:
                logger.warning(f"Error in initial execution for task '{self.name}': {e}", exc_info=True)


        while self.is_activated:
            try:
                if self.cron_expression.should_run(self.last_attempt_at, self.last_failure_age):
                    self._task_instance.on_execute_fw("cron", list())
            except Exception as e:
               logger.warning(f"Error in cron loop for task '{self.name}': {e}", exc_info=True)
            self._wakeup.wait(timeout=3)

        try:
            self._task_instance.on_deactivate()
        except Exception as e:
            logger.error(f"Error deactivating task '{self.name}': {e}", exc_info=True)

    def deactivate(self):
        self.is_activated = False
        self._wakeup.set()

    def __del__(self):
        self.deactivate()

    def execute_manually(self, trigger: str, params: List[str]) -> TaskResult:
        return self._task_instance.on_execute_fw(trigger, params)

    def _execute_sync(self, trigger: str, call) -> TaskResult:
        start = datetime.now()
        output_buffer = io.StringIO()
        try:
            # Capture both standard output (print) and standard error (warnings/logs)
            with redirect_stdout(output_buffer), redirect_stderr(output_buffer):
                result = call()
                elapsed = datetime.now() - start
                task_result = TaskResult(self, trigger, elapsed, result=result, output=output_buffer.getvalue())
                return result
        except Exception as e:
            elapsed = datetime.now() - start
            task_result = TaskResult(self, trigger, elapsed, error=str(e), output=output_buffer.getvalue())
            raise e
        finally:
            self.last_executions.append(task_result)
            if len(self.last_executions) > 10:
                del self.last_executions[0]
            logger.info(task_result)

    def data(self) -> Dict[str, str]:
        """Returns the current persistent state stored for this specific task."""
        return {key: self._scoped_store.get(key) for key in self._scoped_store.keys()}

    def reset(self) -> None:
        """Clears all persistent state for this task."""
        for key in self._scoped_store.keys():
            self._scoped_store.delete(key)

    @property
    def is_background_task(self) -> bool:
        return isinstance(self._task_instance, BackgroundTask)

    @property
    def run_on_start(self) -> bool:
        return self.props.get("run_on_start", False)

    @property
    def cron(self) -> str:
        return self.props.get("cron", "")

    @property
    def created_at(self) -> datetime:
        return datetime.fromisoformat(self.props.get("created_at", datetime.now().isoformat()))

    @property
    def valid_to(self) -> Optional[datetime]:
        """Returns the expiration timestamp of the task's TTL, if set."""
        if "valid_to" in self.props:
            return datetime.fromisoformat(self.props["valid_to"])
        return datetime(2999, 1, 1)

    def is_expired(self) -> bool:
        """Checks if the task's TTL has expired based on the 'valid_to' property."""
        return datetime.now() > self.valid_to

    @property
    def last_attempt_at(self) -> Optional[datetime]:
        """Returns the timestamp of the most recent execution attempt."""
        if not self.last_executions:
            return None
        return self.last_executions[-1].date

    @property
    def last_failure_age(self) -> Optional[timedelta]:
        """Returns the duration since the last failed execution."""
        if not self.last_executions or self.last_executions[-1].error is None:
            return None
        return datetime.now() - self.last_executions[-1].date


    def __str__(self) -> str:

        # 1. Header
        lines = [f"Task '{self.name}' | State: {self.state.upper()}"]

        # 2. Configuration Section
        triggers = []
        trigger_str = " + ".join(triggers) if triggers else "manual_only"

        lines.append("--- Configuration ---")
        lines.append(f"  | Configured: {trigger_str}")
        lines.append(f"  | Timeout:    {self.props.get('timeout', self.default_timeout_sec)}s")

        # 3. Last Execution Section
        lines.append("--- Last Execution ---")
        if not self.last_executions:
            lines.append("  | Never executed")
        else:
            last_run = self.last_executions[-1]
            status = "❌ FAILED" if last_run.error else "✅ SUCCESS"
            run_date = last_run.date.strftime("%Y-%m-%d %H:%M:%S")

            # Extract the trigger used for this specific run
            trigger_used = getattr(last_run, 'trigger', 'unknown')

            lines.append(f"  | Status:     {status} ({last_run.elapsed.total_seconds():.3f}s) at {run_date}")
            lines.append(f"  | Fired By:   '{trigger_used}'")

            # Show up to 3 lines of error
            if last_run.error:
                lines.append("  | Error:")
                err_lines = str(last_run.error).strip().splitlines()
                for line in err_lines[:3]:
                    lines.append(f"  |   {line}")
                if len(err_lines) > 3:
                    lines.append("  |   ... (truncated)")

            # Show up to 3 lines of output
            elif last_run.output:
                lines.append("  | Output:")
                out_lines = str(last_run.output).strip().splitlines()
                for line in out_lines[:3]:
                    lines.append(f"  |   {line}")
                if len(out_lines) > 3:
                    lines.append("  |   ... (truncated)")

        return "\n".join(lines)


class ManagedTaskFactory:

    def __init__(self, store: SimpleStore, executor: ThreadPoolExecutor):
        self._store = store
        self._executor = executor

    def create(self,
               name: str,
               is_ephemeral: bool,
               is_test: bool,
               code: str,
               run_on_start: bool,
               cron: str,
               description: str,
               ttl: Optional[int] = None):

        props: Dict [str, Any] = {  'created_at': datetime.now().isoformat(),
                                    'description': description,
                                    'run_on_start': run_on_start,
                                    'con': cron,
                                    'ttl': -1 if ttl is None else ttl}

        return ManagedTask(self._executor,
                           ScopedStore(self._store, name),
                           name,
                           is_ephemeral,
                           is_test,
                           code,
                           description,
                           props)


    def restore(self,
                name: str,
                is_ephemeral: bool,
                is_test: bool,
                code: str,
                desc: str,
                props: Dict[str, Any]):

        return ManagedTask(self._executor,
                           ScopedStore(self._store, name),
                           name,
                           is_ephemeral,
                           is_test,
                           code,
                           desc,
                           props)

