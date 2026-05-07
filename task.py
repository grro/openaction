import concurrent
import logging
import io
from contextlib import redirect_stdout
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from adapter_impl import AdapterManager
from store_impl import Store, SimpleStore, ScopedStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
class PropertiesObserved:
    service: str
    prop: str
    min_interval_sec: int = field(default=10, compare=False)

    @classmethod
    def from_string(cls, config: str) -> "PropertiesObserved":
        """
        Parses a configuration string into a PropertiesObserved instance.
        Expected format: "service_name#property_name [interval_in_seconds]"
        """
        if not config or not config.strip():
            raise ValueError("Configuration string cannot be empty.")

        # .split() with no arguments splits on ANY whitespace and removes duplicates
        parts = config.strip().split()
        entity = parts[0]

        if "#" not in entity:
            raise ValueError(f"Invalid format '{entity}'. Expected 'service#prop'.")

        # Split with maxsplit=1 ensures safe parsing even if the property contains a '#'
        service, prop = entity.split("#", 1)

        interval = 10
        if len(parts) > 1:
            try:
                interval = int(parts[1])
            except ValueError:
                logger.warning(f"Invalid interval '{parts[1]}' in config '{config}'. Defaulting to 10s.")
                interval = 10

        # Use 'cls' instead of hardcoding the class name
        return cls(service=service.strip(), prop=prop.strip(), min_interval_sec=interval)

    @property
    def identity(self) -> str:
        """Returns a unique identifier for dictionary keys or logging."""
        return f"{self.service}#{self.prop}"

    def __str__(self) -> str:
        return f"PropertiesObserved({self.identity}, {self.min_interval_sec}s)"


class TaskResult:

    def __init__(self, trigger: str, elapsed: timedelta, output: str = None, error: str = None):
        self.date = datetime.now()
        self.trigger = trigger
        self.elapsed = elapsed
        self.output = output
        self.error = error

    def __str__(self) -> str:
        status = "❌ FAILED" if self.error else "✅ SUCCESS"
        elapsed_sec = f"{self.elapsed.total_seconds():.3f}s"

        # Primary header line
        lines = [f"TaskResult [{status}] | Trigger: '{self.trigger}' | Elapsed: {elapsed_sec}"]

        # Handle optional error (multiline safe)
        if self.error:
            lines.append("--- Error ---")
            lines.extend(f"  | {line}" for line in str(self.error).strip().splitlines())

        # Handle optional multiline output
        if self.output:
            lines.append("--- Output ---")
            lines.extend(f"  | {line}" for line in str(self.output).strip().splitlines())

        return "\n".join(lines)


    def __repr__(self):
        return self.__str__()


class Task:

    RUNNING = "running"
    IDLING = "idling"

    def __init__(self,
                 executor: ThreadPoolExecutor,
                 store: Store,
                 adapter_manager: AdapterManager,
                 name: str,
                 code: str,
                 props: Dict[str, Any]):
        self._scoped_store = store
        self._adapter_manager = adapter_manager
        self._executor = executor
        self.name = name
        self.props = props
        self.code = code
        self.last_executions: List[TaskResult] = list()
        self.default_timeout_sec = 30
        self.state = self.IDLING

        try:
            self._compiled_code = compile(self.code, f"<task:{self.name}>", 'exec')
        except SyntaxError as e:
            # 1. Format the code with line numbers and point to the error line
            lines = self.code.splitlines()
            numbered_code = []

            for i, line in enumerate(lines):
                line_num = i + 1
                # Add a visual pointer (>>) to the exact line that failed
                marker = ">> " if line_num == e.lineno else "   "
                numbered_code.append(f"{marker}{line_num:03d} | {line}")

            formatted_code = "\n".join(numbered_code)

            # 2. Log a highly structured and readable error block
            logger.error(
                f"Syntax error compiling task '{self.name}' at line {e.lineno}: {e.msg}\n"
                f"--- Source Code ---\n"
                f"{formatted_code}\n"
                f"-------------------"
            )
            raise
        except Exception as e:
            # Catch-all for any other unforeseen compilation errors (e.g., MemoryError, TypeError)
            logger.error(f"Unexpected error compiling task '{self.name}': {e}", exc_info=True)
            raise

    def _add_task_result(self, task_result: TaskResult):
        self.last_executions.append(task_result)
        if len(self.last_executions) > 10:
            del self.last_executions[0]

    def data(self) -> Dict[str, str]:
        """Returns the current persistent state stored for this specific task."""
        return {key: self._scoped_store.get(key) for key in self._scoped_store.keys()}

    def reset(self) -> None:
        """Clears all persistent state for this task."""
        for key in self._scoped_store.keys():
            self._scoped_store.delete(key)

    @property
    def props_observed(self) -> Set[PropertiesObserved]:
        """Returns the set of properties this task is observing, if any."""
        raw_props = self.props.get("props_observed", [])
        return {PropertiesObserved.from_string(p) for p in raw_props}

    @property
    def cron_expression(self) -> Optional[str]:
        """Returns the cron expression if this task is a CronTask, otherwise None."""
        return self.props.get("cron_expression", None)

    @property
    def created_at(self) -> datetime:
        return datetime.fromisoformat(self.props.get("created_at", datetime.now().isoformat()))

    @property
    def description(self) -> str:
        return self.props.get("description", "False")

    @property
    def run_on_start(self) -> bool:
        return self.props.get("run_on_start", False)

    @property
    def is_test_task(self) -> bool:
        return self.props.get("is_test", False)

    @property
    def valid_to(self) -> Optional[datetime]:
        """Returns the expiration timestamp of the task's TTL, if set."""
        if "valid_to" in self.props:
            return datetime.fromisoformat(self.props["valid_to"])
        return None

    def is_still_valid(self) -> bool:
        """Checks if the task's TTL has expired based on the 'valid_to' property."""
        if self.valid_to is None:
            return True
        else:
            return datetime.now() < self.valid_to

    def last_attempt_at(self) -> Optional[datetime]:
        """Returns the timestamp of the most recent execution attempt."""
        if not self.last_executions:
            return None
        return self.last_executions[-1].date

    def last_failure_age(self) -> Optional[timedelta]:
        """Returns the duration since the last failed execution."""
        if not self.last_executions or self.last_executions[-1].error is None:
            return None
        return datetime.now() - self.last_executions[-1].date

    def safe_run(self, trigger: str):
        """Executes the task and ensures any unhandled exceptions are logged."""
        try:
            return self.run(trigger)
        except Exception as e:
            msg = f"Execution failed for task '{self.name}': {e}"
            logger.error(msg)
            task_result = TaskResult(trigger, timedelta(0), error=msg)
            self._add_task_result(task_result)
            return task_result

    def run(self, trigger: str) -> TaskResult:
        """
        Executes the task logic within a dedicated thread.
        Handles locking, timeouts, and result logging.
        """

        start = datetime.now()
        task_result = TaskResult(trigger, start-start, error="Unknown error")  # Default in case of unexpected failure before assignment

        # Acquire lock without blocking to prevent overlapping executions of the same task
        if self.state == self.RUNNING:
            msg = f"Task '{self.name}' is already running. Skipping this execution cycle."
            logger.debug(msg)
            task_result = TaskResult(trigger, timedelta(0), error=msg)
            self._add_task_result(task_result)
            return task_result

        # Retrieve custom timeout from properties or use the system default
        timeout_sec = self.props.get("timeout", self.default_timeout_sec)
        try:
            self.state = self.RUNNING
            logger.info("Executing task '%s'", self.name)

            future = self._executor.submit(self._execute_script)
            printed_output = future.result(timeout=timeout_sec)
            elapsed = datetime.now() - start
            task_result = TaskResult(trigger, elapsed, output=printed_output)
        except concurrent.futures.TimeoutError as te:
            error_msg = f"Execution failed (TimeoutError; timeout {timeout_sec} seconds) for task '{self.name}': {str(te)}"
            logger.warning(error_msg)
            elapsed = datetime.now() - start
            task_result = TaskResult(trigger, elapsed, error=error_msg)

        except Exception as e:
            elapsed = datetime.now() - start
            partial_output = getattr(e, 'output', None)
            error_msg = f"Execution failed for task '{self.name}': {type(e).__name__}: {str(e)}"
            if partial_output:
                logger.warning(f"{error_msg}\nPartial Output:\n{partial_output}")
            else:
                logger.warning(error_msg)
            task_result = TaskResult(trigger, elapsed, output=partial_output, error=error_msg)

        finally:
            self.state = self.IDLING
            self._add_task_result(task_result)

        return task_result

    def _execute_script(self):
        """
        The wrapper that actually runs the compiled code.
        This is what gets sent to the ThreadPoolExecutor.
        """
        # Define the environment available to the script
        global_env = {
            "registry": self._adapter_manager,
            "store": self._scoped_store,
            "props": self.props,
            "__builtins__": __builtins__
        }

        output_buffer = io.StringIO()
        try:
            with redirect_stdout(output_buffer):
                # Execute the procedural script
                # Any variables defined in the script stay in global_env
                exec(self._compiled_code, global_env)
        except Exception as e:
            # Attach the output generated so far to the exception
            e.output = output_buffer.getvalue()
            raise e

        return output_buffer.getvalue()

    def __str__(self) -> str:
        type_flag = "🧪 TEST" if getattr(self, 'is_test_task', False) else "🛡️ PROD"

        # 1. Header
        lines = [f"Task [{type_flag}] | '{self.name}' | State: {self.state.upper()}"]

        # 2. Configuration Section
        triggers = []
        if getattr(self, 'cron_expression', None):
            triggers.append(f"cron({self.cron_expression})")
        if getattr(self, 'props_observed', None):
            triggers.append(f"subs({len(self.props_observed)})")
        if getattr(self, 'run_on_start', False):
            triggers.append("on_start")

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


class TaskFactory:

    def __init__(self, store: SimpleStore, adapter_manager: AdapterManager, executor: ThreadPoolExecutor):
        self._store = store
        self._adapter_manager = adapter_manager
        self._executor = executor

    def create(self,
               name: str,
               code: str,
               description: str,
               cron_expression: Optional[str],
               subscriptions: List[str],
               run_on_start: bool  = False,
               ttl: Optional[int] = None,
               is_test: bool = False):

        props: Dict [str, Any] = {  'created_at': datetime.now().isoformat(),
                                    'description': description,
                                    'run_on_start': run_on_start,
                                    'is_test': is_test,
                                    'ttl': -1 if ttl is None else ttl}
        if cron_expression is not None:
            props['cron_expression'] = cron_expression
        if subscriptions is not None:
            props['props_observed'] = subscriptions

        return Task(self._executor,
                    ScopedStore(self._store, name),
                    self._adapter_manager,
                    name,
                    code,
                    props)


    def restore(self,
                name: str,
                code: str,
                props: Dict[str, Any]):

        return Task(self._executor,
                    ScopedStore(self._store, name),
                    self._adapter_manager,
                    name,
                    code,
                    props)

