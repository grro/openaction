import os
import sys
import logging
import importlib.metadata
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import  List

from mcp_server import McpServer
from store_impl import SimpleStore
from managed_task import ManagedTask, ManagedTaskFactory
from task_repository import ManagedTaskRepository



logger = logging.getLogger(__name__)



class ExecutionHistory:

    def __init__(self, limit: int):
        self.limit = limit
        self._last_tasks: List[ManagedTask] = []

    def add(self, task: ManagedTask):
        # 1. Check if a task with this name already exists in the history buffer
        existing_task = next((t for t in self._last_tasks if t.name == task.name), None)

        if existing_task:
            # 2. If the name AND the source code are identical (and it's an ephemeral test task),
            # carry over the previous execution history into the newly instantiated task object!
            if existing_task.code == task.code:
                # Prepend the older executions BEFORE the current one
                task.last_executions = existing_task.last_executions + task.last_executions

            # Regardless of code match: Remove the old entry from the list to prevent duplicates
            self._remove(task.name)

        # 3. Append the new (or now history-enriched) task to the end of the list
        self._last_tasks.append(task)

        # 4. Enforce the buffer limit
        while len(self._last_tasks) > self.limit:
            del self._last_tasks[0]

    def _remove(self, name: str):
        self._last_tasks = [task for task in self._last_tasks if task.name != name]

    @property
    def last_tasks(self) -> List[ManagedTask]:
        self._last_tasks = [task for task in self._last_tasks if not task.is_expired()]
        return list(self._last_tasks)




class OpenActionServer(McpServer):

    def __init__(self, name: str, port: int, dir: str, host: str = "0.0.0.0"):
        super().__init__(name, port, host)
        self.store = SimpleStore(name="state", directory=dir)
        self.execution_history = ExecutionHistory(15)
        self.executors = ThreadPoolExecutor(max_workers=20, thread_name_prefix="Taskexecutor")
        self.task_factory = ManagedTaskFactory(self.store, self.executors)
        self.task_repository = ManagedTaskRepository(os.path.join(dir, "tasks"), self.task_factory, self.store)
        self.task_repository.start()



        @self.mcp.tool()
        def list_available_modules() -> str:
            """
            Lists the current Python version and all external third-party Python packages installed.
            Use this to determine environment capabilities and which libraries can be imported inside tasks.
            """
            try:
                python_version = sys.version.split()[0]
                dists = importlib.metadata.distributions()
                packages = [
                    (dist.metadata['Name'], dist.version)
                    for dist in dists
                    if dist.metadata.get('Name')
                ]

                report = [
                    f"### 🐍 Python Environment: `v{python_version}`",
                    "---"
                ]

                if not packages:
                    report.append("No external Python packages found in the current environment.")
                else:
                    packages.sort(key=lambda x: x[0].lower())
                    report.extend([
                        "### 📦 Available External Packages",
                        "> **Important Notes for Scripting:**",
                        "> 1. **Standard Libraries:** All built-in Python modules for this version are implicitly available.",
                        "> 2. **Import Names:** The names below are package distribution names. The actual import statement might differ slightly.",
                        ""
                    ])
                    report.extend([f"- **`{name}`** `(v{version})`" for name, version in packages])

                return "\n".join(report)

            except Exception as e:
                logger.error(f"Failed to list modules: {e}", exc_info=True)
                return f"Error: Could not retrieve the environment details: {type(e).__name__} - {str(e)}"




        @self.mcp.tool()
        def list_example_tasks() -> str:
            """
            Retrieves the source code of all example tasks.
            """
            try:
                examples_dir = (Path(__file__).parent / "examples").resolve()

                if not examples_dir.is_dir():
                    return f"Error: No 'examples' directory found at {examples_dir}."

                examples = []
                for file_path in examples_dir.glob("*.py"):
                    if file_path.is_file():
                        content = file_path.read_text(encoding="utf-8")

                        task_block = (
                            f"### 📄 `{file_path.name}`\n"
                            f"```python\n{content}\n```"
                        )
                        examples.append(task_block)

                if not examples:
                    return "No example Python scripts found in the 'examples' directory."

                return "\n\n---\n\n".join(examples)

            except Exception as e:
                return f"Error reading examples directory: {type(e).__name__} - {e}"


        @self.mcp.tool()
        def list_api() -> str:
            """
            Retrieves the source code of the API base classes.
            """
            try:
                api_dir = (Path(__file__).parent / "api").resolve()

                if not api_dir.is_dir():
                    return f"Error: No 'api' directory found at {api_dir}."

                apis = []
                # FIX: Applied the .py restriction here as well for safety
                for file_path in api_dir.glob("*.py"):
                    if file_path.is_file():
                        content = file_path.read_text(encoding="utf-8")
                        apis.append(f"--- {file_path.name} ---\n```python\n{content}\n```")

                if not apis:
                    return "No api python files found in the 'api' directory."

                return "\n\n".join(apis)

            except Exception as e:
                return f"Error reading API directory: {type(e).__name__} - {e}"


        @self.mcp.tool()
        def register_task(name: str, script: str, description: str, run_on_start: bool, cron: str = "") -> str:
            """
            Registers a new permanent, Python-based automation task in the OpenAction system.

            Args:
                name (str): A unique, URI-safe identifier (alphanumeric, hyphens, underscores).
                    WARNING: Production tasks MUST NOT start with the 'test_' prefix.
                script (str): The Python source code. MUST define a class inheriting from the
                    abstract `Task` class (use `list_api` to view the required interface).
                    *Injection Note:* Do NOT import `Task` or `Store` in your script; they are
                    automatically injected into the execution namespace.
                    *Architecture Constraint:* Consolidate logic. Create ONE script per target
                    device/service (e.g., a single heater) rather than fragmenting into multiple tasks.
                description (str): A clear explanation of what the task does and its intended triggers.
                run_on_start (bool): If True, the task will execute immediately when the system boots.
                cron (str, optional): A standard cron expression (e.g., "*/5 * * * *") defining
                    the schedule. Omit or pass an empty string if this is purely an event-driven task.

            Returns:
                str: A confirmation message indicating the registration status.

            ==============================
            MANDATORY PROTOCOLS
            ==============================
            1. ERROR HANDLING & RETRIES:
               - The script MUST evaluate external service responses for error states.
               - If an external call fails, you MUST `raise Exception("...")`. The OpenAction
                 engine catches this and triggers automatic retry logic (1-minute delay). Do not
                 swallow errors silently.

            2. PRE-REGISTRATION VALIDATION (Strict Enforcement):
               - STEP A (Check Existing): Use the `list_tasks` and `get_task` tools to verify if a
                 task already exists for the target device. If it does, fetch it and MERGE your
                 new logic into it. Do not create duplicates.
               - STEP B (Map Environment): Use `list_available_services` and `list_available_modules`
                 to ensure your required endpoints and libraries actually exist in the environment.
               - STEP C (Test First): You MUST execute your code using the `execute_task` tool to
                 validate syntax, logic, and JSON parsing BEFORE calling `register_task(name)`.
                 Blind registration is strictly prohibited.
            """

            try:
                self.task_repository.register(name, script, description, run_on_start, cron)
                logger.info(f"Production Task '{name}' registered successfully.")
                return f"Success: Task '{name}' has been successfully registered as a persistent production task."

            except Exception as e:
                logger.error(f"Failed to register task '{name}': {e}", exc_info=True)
                return f"Error: An internal error occurred during registration: {type(e).__name__} - {str(e)}"


        @self.mcp.tool()
        def deregister_task(name: str, reason: str) -> str:
            """
            Deregisters and removes a previously registered task.
            """
            try:
                # Check if the task exists in the active registry
                if name not in self.task_repository.tasks:
                    return f"Error: Task '{name}' not found."

                self.task_repository.deregister(name, reason)

                logger.info(f"Task '{name}' unregistered successfully.")
                return f"Task '{name}' has been successfully unregistered."

            except Exception as e:
                logger.error(f"Failed to unregister task '{name}': {e}", exc_info=True)
                return f"Error: Failed to unregister task '{name}': {str(e)}"


        @self.mcp.tool()
        def list_tasks() -> str:
            """
            Retrieves a concise list of all currently registered permanent tasks,
            as well as recently executed ephemeral (test) tasks and their execution frequency.
            Includes indicators for whether a task is an AdhocTask or a BackgroundTask.

            Note: If an ephemeral test task is executed frequently, it should be registered
            as a permanent ad hoc task to avoid recreating the script repeatedly.
            """
            try:
                output = []


                # --- 1. Permanent / Registered Tasks ---
                permanent_tasks = self.task_repository.tasks.values()
                output.append("### 🛡️ Registered (Permanent) Tasks")
                output.append("=================================\n")

                if permanent_tasks:
                    for task in permanent_tasks:
                        type_label =  "⚙️ BACKGROUND" if task.is_background_task else "🛠️ ADHOC"
                        output.append(f"• **{task.name}** `[{type_label}]`: {task.description}")
                else:
                    output.append("*Currently, no permanent tasks are registered.*")

                output.append("\n") # Spacer


                ephemeral_tasks = [task for task in self.execution_history.last_tasks if task.is_ephemeral]

                output.append("### 🫧 Recently Executed Ephemeral Tasks")
                output.append("==========================================\n")

                if ephemeral_tasks:
                    # Iterate in reverse to show the most recent test logic at the top
                    for task in reversed(ephemeral_tasks):
                        exec_count = len(task.last_executions) if task.last_executions else 0

                        # Determine labels
                        type_label =  "⚙️ BACKGROUND" if task.is_background_task else "🛠️ ADHOC"

                        # Check if it's explicitly a test task for additional flagging
                        test_flag = " `[🧪 TEST]`" if task.is_test else ""

                        output.append(f"• **{task.name}**{test_flag} | Executed: `{exec_count}x` | `[{type_label}]`: {task.description}")
                else:
                    output.append("*No ephemeral tasks found in the recent execution history.*")

                return "\n".join(output)

            except Exception as e:
                logger.error(f"Failed to list tasks: {e}", exc_info=True)
                return "Error: Could not retrieve tasks from the registry or history. Check server logs."


        @self.mcp.tool()
        def get_task(name: str) -> str:
            """
            Retrieves detailed metadata, execution history, and source code for a specific task.

            This tool automatically identifies if the task is a permanently registered
            production task or an ephemeral test task from the recent execution history.
            """
            try:
                # 1. Search Strategy: Repository first, then Execution History
                task = self.task_repository.tasks.get(name)
                is_ephemeral = False

                if not task:
                    # Search history buffer (latest first)
                    history_tasks = self.execution_history.last_tasks
                    task = next((t for t in reversed(history_tasks) if t.name == name), None)
                    is_ephemeral = True

                if not task:
                    return f"Error: Task '{name}' not found in registered tasks or recent history."

                # 2. Determine Task Type & Lifecycle Labels
                is_bg = task.is_background_task if callable(getattr(task, 'is_background_task', None)) else getattr(task, 'is_background_task', False)
                type_label = "⚙️ BACKGROUND" if is_bg else "🛠️ ADHOC"
                lifecycle_label = "🧪 [EPHEMERAL]" if is_ephemeral else "🛡️ [PERMANENT]"

                lines = [
                    f"## {lifecycle_label} {task.name}",
                    f"**Type:** `{type_label}`",
                    f"**Description:** {getattr(task, 'description', 'No description provided.')}",
                    f"**Current State:** `{getattr(task, 'state', 'UNKNOWN')}`",
                    ""
                ]

                # 3. Execution History Section
                if not task.last_executions:
                    lines.append("### 🕒 History\n- *Never executed*\n")
                else:
                    lines.append("### 🕒 Recent Executions (Latest First)")
                    for run in list(reversed(task.last_executions))[:5]:
                        ts = run.date.strftime("%Y-%m-%d %H:%M:%S")
                        status = "✅ OK" if run.error is None else "❌ ERROR"
                        duration = f"{run.elapsed.total_seconds():.2f}s"
                        trigger = getattr(run, 'trigger', 'unknown')

                        lines.append(f"- **{ts}** | ⚡ `{trigger}` | {status} ({duration})")

                        if run.error:
                            lines.append(f"  - `Detail: {run.error}`")
                        elif run.output:
                            display_out = (run.output[:597] + "...") if len(run.output) > 600 else run.output
                            indented_out = display_out.replace('\n', '\n    ')
                            lines.append(f"  - `Output:\n    {indented_out}`")
                    lines.append("")

                # 4. Persistent Store Data Section (Only if applicable/available)
                if hasattr(task, 'data'):
                    lines.append("### 💾 Persistent Store Data")
                    try:
                        store_data = task.data()
                        if not store_data:
                            lines.append("- *No persistent data stored.*")
                        else:
                            for key, value in sorted(store_data.items()):
                                display_value = str(value)
                                if len(display_value) > 200:
                                    display_value = display_value[:197] + "..."
                                lines.append(f"- **`{key}`**: `{display_value}`")
                    except Exception as e:
                        lines.append(f"- *Error retrieving store data: {e}*")
                    lines.append("")

                # 5. Source Code Section
                code = getattr(task, 'code', '# No source code available.').strip()
                lines.append("### 📝 Source Code")
                lines.append(f"```python\n{code}\n```")

                return "\n".join(lines)

            except Exception as e:
                logger.error(f"Failed to retrieve task '{name}': {e}", exc_info=True)
                return f"Error: Internal server error while retrieving task details: {type(e).__name__} - {str(e)}"



        @self.mcp.tool()
        def execute_task(name: str, params: List[str]) -> str:
            """
            Manually triggers the immediate execution of a registered ad hoc or background task by name.

            Use this to run production tasks on-demand. Ad hoc tasks will process the
            provided parameters, while background tasks will run their logic immediately
            bypassing their usual schedule.

            Args:
                name (str): The unique identifier of the registered task to execute.
                params (List[str]): Parameters to pass to the task (e.g., ["brightness=50"]).
                    Leave empty [] for background tasks.
            """
            try:
                task = self.task_repository.tasks.get(name)
                if not task:
                    logger.warning(f"Manual execution aborted: Task '{name}' not found.")
                    return f"Error: Task '{name}' is not currently registered in the system."

                logger.info(f"Manually triggering task '{name}' with parameters: {params}")

                # Execute the task safely
                # Using a standardized trigger name for history tracking
                task.execute_manually("manual_trigger_by_name", params)
                self.execution_history.add(task)

                return f"Success: Task '{name}' was executed successfully."

            except Exception as e:
                logger.error(f"Critical failure during manual execution of task '{name}': {e}", exc_info=True)
                return f"Error: Failed to execute task '{name}': {type(e).__name__} - {str(e)}"


        @self.mcp.tool()
        def execute_ephemeral_task(name: str, code: str, description: str, is_test: bool = True, ttl: int = 7 * 24 * 3600) -> str:
            """
            Creates an ephemeral, non-persistent task and executes it immediately.

            Use this tool to validate logic, check service responses, and debug scripts
            before permanently registering them. This tool is strictly intended for
            iterative development and testing of draft code.

            Args:
                name (str): A unique identifier for this run.
                code (str): The Python source code for the task.
                description (str): A brief explanation of the task's intent.
                is_test (bool): Should be set to True if this is a test/draft execution.
                                Defaults to True.
                ttl (int): Time-to-live in seconds for this ephemeral task. After this period,
                           the task will be automatically removed from the execution history.
                           Defaults to 604800 (7 days).

            Returns:
                str: A detailed execution report including results or errors.
            """
            try:
                # 1. Create a temporary task instance via the factory
                # We explicitly set is_ephemeral and pass along the test flag and TTL.
                task = self.task_factory.create(name, True, is_test, code, False, '', description, ttl)
                task.activate()
                logger.info(f"Executing ephemeral task '{name}' (is_test={is_test}, ttl={ttl}s)")

                # 2. Run the task (returns a TaskResult object)
                # Using a standardized trigger name for clear history tracking
                result = task._execute_sync("manual_ephemeral_execution", [])

                # Update the shared execution history buffer
                # This allows get_task and manual_execution_history to retrieve it later
                self.execution_history.add(task)

                # 3. Build the response utilizing the TaskResult's built-in formatting
                header_icon = "🧪" if is_test else "⚡"
                response = [
                    f"### {header_icon} Ephemeral Execution Report: `{name}`",
                    "---",
                    str(result)
                ]

                final_output = "\n".join(response)
                logger.info(f"Executed ephemeral script {name}:\n{final_output}")
                return final_output

            except Exception as e:
                logger.error(f"Failed to initialize or run ephemeral task '{name}': {e}", exc_info=True)
                return f"Error: Critical failure during task setup: {type(e).__name__} - {str(e)}"


    def stop(self):
        self.task_repository.stop()
        self.mdns.unregister_mdns(self.name)
        self.executors.shutdown(wait=False)
        super().stop()

