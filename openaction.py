import os
import sys
import asyncio
import logging
import importlib.metadata
from pathlib import Path
from time import sleep
from concurrent.futures import ThreadPoolExecutor
from typing import Dict
from datetime import datetime, timedelta
from fastmcp import FastMCP

from mdns import MDNS, MDNSRegistry
from cron import CronService
from config import ServiceRegistry, Service, Configs
from store_impl import SimpleStore
from task_adapter import TaskAdapterFactory
from task_repository import TaskAdapterRepository



logger = logging.getLogger(__name__)





class OpenActionServer:

    def __init__(self, port: int, dir: str, configs: Dict[str, Service], autoscan: bool, host: str = "0.0.0.0"):
        self.name = 'OpenAction'
        self.host = host
        self.port = port
        self.store = SimpleStore(name="state", directory=dir)
        self.mdns_registry = MDNSRegistry(self.store)
        self.manual_registry = ServiceRegistry(configs)
        self.executors = ThreadPoolExecutor(max_workers=20, thread_name_prefix="Taskexecutor")
        self.task_factory = TaskAdapterFactory(self.store, self.executors)
        self.task_repository = TaskAdapterRepository(os.path.join(dir, "tasks"), self.task_factory, self.store)
        self.cron = CronService(self.task_repository)

        self.manual_registry.start()
        self.mdns_registry.start()
        self.task_repository.start()
        self.cron.start()

        self.mdns = MDNS()
        self.mcp = FastMCP(self.name)
        self.loop = asyncio.new_event_loop()


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
        def list_available_services() -> str:
            """
            Lists all manually configured services and locally discovered mDNS services.
            Use this to identify available hardware, endpoints, or network APIs.
            """
            try:
                report = [
                    "### 📡 Available Services",
                    "=========================\n"
                ]

                # --- 1. Manually Configured Services ---
                report.append("#### 🛠️ Manually Configured Services")
                manual_services = getattr(self.manual_registry, 'services', [])

                if not manual_services:
                    report.append("*No manually configured services found.*\n")
                else:
                    for svc in manual_services:
                        name = getattr(svc, 'name', 'Unknown')
                        svc_type = getattr(svc, 'type', 'Unknown').upper()
                        url = getattr(svc, 'url', 'Unknown URL')
                        report.append(f"• **{name}** [{svc_type}]: `{url}`")
                    report.append("\n") # Spacer

                # --- 2. mDNS Discovered Services ---
                report.append("#### 🔍 Discovered Local Services (mDNS)")
                mdns_services = getattr(self.mdns_registry, 'services', {})

                if not mdns_services:
                    report.append("*No mDNS services currently discovered on the local network.*")
                else:
                    # Normalize iteration if it's a dictionary
                    iterator = mdns_services.values() if isinstance(mdns_services, dict) else mdns_services

                    for svc in iterator:
                        name = svc.name
                        port = svc.port
                        server = svc.host

                        # Extract and format the last_seen timestamp
                        last_seen = svc.last_seen
                        time_str = last_seen.strftime("%Y-%m-%d %H:%M:%S")

                        # Extract and safely decode byte-encoded properties common in Zeroconf
                        props = getattr(svc, 'properties', {})
                        props_str = ""
                        if props:
                            safe_props = {}
                            for k, v in props.items():
                                safe_k = k.decode('utf-8') if isinstance(k, bytes) else str(k)
                                safe_v = v.decode('utf-8') if isinstance(v, bytes) else str(v)
                                safe_props[safe_k] = safe_v
                            props_str = f"\n  - *Properties:* `{safe_props}`"

                        # Append the formatted service entry
                        report.append(f"• **{name}** (`{server}:{port}`) | *last seen: {time_str}*{props_str}")

                return "\n".join(report)

            except Exception as e:
                logger.error(f"Failed to list available services: {e}", exc_info=True)
                return f"Error: Could not retrieve services: {type(e).__name__} - {str(e)}"


        @self.mcp.tool()
        def register_task(name: str,
                          script: str,
                          description: str) -> str:
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
                 validate syntax, logic, and JSON parsing BEFORE calling `register_task`.
                 Blind registration is strictly prohibited.
            """

            try:
                self.task_repository.register(
                    name=name,
                    code=script,
                    description=description,
                    ttl=None,
                    is_test=False
                )

                logger.info(f"Production Task '{name}' registered successfully.")
                return f"Success: Task '{name}' has been successfully registered as a persistent production task."

            except Exception as e:
                logger.error(f"Failed to register task '{name}': {e}", exc_info=True)
                return f"Error: An internal error occurred during registration: {type(e).__name__} - {str(e)}"


        # Expose this function as a callable tool over the Model Context Protocol
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
            Retrieves a concise list of all registered tasks, indicating which are temporary tests.
            """
            try:
                if not self.task_repository.tasks:
                    return "Currently, no tasks are registered."

                output = ["### Registered Tasks", "=================\n"]

                for task in self.task_repository.tasks.values():
                    # Add a clear [TEST] marker if the task is temporary
                    test_tag = " [TEST]" if getattr(task, 'is_test_task', False) else ""
                    output.append(f"• **{task.name}**{test_tag} [{task.state}]: {task.description}")

                return "\n".join(output)

            except Exception as e:
                logger.error(f"Failed to list tasks: {e}", exc_info=True)
                return "Error: Could not retrieve tasks from the registry. Check server logs."


        @self.mcp.tool()
        def get_task(name: str) -> str:
            """
            Retrieves detailed metadata, execution history, and source code for a specific task.
            """
            try:
                task = self.task_repository.tasks.get(name)
                if not task:
                    return f"Error: Task '{name}' is not currently registered."

                is_test = task.is_test_task
                type_label = "🧪 [TEST TASK]" if is_test else "🛡️ [PERMANENT]"

                # We'll use 'lines' to build the report to avoid shadowing 'run.output'
                lines = [
                    f"## {type_label} {task.name}",
                    f"**Description:** {task.description}",
                    f"**Current State:** `{task.state}`",
                    ""
                ]

                # History Section
                if not task.last_executions:
                    lines.append("### 🕒 History\n- *Never executed*\n")
                else:
                    lines.append("### 🕒 Recent Executions (Latest First)")
                    for run in list(reversed(task.last_executions))[:5]:
                        ts = run.date.strftime("%Y-%m-%d %H:%M:%S")

                        # FIX: run.error is now a string, so we just check if it exists
                        status = "✅ OK" if run.error is None else "❌ ERROR"
                        duration = f"{run.elapsed.total_seconds():.2f}s"

                        # FIX: Extract trigger (using getattr as a safety net for backwards compatibility)
                        trigger = getattr(run, 'trigger', 'unknown')

                        # Added trigger visually into the history bullet point
                        lines.append(f"- **{ts}** | ⚡ `{trigger}` | {status} ({duration})")

                        if run.error:
                            lines.append(f"  - `Detail: {run.error}`")
                        elif run.output:
                            display_out = (run.output[:597] + "...") if len(run.output) > 600 else run.output
                            # Handle newlines gracefully so output doesn't break Markdown formatting
                            indented_out = display_out.replace('\n', '\n    ')
                            lines.append(f"  - `Output:\n    {indented_out}`")
                    lines.append("")

                # Persistent Store Data Section
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
                    lines.append(f"- *Error retrieving store data: {type(e).__name__} - {e}*")
                lines.append("")

                lines.append("### 📝 Source Code")
                lines.append(f"```python\n{task.code.strip()}\n```")

                return "\n".join(lines)

            except Exception as e:
                logger.error(f"Failed to retrieve task '{name}': {e}", exc_info=True)
                return f"Error: Internal server error while retrieving task '{name}'."


        @self.mcp.tool()
        def execute_task(name: str, code: str) -> str:
            """
            Creates an ephemeral test task and executes it immediately.
            Use this to validate logic, check service responses, and debug scripts
            before permanent registration.

            Args:
                name: A unique name for this test execution (e.g., 'test_light_check').
                code: The procedural Python code to execute.

            Returns:
                str: A detailed report of the execution outcome, including results or errors.
            """

            test_name = name if name.startswith("test_") else f"test_{name}"

            try:
                # 1. Create a temporary task instance
                task = self.task_factory.create(
                    name=test_name,
                    code=code,
                    description=f"Manual execution trigger: {datetime.now().isoformat()}",
                    ttl=3600,
                    is_test=True
                )

                logger.info("Executing ephemeral task '%s'", test_name)

                # 2. Run the task (returns a TaskResult object)
                result = task.run(trigger="manual_test")

                # 3. Build the response utilizing the TaskResult's built-in formatting
                response = [
                    f"### Execution Report: {test_name}",
                    "---",
                    str(result)
                ]

                final_output = "\n".join(response)

                logger.info(f"Executed test script {test_name}:\n{final_output}")
                return final_output

            except Exception as e:
                logger.error(f"Failed to initialize or run task '{name}': {e}", exc_info=True)
                return f"Error: Critical failure during task setup: {type(e).__name__}: {str(e)}"


        @self.mcp.tool()
        def get_task_health() -> str:
            """
            Provides a high-level health status of all tasks, focusing on failures and stalls.
            """
            try:
                tasks = self.task_repository.tasks.values()
                if not tasks:
                    return "Health Check: OK. No tasks registered."

                critical_failures = []
                warnings = []
                healthy_count = 0
                now = datetime.now()

                for task in tasks:
                    # Case 1: Task has execution history
                    if task.last_executions:
                        last_run = task.last_executions[-1]

                        # Check for Exceptions
                        if last_run.error:
                            error_msg = str(last_run.error)
                            critical_failures.append(
                                f"❌ **{task.name}**: Failed at {last_run.date.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                f"   Reason: `{error_msg[:200]}`"
                            )
                            continue

                        # Check for "Stale" tasks (Heuristic: No run for > 24h)
                        # Note: In a production version, you'd compare against the actual cron interval.
                        if now - last_run.date > timedelta(hours=24):
                            warnings.append(f"⏳ **{task.name}**: Stale? Last successful run was over 24h ago.")
                            continue

                    # Case 2: Task has never run
                    else:
                        warnings.append(f"⚠️ **{task.name}**: Registered but has NEVER executed.")
                        continue

                    healthy_count += 1

                # Build the report
                report = ["### OpenAction System Health", "==========================\n"]

                if critical_failures:
                    report.append("#### 🚨 CRITICAL FAILURES")
                    report.extend(critical_failures)
                    report.append("")

                if warnings:
                    report.append("#### ⚠️ WARNINGS / OBSERVATIONS")
                    report.extend(warnings)
                    report.append("")

                report.append(f"**Summary:** {healthy_count} of {len(tasks)} tasks are currently healthy.")

                if not critical_failures and not warnings:
                    return "✅ All tasks are healthy and running as scheduled."

                return "\n".join(report)

            except Exception as e:
                logger.error(f"Health check failed: {e}", exc_info=True)
                return f"Error: Failed to perform health check: {type(e).__name__} - {str(e)}"


    async def __run(self) -> None:
        logger.info(f"MCP Server '{self.name}' running on http://{self.host}:{self.port}/sse")
        await self.mcp.run_async(transport="sse", host=self.host, port=self.port)


    def start(self):
        self.mdns.register_mdns(self.name, self.port)
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.__run())
        finally:
            self.loop.close()


    def stop(self):
        self.cron.stop()
        self.task_repository.stop()
        self.mdns_registry.stop()
        self.manual_registry.stop()
        self.mdns.unregister_mdns(self.name)
        self.executors.shutdown(wait=False)
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
        logging.info("MCP Server stopped")


def run_server(port: int, dir, configs: Dict[str, Service], autoscan: bool):
    mcp_server = OpenActionServer(port, dir, configs, autoscan)
    try:
        mcp_server.start()
        while True:
            sleep(5)
    except KeyboardInterrupt:
        logger.info('Stopping the server...')
        mcp_server.stop()


if __name__ == '__main__':
    # Globally setup format and log level for the application root
    logging.basicConfig(format='%(asctime)s %(name)-20s: %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    # Silence chatty third-party modules
    logging.getLogger('tornado.access').setLevel(logging.ERROR)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    logging.getLogger('starlette.middleware.base').setLevel(logging.WARNING)
    logging.getLogger('fastmcp').setLevel(logging.WARNING)
    logging.getLogger('uvicorn.access').disabled = True
    logging.getLogger('uvicorn.error').setLevel(logging.WARNING)
    logging.getLogger('uvicorn').setLevel(logging.WARNING)

    port = int(sys.argv[1])
    work_dir = sys.argv[2]
    config = Configs.read(sys.argv[3])
    autoscan = sys.argv[4].upper() == 'ON'

    run_server(port, work_dir, config, autoscan)





# test with
# npx @modelcontextprotocol/inspector node build\index.js