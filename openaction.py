import os
import asyncio
import logging
import importlib.metadata
from pathlib import Path
from time import sleep
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
from datetime import datetime, timedelta

import sys
from fastmcp import FastMCP

from mdns import MDNS
from adapter_impl import AdapterManager
from cron import CronService
from http_support import HttpRegistry
from mcp_support import McpRegistry, McpServiceScanner
from services import ServiceRegistry, ServiceConfig, Configs
from store_impl import SimpleStore
from subscription import SubscriptionService
from task import TaskFactory
from task_repository import TaskRepository


logger = logging.getLogger(__name__)





class OpenActionServer:

    def __init__(self, port: int, dir: str, configs: Dict[str, ServiceConfig], autoscan: bool, host: str = "0.0.0.0"):
        self.name = 'OpenAction'
        self.host = host
        self.port = port
        self.store = SimpleStore(name="state", directory=dir)
        if autoscan:
            self.service_registry = ServiceRegistry(configs, {McpServiceScanner()})
        else:
            self.service_registry = ServiceRegistry(configs, list())
        self.mcp_registry = McpRegistry(self.service_registry)
        self.adapter_manager = AdapterManager({HttpRegistry.NAME: HttpRegistry(), McpRegistry.NAME: self.mcp_registry})
        self.subscription_service = SubscriptionService(self.adapter_manager)
        self.executors = ThreadPoolExecutor(max_workers=20, thread_name_prefix="Taskexecutor")
        self.task_factory = TaskFactory(self.store, self.adapter_manager, self.executors)
        self.task_repository = TaskRepository(os.path.join(dir, "tasks"), self.task_factory, self.store, self.adapter_manager, self.subscription_service)
        self.cron = CronService(self.task_repository)

        self.mcp_registry.start()
        self.task_repository.start()
        self.subscription_service.start()
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
                # Fetch the clean Python version string (e.g., "3.11.4")
                python_version = sys.version.split()[0]

                # Retrieve all installed distributions
                dists = importlib.metadata.distributions()

                # Extract name and version, filtering out empty names
                packages = [
                    (dist.metadata['Name'], dist.version)
                    for dist in dists
                    if dist.metadata.get('Name')
                ]

                # 1. Environment Header
                report = [
                    f"### 🐍 Python Environment: `v{python_version}`",
                    "---"
                ]

                # 2. Package List
                if not packages:
                    report.append("No external Python packages found in the current environment.")
                else:
                    # Sort case-insensitively for better readability
                    packages.sort(key=lambda x: x[0].lower())

                    report.extend([
                        "### 📦 Available External Packages",
                        "> **Important Notes for Scripting:**",
                        "> 1. **Standard Libraries:** All built-in Python modules for this version (e.g., `json`, `datetime`, `math`, `re`, `urllib`) are implicitly available and are *not* listed below.",
                        "> 2. **Import Names:** The names below are package distribution names. The actual import statement might differ slightly (e.g., package `PyYAML` is imported as `yaml`).",
                        ""
                    ])

                    # Format cleanly. E.g., "- **requests** (v2.31.0)"
                    report.extend([f"- **`{name}`** `(v{version})`" for name, version in packages])

                return "\n".join(report)


            except Exception as e:
                logger.error(f"Failed to list modules: {e}", exc_info=True)
                return f"Error: Could not retrieve the environment details: {type(e).__name__} - {str(e)}"

        @self.mcp.tool()
        def list_api() -> str:
            """
            Provides the source code definitions for all available adapter types (connectors) and the local Store.

            This tool serves as the 'Source of Truth' for developers writing task logic. It reveals
            the actual class definitions, method signatures, and docstrings for the components
            available in the execution environment.

            Runtime Behavior:
            - Both a 'Store' instance and an 'AdapterRegistry' are automatically injected into
              the task's execution context.
            - The 'Store' instance is used to maintain task state across different executions.
              It provides simple get/set methods and is backed by local file-based storage.
            - The 'AdapterRegistry' is used to retrieve concrete adapter instances by specifying
              the 'adapter_type' and an optional 'name'.
            - Note: Certain adapter types do not require a name and will return a default
              implementation if the name is omitted.
            - No manual Python imports are required within your task scripts to use these components.
            - Use the source code provided by this tool to understand the available methods
              and expected data structures for both the Store and all connectors.

            Returns:
                str: A formatted string containing the Python source code of the Store interface
                     as well as all available adapter classes.
            """
            try:
                # Resolve the absolute path to the 'api' directory relative to this file
                api_dir = Path(__file__).parent / "api"

                if not api_dir.is_dir():
                    return "Error: No 'api' directory found."

                apis = []

                for file_path in api_dir.glob("*"):
                    if file_path.is_file():
                        # read_text safely opens, reads, and closes the file automatically
                        content = file_path.read_text(encoding="utf-8")
                        apis.append(f"--- {file_path.name} ---\n```python\n{content}\n```")

                if not apis:
                    return "No api classes found in the 'api' directory."

                return "\n\n".join(apis)

            except Exception as e:
                # Providing the exception type makes debugging significantly easier
                return f"Error reading API directory: {type(e).__name__} - {e}"


        @self.mcp.tool()
        def register_task(name: str,
                          script: str,
                          description: str,
                          subscriptions: Optional[List[str]] = None,
                          cron: Optional[str] = None,
                          run_on_start: bool = False) -> str:
            """
            Registers a new permanent Python-based task

            Args:
                name (str): A unique, URI-safe identifier (alphanumeric, hyphens, underscores, or dots).
                    Note: Production tasks MUST NOT start with the 'test_' prefix.
                script (str): The procedural Python source code to execute.
                    Constraint: Consolidate all logic for a specific device (e.g., a single heater
                    or roller shutter) into one script rather than creating multiple fragmented tasks.
                description (str): A brief, clear explanation of the task's logic and purpose.

                --- Trigger Configurations ---
                subscriptions (list of str, optional): Notification-based trigger. A list of
                    identifiers (e.g., MCP resource URIs). If provided, the task subscribes to
                    these properties for real-time, event-driven execution.
                cron (str, optional): Clock-based trigger. A standard cron expression defining
                    a recurring execution schedule.
                run_on_start (bool): Startup trigger. If True, the task executes immediately
                    upon registration or system boot.

            Returns:
                str: A confirmation message indicating the registration status.

            ==============================
            SCRIPT EXECUTION ENVIRONMENT
            ==============================

            The script is executed as a procedural block. The following objects are
            injected automatically into the global scope:

                1. `store`: A persistence object for maintaining state across execution cycles.
                   E.g. use this to cache values and minimize redundant external service calls.
                2. `registry`: A centralized manager used to retrieve specific adapters
                   (e.g., `registry.get_adapter("http_adapter")`).


            ==============================
            MANDATORY PROTOCOLS
            ==============================

            Error Handling & Retries:
                The script MUST evaluate all external service responses for error states.
                If a failure occurs, an Exception MUST be raised. This ensures the system
                triggers the automatic retry logic (1-minute delay).

            Validation & Consolidation (Required):
                1. Check Existing: Verify if a task already exists for the target device.
                   If found, MERGE your logic into the existing script.
                2. Test-First: You MUST use the `execute_task` tool to validate your script
                   logic and JSON parsing BEFORE calling `register_task`.
            """

            # Guard clause: Ensure the user isn't trying to deploy a test script permanently
            if name.startswith("test_"):
                return (
                    "Error: Validation failed. Production tasks cannot start with 'test_'. "
                    "If you are trying to test a script, use the `execute_task` tool instead."
                )

            # Normalize defaults to prevent NoneType errors downstream
            subs = subscriptions if subscriptions is not None else []

            try:
                self.task_repository.register(
                    name=name,
                    code=script,
                    description=description,
                    cron=cron,
                    subscriptions=subs,
                    run_on_start=run_on_start,
                    ttl=None,
                    is_test=False
                )

                logger.info(f"Production Task '{name}' registered successfully.")
                return f"Success: Task '{name}' has been successfully registered as a persistent production task."

            except Exception as e:
                logger.error(f"Failed to register task '{name}': {e}", exc_info=True)
                return f"Error: An internal error occurred during registration: {type(e).__name__} - {str(e)}"


        @self.mcp.tool()
        def list_provided_services() -> str:
            """
            Lists all external hardware and software services currently available to OpenAction.

            DISCOVERY FLOW:
            1. Call this tool to identify WHAT is connected (e.g., 'hue_lights', 'office_shutter').
            2. Call 'list_api' to determine HOW to interact with them via code.

            Service Categorization & Access:
            - MCP [SSE]: Native Model Context Protocol servers.
              Access: Use `registry.get_adapter("mcp_adapter", "service_name")`.
            - HTTP [REST]: Standard web services.
              Access: Use `registry.get_adapter("mcp_adapter")`.
            - SHELLY: Local IoT devices (switches, roller shutters, etc.).
              Access: Use `registry.get_adapter("mcp_adapter")`
              IMPORTANT: Specific API structures for Shelly are NOT provided by 'list_service_apis'.
              You must implement the correct HTTP endpoints (e.g., Shelly Gen 1 '/relay/0'
              or Gen 2 RPC calls) based on the device URL and standard Shelly documentation.

            Returns:
                str: A formatted list containing service names, protocols, endpoints, and reachability status.
            """
            try:
                if not self.service_registry.registered_services:
                    return "Environment Empty: No external services are currently configured."

                output = [
                    "Active Service Environment:",
                    "===========================",
                    "Status Legend: [✔] Reachable | [✘] Unreachable\n",
                    "Use the names below as identifiers in your Python scripts:\n"
                ]

                # Iterate through registered services to build the discovery list
                for name, conf in self.service_registry.registered_services.items():
                    is_reachable = name in self.service_registry.reachable_services
                    status_icon = "✔" if is_reachable else "✘"
                    svc_type = str(conf.type).upper()

                    output.append(f"• [{status_icon}] **{conf.name}** [{svc_type}]: `{conf.url}`")
                    output.append("")  # Spacer for readability

                return "\n".join(output)

            except Exception as e:
                logger.error(f"Discovery Error: {e}", exc_info=True)
                return f"Error: Could not map the environment: {type(e).__name__} - {str(e)}"


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
                    cron_expression=None,
                    subscriptions=[],
                    run_on_start=False,
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
        self.subscription_service.stop()
        self.task_repository.stop()
        self.mcp_registry.stop()
        self.mdns.unregister_mdns(self.name)
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
        logging.info("MCP Server stopped")


def run_server(port: int, dir, configs: Dict[str, ServiceConfig], autoscan: bool):
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





# test with npx @modelcontextprotocol/inspector node build\index.js