import os
import asyncio
import logging
import threading
import socket
from pathlib import Path
from time import sleep
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime, timedelta

import sys
from fastmcp import FastMCP, Context
from zeroconf import IPVersion, ServiceInfo, Zeroconf

from cron import CronService
from mcp_client import McpRegistry
from services import ServiceRegistry, ServiceConfig, Configs
from store import Store
from task import TaskFactory, TaskAdapter
from task_repository import TaskRepository


logger = logging.getLogger(__name__)




class MDNS:
    def __init__(self):
        self.registered: Dict[str, ServiceInfo] = dict()
        self.zc = Zeroconf(ip_version=IPVersion.V4Only)
        self.service_type = "_mcp._tcp.local."
        self.hostname = socket.gethostname()
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            self.local_ip = s.getsockname()[0]
        finally:
            s.close()

    def register_mdns(self, name: str, port: int):
        try:
            service_name = f"{name}.{self.service_type}"
            service_info = ServiceInfo(
                type_=self.service_type,
                name=service_name,
                addresses=[socket.inet_aton(self.local_ip)],
                port=port,
                properties={
                    "version": "1.0",
                    "path": "/sse",
                    "server_type": "fastmcp"
                },
                server=f"{self.hostname}.local.",
            )

            logging.info(f"mDNS: Registering {service_name} at {self.local_ip}:{port}")
            self.zc.register_service(service_info)
            self.registered[name] = service_info
        except Exception as e:
            logging.error(f"mDNS Registration failed: {e}")

    def unregister_mdns(self, name: str):
        service_info = self.registered.get(name)
        if service_info is not None:
            logging.info("mDNS: Unregistering service...")
            self.zc.unregister_service(service_info)
            self.zc.close()



class OpenActionServer:

    def __init__(self, port: int, dir: str, configs: Dict[str, ServiceConfig], autoscan: bool, host: str = "0.0.0.0"):
        self.name = 'OpenAction'
        self.host = host
        self.port = port
        self.config = configs
        self.mcp_registry = McpRegistry(ServiceRegistry(configs, autoscan))
        self.store = Store(name="state", directory=dir)
        self.task_repository = TaskRepository(os.path.join(dir, "tasks"), TaskFactory(self.store, self.mcp_registry)).start()
        self.cron = CronService(self.task_repository).start()

        self.mdns = MDNS()
        self.mcp = FastMCP(self.name)
        self.loop = asyncio.new_event_loop()

        @self.mcp.tool()
        def list_service_apis() -> str:
            """
            Retrieves the Python source code defining all available service client interfaces.

            CRITICAL: This is your primary discovery tool for writing task logic.
            It reveals the actual class definitions, method signatures, and docstrings
            for the clients injected into your 'execute' function.

            Note: No Python imports are required to use the injected services.
            They are provided automatically as arguments at runtime.

            Use this tool to:
            1. Check which methods exist on a specific registry.
            2. Understand the required arguments and return types for hardware interactions.
            3. Identify the correct data structures for state persistence.

            Returns:
                str: Python source code of all available service access classes.
            """
            try:
                # Resolve the absolute path to the 'api' directory relative to this file
                api_dir = Path(__file__).parent / "api"

                if not api_dir.is_dir():
                    return "Error: No 'api' directory found."

                services = []

                # Use glob to effortlessly find all matching service files
                for file_path in api_dir.glob("*_service.py"):
                    if file_path.is_file():
                        # read_text safely opens, reads, and closes the file automatically
                        content = file_path.read_text(encoding="utf-8")
                        services.append(f"--- {file_path.name} ---\n```python\n{content}\n```")

                if not services:
                    return "No service classes found in the 'api' directory."

                return "\n\n".join(services)

            except Exception as e:
                # Providing the exception type makes debugging significantly easier
                return f"Error reading API directory: {type(e).__name__} - {e}"


        @self.mcp.tool()
        def list_provided_services() -> str:
            """
            Lists all external hardware and software services currently available to OpenAction.

            DISCOVERY FLOW:
            1. Call this tool to see WHAT is connected (e.g., 'hue_lights', 'office_shutter').
            2. Call 'list_service_client' to see HOW to use the code to talk to them.

            This tool distinguishes between:
            - MCP [SSE]: Native Model Context Protocol servers. Access via 'mcp_registry.get("name")'.
            - HTTP [REST]: Standard web services. Access via 'http_client'.
            - SHELLY: Local IoT devices (smart switches, roller shutters, etc.).
              Access via: The injected `http_client` to call their URL.
              IMPORTANT: The specific API is NOT provided by 'list_service_apis'.
              You must determine the correct HTTP endpoints yourself (e.g., Shelly Gen 1
              '/relay/0' or Gen 2 RPC) based on the device URL and common Shelly documentation.

            Returns:
                str: A formatted list of service names, protocols, and endpoints.
            """
            try:
                if not hasattr(self, 'config') or not self.config:
                    return "Environment Empty: No external services are currently configured."

                output = [
                    "Active Service Environment:",
                    "===========================\n",
                    "Use the names below as identifiers in your Python scripts:\n"
                ]

                for conf in self.config.values():
                    svc_type = str(conf.type).upper()
                    output.append(f"• **{conf.name}** [{svc_type}]: `{conf.url}`")
                    output.append("") # Spacer for readability

                return "\n".join(output)

            except Exception as e:
                logger.error(f"Discovery Error: {e}", exc_info=True)
                return f"Error: Could not map the environment: {type(e).__name__} - {str(e)}"


        # Expose this function as a callable tool over the Model Context Protocol
        @self.mcp.tool()
        def register_task(name: str, script: str, description: str, is_test: bool, ttl:int = None) -> str:
            """
            Registers a new Python-based task via the MCP interface.

            Args:
                name (str): The unique identifier for the task. Must be URI-safe
                    (alphanumeric, hyphens, underscores, or dots).
                    A test task name must start with 'test_' and include a `ttl`
                script (str): The Python source code to be executed. Note: Consolidate
                    the logic for a specific device (e.g., a single heater or roller
                    shutter) into a single task script rather than creating multiple.
                description (str): A brief explanation of what the task does.
                is_test (bool): Flag to distinguish between temporary validation and
                    permanent deployment.
                ttl (int, optional): The "Time To Live" in seconds. This is used for
                    test tasks only. Test tasks should always be created with a TTL
                    greater than 900 (15 min) to avoid orphaned processes

            Returns:
                str: A confirmation message indicating the registration status.

            ====================
            SCRIPT REQUIREMENTS
            ====================
            The provided `script` string MUST define the following two functions:

            1. `def cron():`
                Defines the execution schedule for the task.
                Returns:
                    A standard 5-field cron expression (minute, hour, day of month, month, day of week).

            2. `def execute(store_service, mcp_registry, http_client):`
                The callback function executed whenever the task is triggered by the cron schedule.
                This function contains the core logic of the task. Environment clients will be
                injected for each task individually within a dedicated namespace.
                Returns:
                    A human-readable summary of the task execution result and the
                    updated device state, if applicable  (in a few sentences).

                Available Environment Tools:
                    Before implementation, call these tools to map the environment:
                    - `list_provided_services()`: To find available services
                    - `list_service_apis()`: To retrieve method signatures for the injected registries.

                Injected parameters:
                    store_service (StoreService): A persistence service for storing state across executions.
                    mcp_registry (MCPClientRegistry): A registry for accessing configured MCP clients.
                    http_client (HttpClient): A http client with cached sessions

                Mandatory Error Handling:
                    The script must implement robust error handling. Responses from external clients
                    and services MUST be explicitly evaluated for error states. If an error occurs,
                    an Exception MUST be raised. Raising an exception ensures the system will
                    automatically retry the task 1 minute later.

                Validation & Consolidation Protocol (Mandatory):
                    Before final deployment, you MUST adhere to the following workflow:
                    1. Check Existing: Verify no script already exists managing the same device.
                       If one exists, MERGE your logic into the existing task.
                    2. Test First: Register a temporary test task (is_test=True).
                    3. Naming: The test task name MUST start with 'test_' and include a `ttl`.
                    4. Verify: Trigger execution manually via the `execute_task_now` tool and
                       ensure all JSON response structures are correctly handled.
                    5. Cleanup: Delete the test task after validation.
            """

            if is_test:
                # Rule 1: Test tasks must start with 'test_'
                if not name.startswith("test_"):
                    return f"Error: Validation failed. Test task names must start with 'test_'. Received: '{name}'"

                # Rule 2: Test tasks must have a TTL
                if ttl is None:
                    return "Error: Validation failed. Test tasks (is_test=True) must include a 'ttl' value."

                # Rule 3: Minimum TTL of 900 seconds (15 minutes)
                if ttl < 900:
                    return f"Error: Validation failed. Test task 'ttl' must be at least 900 seconds to prevent orphaned processes. Received: {ttl}"
            else:
                # Rule 4: Production tasks should not start with 'test_' to avoid confusion
                if name.startswith("test_"):
                    return f"Error: Validation failed. Production tasks (is_test=False) should not start with 'test_'."

            try:
                self.task_repository.register(name, script, description, ttl)

                if ttl is None:
                    logger.info(f"Production Task '{name}' registered successfully.")
                    return f"Task '{name}' has been successfully registered as a persistent production task."
                else:
                    logger.info(f"Test Task '{name}' registered successfully with ttl={ttl}s.")
                    return f"Test task '{name}' has been successfully registered and will be removed in {ttl} seconds."

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
            Retrieves a concise list of all registered tasks.
            """
            try:
                # Pythonic check for empty dictionary
                if not self.task_repository.tasks:
                    return "Currently, no tasks are registered."

                output = ["### Registered Tasks", "=================\n"]

                # Iterate over tasks and extract only name and description
                for task in self.task_repository.tasks.values():
                    output.append(f"• **{task.name}** [{task.state}]: {task.description}")

                return "\n".join(output)

            except Exception as e:
                logger.error(f"Failed to list tasks: {e}", exc_info=True)
                return "Error: Could not retrieve tasks from the registry. Check server logs."

        @self.mcp.tool()
        def get_task(name: str) -> str:
            """
            Retrieves detailed information and status for a specific task.
            """
            try:
                # Use .get() to avoid redundant dictionary lookups
                task = self.task_repository.tasks.get(name)

                if not task:
                    return f"Error: Task '{name}' is not currently registered."

                # Updated headers to reflect a single task rather than a list
                output = [f"### Task Overview: {task.name}", "=================\n"]
                output.append(f"**Description:** {task.description}\n")

                output.append(f"**State:** {task.state}\n")

                # Handle stored task state
                task_data = task.data()
                if task_data:
                    output.append("**Stored Data:**")
                    for k, v in task_data.items():
                        v_str = str(v)
                        if len(v_str) > 200:
                            v_str = v_str[:197] + "..."
                        output.append(f"  - `{k}`: `{v_str}`")
                    output.append("") # Spacing for readability

                # Handle execution history (Pythonic check for empty lists)
                if not task.last_executions:
                    output.append("**Last Results:** Never executed\n")
                else:
                    output.append("**Last Results:**")
                    for run in reversed(task.last_executions):
                        timestamp = "n/a" if run.date is None else run.date.isoformat()
                        result_text = run.result if run.error is None else f"ERROR: {run.error}"

                        if len(result_text) > 600:
                            result_text = result_text[:597] + "..."
                        output.append(f"  - `{timestamp}`: {result_text}")
                    output.append("") # Spacing for readability

                output.append("**Source Code:**")
                output.append(f"```python\n{task.code}\n```")
                output.append("---\n")

                return "\n".join(output)

            except Exception as e:
                # Fixed copy-paste error in the log and return message
                logger.error(f"Failed to retrieve task '{name}': {e}", exc_info=True)
                return f"Error: Could not retrieve task '{name}'. Check server logs."


        @self.mcp.tool()
        def execute_task_now(name: str) -> str:
            """
            Triggers the immediate manual execution of a registered Python task.
            """
            try:
                # Find the task by name in the registry
                task_to_execute: TaskAdapter | None = None

                # Note: Assuming self.task_registry.tasks is a dict, we iterate over .values()
                for task in self.task_repository.tasks.values():
                    if hasattr(task, 'name') and task.name == name:
                        task_to_execute = task
                        break

                if task_to_execute is None:
                    available_tasks = [t.name for t in self.task_repository.tasks.values() if hasattr(t, 'name')]
                    return f"Error: Task '{name}' not found in registry. Available tasks: {available_tasks}"

                # Execute the task immediately and capture the result
                raw_result = task_to_execute.run()

                # Limit the result output to 300 characters to prevent log/token flooding
                result_str = str(raw_result)
                if len(result_str) > 300:
                    result_str = result_str[:297] + "..."

                logger.info(f"Task '{name}' executed \nResult: {result_str}")

                # Format execution timestamp if available
                timestamp = getattr(task_to_execute, 'last_execution', None)
                if timestamp:
                    return f"Task '{name}' executed at {timestamp.isoformat()}.\nResult: {result_str}"
                else:
                    return f"Task '{name}' executed \nResult: {result_str}"

            except Exception as e:
                logger.error(f"Failed to execute task '{name}': {e}", exc_info=True)
                return f"Error: Failed to execute task '{name}': {str(e)}"

        @self.mcp.tool()
        def run_backup() -> str:
            """
            Creates a backup of all registered task scripts and descriptions.
            """
            try:
                # Call the backup method we implemented in the CodeRegistry
                backup_path = self.task_repository.backup()

                if backup_path:
                    return f"Successfully created backup of all tasks. File saved at: {backup_path}"
                else:
                    return "Error: Backup process failed. Please check the server logs for details."

            except Exception as e:
                logger.error(f"Failed to execute backup_tasks tool: {e}", exc_info=True)
                return f"Error: Failed to create backup: {str(e)}"


        @self.mcp.tool()
        def list_backups() -> str:
            """
            Lists all existing backup files of registered tasks.
            """
            try:
                backups = self.task_repository.list_backup()

                if not backups:
                    return "No backups found. Create a backup using the 'run_backup' tool."

                output = ["Available Backups:", "=================\n"]
                for i, backup in enumerate(backups, 1):
                    output.append(f"{i}. {backup}")

                return "\n".join(output)

            except Exception as e:
                logger.error(f"Failed to list backups: {e}", exc_info=True)
                return f"Error: Could not retrieve backup list: {str(e)}"


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