import logging
import os
import sys
from pathlib import Path
from typing import Dict
from time import sleep
from mcp_server import MCPServer
from cron_service import CronService
from mcp_client import McpRegistry
from http_client import HttpClient, AutoRecreateHttpClient
from store_service import Store
from task import TaskAdapter
from task_registry import TaskRegistry, CodeRepository



class OpenActionServer(MCPServer):
    """
    Custom MCP Server implementation that integrates a task registry,
    a cron-based scheduling service, and persistent storage.
    """

    def __init__(self, port: int, dir: str, mcp_server: Dict[str, str],autoscan: bool):
        """
        Initializes the server and its core components.

        Args:
            port (int): The port on which the server will listen.
            dir (str): The base directory path for storing scripts and state.
        """
        # Initialize the parent MCPServer with name and port
        super().__init__("OpenAction", port)

        self.mcp_registry = McpRegistry(mcp_server, autoscan)
        self.http_client = AutoRecreateHttpClient()
        self.store = Store(name="state", directory=dir)
        self.code_registry = CodeRepository(codedir=os.path.join(dir, "tasks"))
        self.task_registry = TaskRegistry(self.code_registry).start()
        self.cron = CronService(self.store, self.mcp_registry, self.task_registry, self.http_client).start()



        @self.mcp.tool()
        def list_service_api() -> str:
            """Retrieves the complete API of all available service access classes.

            This tool is essential for understanding the available backend interfaces,
            method signatures, and data structures. Call this tool when you need to
            know exactly how to interact with the system's environment or to check
            which service methods can be used within your generated tasks.

            Returns:
                str: A formatted string containing the Python source code of all
                    service access classes, or an error message if the directory
                    cannot be read.
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
        def list_provided_mcp_services() -> str:
            """
            Lists the external MCP services that are currently configured and available to tasks.

            Returns:
                str: A formatted string of available MCP server names and their connection details (e.g., URLs).
            """
            try:
                if not self.mcp_registry.keys():
                    return "No external MCP services are currently configured."
                output = ["Configured MCP Servers:", "========================\n"]
                for name in self.mcp_registry.keys():
                    output.append(f"• **{name}**: `{self.mcp_registry[name].url}`")

                return "\n".join(output)

            except Exception as e:
                logging.error(f"Failed to list provided MCP services: {e}", exc_info=True)
                return "Error: Could not retrieve MCP service configurations."


        # Expose this function as a callable tool over the Model Context Protocol
        @self.mcp.tool()
        def register_task(name: str, script: str, description: str, ttl:int = None) -> str:
            """
            Registers a new Python-based task via the MCP interface.

            Args:
                name (str): The unique identifier for the task. Must be URI-safe
                    (alphanumeric, hyphens, underscores, or dots).
                script (str): The Python source code to be executed. Note: Consolidate
                    the logic for a specific device (e.g., a single heater or roller
                    shutter) into a single task script rather than creating multiple.
                description (str): A brief explanation of what the task does.
                ttl (int, optional): The "Time To Live" in seconds. This is used for
                    test tasks only. Test tasks should always be created with a TTL
                    greater than 900 (15 min) to avoid orphaned processes

            Returns:
                str: A confirmation message indicating the registration status.

            ====================
            SCRIPT REQUIREMENTS
            ====================
            The provided `script` string MUST define the following two functions:

            1. `def cron() -> str:`
                Defines the execution schedule for the task.
                Returns:
                    str: A standard 5-field cron expression (minute, hour, day of month, month, day of week).

            2. `def execute(store_service: StoreService, mcp_registry: MCPClientRegistry, session: HttpSession) -> str:`
                The callback function executed whenever the task is triggered by the cron schedule.
                This function contains the core logic of the task.

                Available Environment Tools:
                    Before implementation, call these tools to map the environment:
                    - `list_provided_mcp_services()`: To find available MCP client names.
                    - `list_service_api()`: To retrieve method signatures for the injected registries.

                Injected parameters:
                    store_service (StoreService): A persistence service for storing state across executions.
                    mcp_registry (MCPClientRegistry): A registry for accessing configured MCP clients.
                    http_client (HttpClient): A http client with cached sessions

                Nested Returns:
                    str: A human-readable summary of the task execution result in a few sentences.

                Mandatory Error Handling:
                    The script must implement robust error handling. Responses from external clients
                    and services MUST be explicitly evaluated for error states. If an error occurs,
                    an Exception MUST be raised. Raising an exception ensures the system will
                    automatically retry the task 1 minute later.

                Script Validation Protocol:
                    Before final registration, you MUST validate the script logic and
                    API calls by registering a temporary test task.
                    - Test task names MUST start with 'test.'
                    - Test tasks MUST include a `ttl`.
                    - Test tasks must be deleted after validation.
                    Ensure all JSON response structures are handled correctly before
                    deploying the final production version.
            """
            self.code_registry.register(name, script, description, ttl)
            self.task_registry.reload()
            if ttl is None:
                logging.info(f"Task '{name}' registered successfully.")
            else:
                logging.info(f"Task '{name}' registered successfully. ttl={ttl}")
            return f"Task '{name}' has been successfully registered."



        # Expose this function as a callable tool over the Model Context Protocol
        @self.mcp.tool()
        def deregister_task(name: str, reason: str) -> str:
            """
            Deregisters and removes a previously registered task.

            Args:
                name (str): The unique name of the task to remove.

            Returns:
                str: A confirmation message indicating the operation's result.
            """
            try:
                # Check if the task exists in the active registry
                if name not in self.task_registry.tasks:
                    return f"Error: Task '{name}' not found."

                self.code_registry.deregister(name, reason)
                self.task_registry.reload()

                logging.info(f"Task '{name}' unregistered successfully.")
                return f"Task '{name}' has been successfully unregistered."

            except Exception as e:
                logging.error(f"Failed to unregister task '{name}': {e}", exc_info=True)
                return f"Error: Failed to unregister task '{name}': {str(e)}"


        @self.mcp.tool()
        def list_tasks() -> str:
            """
            Lists all currently registered tasks.

            Returns:
                str: A formatted string containing the names, descriptions, and code of all registered tasks.
            """
            try:
                if len(self.task_registry.tasks) == 0:
                    return "Currently, no tasks are registered."

                output = ["Registered Tasks:", "=================\n"]

                for task in self.task_registry.tasks.values():
                    output.append(f"• **{task.name}**: {task.description}")

                    if len(task.last_executions) == 0:
                        output.append("  Last results: never")
                    else:
                        output.append("  Last results:")
                        for run in reversed(task.last_executions):
                            timestamp = "n/a" if run.date is None else run.date.isoformat()
                            result_text = run.result if run.error is None else str(run.error)
                            if len(result_text) > 600:
                                result_text = result_text[:597] + "..."
                            output.append(f"    - `{timestamp}`: `{result_text}`")

                    output.append("  Code:")
                    output.append(f"```python\n{task.code}\n```")
                    output.append("-------------------\n")

                return "\n".join(output)

            except Exception as e:
                logging.error(f"Failed to list tasks: {e}", exc_info=True)
                return "Error: Could not retrieve tasks from the registry. Check server logs."


        @self.mcp.tool()
        def execute_task_now(name: str) -> str:
            """
            Executes a registered task immediately, bypassing the cron schedule.

            Args:
                name (str): The name of the task to execute.

            Returns:
                str: A confirmation message with execution status and timestamp.
            """
            try:
                # Find the task by name in the registry
                task_to_execute: TaskAdapter | None = None

                # Note: Assuming self.task_registry.tasks is a dict, we iterate over .values()
                for task in self.task_registry.tasks.values():
                    if hasattr(task, 'name') and task.name == name:
                        task_to_execute = task
                        break

                if task_to_execute is None:
                    return f"Error: Task '{name}' not found in registry. Available tasks: {[t.name for t in self.task_registry.tasks.values() if hasattr(t, 'name')]}"

                # Execute the task immediately and capture the result
                result = task_to_execute.run(self.store, self.mcp_registry, self.http_client)

                # Format execution timestamp if available
                timestamp = getattr(task_to_execute, 'last_execution', None)
                if timestamp:
                    return f"Task '{name}' executed successfully at {timestamp.isoformat()}.\nResult: {result}"
                else:
                    return f"Task '{name}' executed successfully.\nResult: {result}"

            except Exception as e:
                logging.error(f"Failed to execute task '{name}': {e}", exc_info=True)
                return f"Error: Failed to execute task '{name}': {str(e)}"



        @self.mcp.tool()
        def run_backup() -> str:
            """
            Creates a backup of all registered task scripts and descriptions.

            Returns:
                str: A confirmation message containing the absolute path to the backup file,
                     or an error message if the backup failed.
            """
            try:
                # Call the backup method we implemented in the CodeRegistry
                backup_path = self.code_registry.backup()

                if backup_path:
                    return f"Successfully created backup of all tasks. File saved at: {backup_path}"
                else:
                    return "Error: Backup process failed. Please check the server logs for details."

            except Exception as e:
                logging.error(f"Failed to execute backup_tasks tool: {e}", exc_info=True)
                return f"Error: Failed to create backup: {str(e)}"


        @self.mcp.tool()
        def list_backups() -> str:
            """
            Lists all existing backup files of registered tasks.

            Returns:
                str: A formatted string containing all available backup filenames,
                     sorted with newest first, or a message if no backups exist.
            """
            try:
                backups = self.code_registry.list_backup()

                if not backups:
                    return "No backups found. Create a backup using the 'run_backup' tool."

                output = ["Available Backups:", "=================\n"]
                for i, backup in enumerate(backups, 1):
                    output.append(f"{i}. {backup}")

                return "\n".join(output)

            except Exception as e:
                logging.error(f"Failed to list backups: {e}", exc_info=True)
                return f"Error: Could not retrieve backup list: {str(e)}"



def run_server(port: int, dir, mcp_server_uris: Dict[str, str], autoscan: bool):
    mcp_server = OpenActionServer(port, dir, mcp_server_uris, autoscan)
    try:
        mcp_server.start()
        while True:
            sleep(5)
    except KeyboardInterrupt:
        logging.info('Stopping the server...')
        mcp_server.stop()



def read_config(mcp_servers: str) -> Dict[str, str]:
    """
    Parses a configuration string into a dictionary.
    Example: "rolladen=http://22.322/zt&licht=http://33.5" -> {"rollershutter": "http://...", "light": "..."}
    """
    config = {}

    if not mcp_servers:
        return config

    # Split by '&' to get the individual server definitions
    pairs = mcp_servers.split('&')

    for pair in pairs:
        # Ignore empty segments (e.g., if "&&" is accidentally in the string)
        if not pair.strip():
            continue

        # Split only on the FIRST '=' in case the URL itself contains an '='
        parts = pair.split('=', 1)

        if len(parts) == 2:
            name = parts[0].strip()
            url = parts[1].strip()
            config[name] = url
        else:
            logging.warning(f"Ignoring invalid configuration entry: '{pair}'")

    return config


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(name)-20s: %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    logging.getLogger('tornado.access').setLevel(logging.ERROR)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    logging.getLogger('starlette.middleware.base').setLevel(logging.WARNING)
    logging.getLogger('fastmcp').setLevel(logging.WARNING)

    port = int(sys.argv[1])
    work_dir = sys.argv[2]
    mcp_config = sys.argv[3]
    autoscan = sys.argv[4].upper() == 'ON'

    run_server(port, work_dir, read_config(mcp_config), autoscan)
