import logging
import os
import sys
from typing import Dict
from time import sleep
from mcp_server import MCPServer
from cron_service import CronService
from mcp_client import McpRegistry
from store_service import Store
from task_registry import TaskRegistry, CodeRegistry
from task import Task



class OpenActionServer(MCPServer):
    """
    Custom MCP Server implementation that integrates a task registry,
    a cron-based scheduling service, and persistent storage.
    """

    def __init__(self, port: int, dir: str, mcp_server: Dict[str, str], autoscan):
        """
        Initializes the server and its core components.

        Args:
            port (int): The port on which the server will listen.
            dir (str): The base directory path for storing scripts and state.
        """
        # Initialize the parent MCPServer with name and port
        super().__init__("OpenAction", port)

        self.mcp_registry = McpRegistry(mcp_server, autoscan)
        self.store = Store(name="state", directory=dir)
        self.code_registry = CodeRegistry(codedir=os.path.join(dir, "tasks"))
        self.task_registry = TaskRegistry(self.code_registry).start()
        self.cron = CronService(self.store, self.mcp_registry, self.task_registry).start()


        @self.mcp.tool()
        def list_service_access() -> str:
            """
            Retrieves the complete api of all service access classes available.

            This tool is essential for understanding the available backend interfaces, method signatures,
            and data structures. Call this tool when you need to know exactly how to interact with the
            system's environment or to check which service methods can be used within your generated tasks.

            Returns:
                str: A formatted string containing the Python source code of all service access,
                     or an error message if the directory cannot be read.
            """

            try:
                api_dir = os.path.join(os.path.dirname(__file__), 'api')
                if not os.path.exists(api_dir):
                    return "No api directory found."

                services = []
                for filename in os.listdir(api_dir):
                    if filename.endswith(".py") and filename.endswith("_service.py"):
                        with open(os.path.join(api_dir, filename), "r", encoding="utf-8") as f:
                            services.append(f"--- {filename} ---\n```python\n{f.read()}\n```")

                if not services:
                    return "No service classes found in api directory."

                return "\n\n".join(services)
            except Exception as e:
                return f"Error reading api directory: {e}"


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
        def register_task(name: str, script: str, description: str, is_test_task: bool = False) -> str:
            """
            Registers a new python based task via the MCP interface.

            Args:
                name (str): The unique identifier for the task. This name must be
                    URI-safe (containing only alphanumeric characters, hyphens,
                    underscores, or dots).
                script (str): The Python code/script to be executed. Try to implement
                    the logic regarding a specific device (such as a heater or roller
                    shutter) in a single task (script). Avoid creating several scripts
                    regarding the same device.
                description (str): A brief explanation of what the task does.
                is_test_task (bool): Flag indicating whether this is a temporary task
                    used for validating API calls during script creation. Test tasks
                    should generally be removed once validation is complete.

            Returns:
                str: A confirmation message indicating the registration status.


            The script has to provide two functions:

                    def cron_cron() -> str:
                        Defines the execution schedule for the task.

                        Returns:
                        str: A standard 5-field cron expression
                        (minute, hour, day of month, month, day of week).


                    def execute(store: Dict[str, Any], mcp_service: Dict[str, Any]) -> str:
                        Callback function executed whenever the execution (e.g., due to a cron
                        schedule) is triggered.

                        This function contains the core logic of the task. It makes use of
                        the provided environment services such as a persistent state store
                        and a collection of configured MCP services. The available services
                        can be retrieved by using the list_provided_mcp_services tool.
                        The definition of the access classes can be retrieved by using
                        the list_service_access tool.

                        The script must also implement robust error handling. In case of an error,
                        an exception must be raised. Specifically, the responses from MCP clients
                        must be evaluated and checked for error states. Raising an exception will
                        cause the task to be retried 1 minute later.

                        When utilizing MCP services, API calls should be validated during script
                        creation before they are integrated into the final script. A temporary test
                        task can be registered for this purpose, but it should be removed later.

                        Args:
                            store (Dict[str, Any]): A task-specific persistence dictionary
                                for storing state across executions.
                            mcp_service (Dict[str, Any]): A mapping of client names to runtime
                                MCP services.

                        Returns:
                            str: A human-readable summary of the task execution result in a few sentences.
            """

            self.code_registry.register(name, script, description, is_test_task)
            return f"Task '{name}' has been successfully registered."



        # Expose this function as a callable tool over the Model Context Protocol
        @self.mcp.tool()
        def deregister_task(name: str) -> str:
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

                self.code_registry.deregister(name)
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
                task_to_execute: Task | None = None

                # Note: Assuming self.task_registry.tasks is a dict, we iterate over .values()
                for task in self.task_registry.tasks.values():
                    if hasattr(task, 'name') and task.name == name:
                        task_to_execute = task
                        break

                if task_to_execute is None:
                    return f"Error: Task '{name}' not found in registry. Available tasks: {[t.name for t in self.task_registry.tasks.values() if hasattr(t, 'name')]}"

                # Execute the task immediately and capture the result
                result = task_to_execute.run(self.store, self.mcp_registry)

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
        def backup_tasks() -> str:
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
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <port> <work_dir> [mcp_config] [autoscan]")
        print(f"  port: Server port (e.g., 9485)")
        print(f"  work_dir: Work directory (e.g., /etc/work)")
        print(f"  mcp_config: MCP server config, e.g. 'server1=http://host1:port1&server2=http://host2:port2' (default: empty)")
        print(f"  autoscan: Enable mDNS autodiscovery 'ON'/'OFF' (default: ON)")
        sys.exit(1)

    port = int(sys.argv[1])
    work_dir = sys.argv[2]
    mcp_config = sys.argv[3]
    autoscan = sys.argv[4].upper() == 'ON'

    run_server(port, work_dir, read_config(mcp_config), autoscan)
