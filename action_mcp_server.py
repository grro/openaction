import logging
import os
import sys
from typing import Dict
from time import sleep
from mcp_service import MCPServer
from cron_service import CronService
from mcp_client import McpRegistry
from store_service import Store
from task_registry import TaskRegistry, CodeRegistry
from  task import Task



class ActionMCPServer(MCPServer):
    """
    Custom MCP Server implementation that integrates a task registry,
    a cron-based scheduling service, and persistent storage.
    """

    def __init__(self, port: int, dir: str, mcp_server: Dict[str, str]):
        """
        Initializes the server and its core components.

        Args:
            port (int): The port on which the server will listen.
            dir (str): The base directory path for storing scripts and state.
        """
        # Initialize the parent MCPServer with name and port
        super().__init__("Actions", port)

        self.mcp_registry = McpRegistry(mcp_server)
        self.store = Store(os.path.join(dir, "state"))
        self.code_registry = CodeRegistry(os.path.join(dir, "rules"))
        self.task_registry = TaskRegistry(self.code_registry).start()
        self.cron = CronService(self.store, self.mcp_registry, self.task_registry).start()


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



        @self.mcp.tool()
        def provided_mcp_servers() -> str:
            """
            Lists the external MCP servers that are currently configured and available to tasks.

            Returns:
                str: A formatted string of available MCP server names and their connection details (e.g., URLs).
            """
            try:
                if not self.mcp_registry.keys():
                    return "No external MCP servers are currently configured."
                output = ["Configured MCP Servers:", "========================\n"]
                for name in self.mcp_registry.keys():
                    output.append(f"• **{name}**: `{self.mcp_registry[name].url}`")

                return "\n".join(output)

            except Exception as e:
                logging.error(f"Failed to list provided MCP servers: {e}", exc_info=True)
                return "Error: Could not retrieve MCP server configurations."


        # Expose this function as a callable tool over the Model Context Protocol
        @self.mcp.tool()
        def register_task(name: str, script: str, description: str) -> str:
            """
            Registers a new python based task via the MCP interface.

            Args:
                name (str): The unique identifier for the task. This name must be
                    URI-safe (containing only alphanumeric characters, hyphens,
                    underscores, or dots).
                script (str): The Python code/script to be executed.
                description (str): A brief explanation of what the task does.

            Returns:
                str: A confirmation message indicating the registration status.


            The script has to provide two functions:

                    def cron_cron() -> str:
                        Defines the execution schedule for the task.

                        Returns:
                        str: A standard 5-field cron expression
                        (minute, hour, day of month, month, day of week).


                    def execute(store: Dict[str, Any], mcp: Dict[str, Any]) -> str:
                        Callback function executed whenever the execution (e.g., due to a cron
                        schedule) is triggered.

                        This function contains the core logic of the task. It provides access to
                        a persistent state store and a collection of configured MCP clients.

                        The script must also implement robust error handling. In case of an error,
                        an exception must be raised. Specifically, the responses from MCP clients
                        must be evaluated and checked for error states. Raising an exception will
                        cause the task to be retried 1 minute later.

                        Args:
                            store (Dict[str, Any]): A task-specific persistence dictionary
                                for storing state across executions.
                            mcp (Dict[str, Any]): A mapping of client names to runtime
                                MCP context. An MCPClient supports the method:
                                `call_tool(name: str, arguments: dict = None)`
                                to execute a tool request.

                        Returns:
                            str: A human-readable summary of the task execution result in a few sentences.
            """

            self.code_registry.register(name, script, description)
            return f"Task '{name}' has been successfully registered."


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
                for task in self.task_registry.tasks:
                    if hasattr(task, 'name') and task.name == name:
                        task_to_execute = task
                        break

                if task_to_execute is None:
                    return f"Error: Task '{name}' not found in registry. Available tasks: {[t.name for t in self.task_registry.tasks if hasattr(t, 'name')]}"

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



def run_server(port: int, dir, mcp_server_uris: Dict[str, str]):
    mcp_server = ActionMCPServer(port, dir, mcp_server_uris)
    try:
        mcp_server.start()
        while True:
            sleep(5)
    except KeyboardInterrupt:
        logging.info('stopping the server')
        mcp_server.stop()



def read_config(mcp_servers: str) -> Dict[str, str]:
    """
    Parses a configuration string into a dictionary.
    Example: "rolladen=http://22.322/zt&licht=http://33.5" -> {"rolladen": "http://...", "licht": "..."}
    """
    config = {}

    if not mcp_servers:
        return config

    # Am '&' splitten, um die einzelnen Server-Definitionen zu erhalten
    pairs = mcp_servers.split('&')

    for pair in pairs:
        # Leere Segmente ignorieren (z.B. wenn versehentlich "&&" im String steht)
        if not pair.strip():
            continue

        # Nur am ERSTEN '=' splitten, falls die URL selbst '=' enthält
        parts = pair.split('=', 1)

        if len(parts) == 2:
            name = parts[0].strip()
            url = parts[1].strip()
            config[name] = url
        else:
            logging.warning(f"Ignoriere ungültigen Konfigurations-Eintrag: '{pair}'")

    return config


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(name)-20s: %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    logging.getLogger('tornado.access').setLevel(logging.ERROR)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
    run_server(int(sys.argv[1]), sys.argv[2], read_config(sys.argv[3]))
