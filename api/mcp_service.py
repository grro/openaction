from abc import ABC, abstractmethod

from mcp import ListToolsResult
from mcp.types import CallToolResult


class MCPService(ABC):
    """
    A simple  wrapper for the MCP (Model Context Protocol) client.
    It provides a standardized interface to interact with connected MCP servers.
    """

    @abstractmethod
    def list_tools(self) -> ListToolsResult:
        """
        Retrieves a list of all available tools provided by the connected MCP server.

        Returns:
            ListToolsResult: An object containing the available tools and their schemas.
        """
        pass


    @abstractmethod
    def call_tool(self, name: str, arguments: dict = None) -> CallToolResult:
        """
        Executes a specific tool on the connected MCP server.

        Args:
            name (str): The unique identifier/name of the tool to be executed.
            arguments (dict, optional): A dictionary of arguments required by the tool. Defaults to None.

        Returns:
            CallToolResult: The execution result returned by the MCP server.
        """
        pass