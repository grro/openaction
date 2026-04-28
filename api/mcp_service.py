from abc import ABC, abstractmethod
from typing import Any, Optional

from mcp import ListToolsResult
from mcp.types import CallToolResult


class MCPClient(ABC):
    """
    A simple wrapper for the MCP (Model Context Protocol) client.
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
    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> CallToolResult:
        """
        Executes a specific tool on the connected MCP server.

        Args:
            name (str): The unique identifier/name of the tool to be executed.
            arguments (dict[str, Any] | None, optional): A dictionary of arguments
                required by the tool. Defaults to None.

        Returns:
            CallToolResult: The execution result returned by the MCP server.
        """
        pass


class MCPClientRegistry(ABC):
    """
    A service interface for managing and retrieving MCP clients.
    """

    @abstractmethod
    def get(self, name: str) -> Optional[MCPClient]:
        """
        Retrieves an MCPClient instance by its assigned name.

        Args:
            name (str): The unique name/identifier of the desired MCP client.

        Returns:
            MCPClient: The client instance associated with the given name.
        """
        pass