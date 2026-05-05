from abc import ABC, abstractmethod
from typing import Any, Optional

from mcp import ListToolsResult
from mcp.types import CallToolResult


class MCPAdapter(ABC):
    """
    Interface for Model Context Protocol (MCP) clients.

    This wrapper encapsulates communication with connected MCP servers. Connection
    lifecycle management is handled internally.

    Note: Client instances are typically dedicated to specific tasks and are not
    intended to be shared across concurrent executions.
    """

    @abstractmethod
    def list_resources(self):
        """
        Retrieves a list of available resources exposed by the connected MCP server.

        Returns:
            ListResourcesResult: An object containing the available resources, including
                                 their URIs, names, and descriptions.
        """
        pass

    @abstractmethod
    def read_resource(self, uri: str):
        """
        Retrieves a list of available resources exposed by the connected MCP server.

        Returns:
            ListResourcesResult: An object containing the available resources, including
                                 their URIs, names, and descriptions.
        """
        pass

    @abstractmethod
    def list_tools(self) -> ListToolsResult:
        """
        Retrieves the manifest of available tools from the connected MCP server.

        Returns:
            ListToolsResult: An object containing the tool definitions and their
                             respective JSON Schema specifications.
        """
        pass

    @abstractmethod
    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> CallToolResult:
        """
        Invokes a functional tool on the remote MCP server.

        Args:
            name (str): The unique identifier of the tool to execute.
            arguments (dict[str, Any] | None, optional): Parameters for the tool,
                matching its defined JSON Schema. Defaults to None.

        Returns:
            CallToolResult: The structured response from the server, including
                            content (text/images) and success status.
        """
        pass

