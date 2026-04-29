import asyncio
import logging
import threading
from contextlib import AsyncExitStack
from typing import Dict, Optional
from mcp import ClientSession
from mcp.client.sse import sse_client


from api.mcp_service import MCPClientRegistry, MCPClient
from services import MCP_SSE, ServiceRegistry

logger = logging.getLogger(__name__)


class SyncMCPClient(MCPClient):
    """Synchronous MCP client communicating over HTTP/SSE transport."""

    def __init__(self, url: str):
        """
        Args:
            url: The HTTP SSE endpoint of the MCP server, e.g. 'http://host:port/sse'.
        """
        self.url = url
        self._session = None
        self._exit_stack = None
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._start_loop, daemon=True)
        self._thread.start()

    def _start_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _run_sync(self, coroutine):
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result()

    async def _connect_async(self):
        self._exit_stack = AsyncExitStack()

        # Open HTTP/SSE transport to the MCP server
        sse_transport = await self._exit_stack.enter_async_context(sse_client(self.url))
        read, write = sse_transport

        # Wrap transport in a ClientSession and perform MCP handshake
        self._session = await self._exit_stack.enter_async_context(ClientSession(read, write))
        await self._session.initialize()

    def __connect(self):
        self._run_sync(self._connect_async())


    def __close(self):
        if self._exit_stack:
            self._run_sync(self._exit_stack.aclose())
        self._loop.call_soon_threadsafe(lambda: self._loop.stop())
        self._thread.join()


    # ==========================================
    # Public synchronous API
    # ==========================================

    def list_tools(self):
        if not self._session:
            self.__connect()
        try:
            return self._run_sync(self._session.list_tools())
        except Exception as e:
            self.__close()
            raise e


    def call_tool(self, name: str, arguments: dict = None):
        if not self._session:
            self.__connect()
        try:
            return self._run_sync(self._session.call_tool(name, arguments or {}))
        except Exception as e:
            self.__close()
            raise e

    def __repr__(self) -> str:
        return f"SyncMCPClient(url='{self.url}')"


class McpRegistry(MCPClientRegistry):

    def __init__(self, service_registry: ServiceRegistry):
        self._mcp: Dict[str, SyncMCPClient] = {}
        self._service_registry = service_registry
        self._service_registry.add_listener(self._refresh)
        self._refresh()

    def __del__(self):
        self._service_registry.remove_listener(self._refresh)

    def get(self, name: str) -> Optional[MCPClient]:
        """Return the MCP client for the given name, or None."""
        return self._mcp.get(name)

    def clone(self):
        return McpRegistry(self._service_registry)

    def _refresh(self):
        for name, conf in dict(self._service_registry.registered_services).items():
            if conf.type == MCP_SSE:
                if name not in self._mcp.keys():
                    try:
                        self._mcp[name] = SyncMCPClient(conf.url)
                        logger.debug(f"{'auto scanned' if conf.auto_scanned else 'manually'} configured MCP server '{name}' added")
                    except Exception as e:
                        logger.warning(f"Error adding MCP server '{name}': {e}")