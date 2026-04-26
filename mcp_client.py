import asyncio
import threading
from contextlib import AsyncExitStack
from typing import Dict
from mcp import ClientSession
from mcp.client.sse import sse_client



class SyncMCPClient:
    """Synchronous MCP client communicating over HTTP/SSE transport."""

    def __init__(self, url: str):
        """
        Args:
            url: The HTTP SSE endpoint of the MCP server, e.g. 'http://host:port/sse'.
        """
        self.url = url

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._start_loop, daemon=True)
        self._thread.start()

        self._session = None
        self._exit_stack = None

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
        self._loop.call_soon_threadsafe(self._loop.stop)
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





class McpRegistry:

    def __init__(self, services: Dict[str, str]):
        self.__mcp: Dict[str, SyncMCPClient] = {name: SyncMCPClient(url) for name, url in services.items()}

    def get(self, name: str) -> SyncMCPClient | None:
        """Return the MCP client for the given name, or None."""
        return self.__mcp.get(name)

    def __getitem__(self, name: str) -> SyncMCPClient:
        return self.__mcp[name]

    def __contains__(self, name: str) -> bool:
        return name in self.__mcp

    def __len__(self) -> int:
        return len(self.__mcp)

    def __iter__(self):
        return iter(self.__mcp)

    def keys(self):
        return self.__mcp.keys()

    def values(self):
        return self.__mcp.values()

    def items(self):
        return self.__mcp.items()

    def __repr__(self) -> str:
        return f"McpRegistry({list(self.__mcp.keys())})"


