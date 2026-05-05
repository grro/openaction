import asyncio
import logging
import threading
from contextlib import AsyncExitStack
from threading import Thread
from time import sleep
from typing import Dict, Optional, Callable, Any

from fastmcp import Client

from api.mcp_adapter import MCPAdapter
from adapter_impl import Registry
from services import MCP_SSE, ServiceRegistry


logger = logging.getLogger(__name__)



class SyncMCPClient(MCPAdapter):
    """Synchronous MCP client communicating over HTTP/SSE transport using FastMCP."""

    def __init__(self, url: str):
        """
        Args:
            url: The HTTP SSE endpoint of the MCP server, e.g. 'http://host:port/sse'.
        """
        self.url = url
        self._client = None
        self._exit_stack = None

        self._notification_callbacks: Dict[str, set[Callable[[Any], None]]] = dict()

        # Start the background thread exactly ONCE for the life of this client object
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

        # FastMCP abstracts away sse_transport and ClientSession.
        # We just instantiate the Client and enter its async context manager.
        self._client = Client(self.url, message_handler=self._internal_message_handler)
        await self._exit_stack.enter_async_context(self._client)

    def _ensure_connected(self):
        """Ensures the connection is active. Called automatically before any API method."""
        if not self._client:
            try:
                self._run_sync(self._connect_async())
            except Exception:
                self._disconnect() # Clean up failed connection attempt
                raise

    def _disconnect(self):
        """Closes the SSE session but KEEPS the background thread alive for reconnects."""
        if self._exit_stack:
            try:
                self._run_sync(self._exit_stack.aclose())
            except Exception as e:
                logger.debug(f"Error while closing async exit stack: {e}")
        self._exit_stack = None
        self._client = None

    def close(self):
        """Fully shuts down the client and kills its background thread. Cannot be reused after this."""
        self._disconnect()
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread.is_alive():
            self._thread.join()


    def add_notification_listener(self, uri: str, callback: Callable[[Any], None]):
        """Registers a callback function to consume incoming MCP notifications."""
        callbacks = self._notification_callbacks.get(uri, set())
        callbacks.add(callback)
        self._notification_callbacks[uri] = callbacks


    def remove_notification_listener(self, uri: str, callback: Callable[[Any], None]):
        """Removes a previously registered notification callback."""
        callbacks = self._notification_callbacks.get(uri, set())
        callbacks.remove(callback)
        self._notification_callbacks[uri] = callbacks



    async def _internal_message_handler(self, message: Any):
        """
        Internal async handler passed to FastMCP. Routes incoming messages
        to all registered callbacks safely.
        """

        for name, callbacks in self._notification_callbacks.items():
            try:
                msg = str(message)
                if name in msg:
                    # Run sync callbacks in an executor to prevent blocking the SSE event loop
                    for callback in callbacks:
                        self._loop.run_in_executor(None, callback)
            except Exception as e:
                logger.error(f"Error executing notification callback: {e}", exc_info=True)


    # ==========================================
    # Public synchronous API
    # ==========================================

    def list_resources(self):
        self._ensure_connected()
        try:
            return self._run_sync(self._client.list_resources())
        except Exception:
            self._disconnect()
            raise

    def read_resource(self, uri: str):
        self._ensure_connected()
        try:
            return self._run_sync(self._client.read_resource(uri))
        except Exception:
            self._disconnect()
            raise

    def subscribe_resource(self, uri: str, callback):
        self._ensure_connected()
        try:
            self.add_notification_listener(uri, callback)
            return self._run_sync(self._client.session.subscribe_resource(uri))
        except Exception as e:
            # Gracefully handle FastMCP servers that don't support explicit subscriptions
            if "Method not found" in str(e):
                logger.debug(f"Server does not support explicit subscriptions for {uri}. "
                             f"Assuming auto-subscription on read.")
                return None
            else:
                self._disconnect()
                raise

    def list_tools(self):
        self._ensure_connected()
        try:
            return self._run_sync(self._client.list_tools())
        except Exception:
            self._disconnect()
            raise

    def call_tool(self, name: str, arguments: dict = None):
        self._ensure_connected()
        try:
            return self._run_sync(self._client.call_tool(name, arguments or {}))
        except Exception:
            self._disconnect()
            raise

    def __repr__(self) -> str:
        return f"SyncMCPClient(url='{self.url}')"







class McpRegistry(Registry):

    NAME = 'mcp_adapter'

    def __init__(self, service_registry: ServiceRegistry):
        self.is_running = True
        self._mcp: Dict[str, SyncMCPClient] = {}
        self._service_registry = service_registry
        self._service_registry.add_listener(self._refresh)
        self._refresh()

    def start(self):
        self.is_running = True
        Thread(target=self._loop, daemon=True).start()
        return self

    def close(self):
        self.is_running = False
        self._service_registry.remove_listener(self._refresh)
        for client in self._mcp.values():
            client.close()
        self._mcp.clear()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def get_adapter(self, name: Optional[str] = None) -> Optional[Any]:
        """
        Retrieves a specific MCP client by name.

        Note: This specific registry currently does not define a 'default'
        adapter if name is None.
        """
        if name is None:
            logger.warning("McpRegistry: No name provided, and no default MCP client configured.")
            return None

        return self._mcp.get(name)

    def _loop(self):
        while self.is_running:
            try:
                self._refresh()
            except Exception as e:
                logger.warning(f"Error in MCP registry loop: {e}")
            sleep(60)

    def _refresh(self):
        current_services = dict(self._service_registry.registered_services)

        # 1. Add new services
        for name, conf in current_services.items():
            if conf.type == MCP_SSE and name not in self._mcp:
                try:
                    self._mcp[name] = SyncMCPClient(conf.url)
                    logger.debug(f"{'Auto-scanned' if conf.auto_scanned else 'Manually configured'} MCP server '{name}' added")
                except Exception as e:
                    logger.warning(f"Error adding MCP server '{name}': {e}")

        # 2. Remove obsolete services to prevent Thread/Memory leaks
        for name in list(self._mcp.keys()):
            if name not in current_services:
                client = self._mcp.pop(name)
                client.close() # Kills the background thread gracefully
                logger.debug(f"MCP server '{name}' removed and thread stopped.")