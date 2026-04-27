import asyncio
import logging
import threading
import time
from contextlib import AsyncExitStack
from threading import Thread
from typing import Dict
from urllib.parse import urlparse, urlunparse
from mcp import ClientSession
from mcp.client.sse import sse_client
from zeroconf import ServiceBrowser, ServiceListener, Zeroconf


from api.mcp_service import MCPClientRegistry, MCPClient





def _is_valid_mcp_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)


def _replace_url_scheme(url: str, scheme: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or "/sse"
    return urlunparse((scheme, parsed.netloc, path, parsed.params, parsed.query, parsed.fragment))


def _build_discovered_url(host: str, port: int, path: str) -> str:
    safe_host = host
    if ":" in host and not host.startswith("["):
        safe_host = f"[{host}]"
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"http://{safe_host}:{port}{normalized_path}"



class SyncMCPClient(MCPClient):
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



def scan_mcp_servers(timeout_seconds: float = 1.5) -> Dict[str, str]:
    """Scans the local network for MCP servers via mDNS."""
    service_type = "_mcp._tcp.local."
    discovered: Dict[str, str] = {}

    class MCPListener(ServiceListener):
        def add_service(self, zc: Zeroconf, type_: str, name: str) -> None:
            self._process(zc, type_, name)

        def update_service(self, zc: Zeroconf, type_: str, name: str) -> None:
            self._process(zc, type_, name)

        def remove_service(self, zc: Zeroconf, type_: str, name: str) -> None:
            pass # We don't need to handle removal for a quick static scan

        def _process(self, zc: Zeroconf, type_: str, name: str) -> None:
            info = zc.get_service_info(type_, name)
            if not info or not info.parsed_addresses():
                return
            if info.port is None:
                return

            host = info.parsed_addresses()[0]

            # Extract path, default to "/sse", and ensure it starts with "/"
            path_raw = info.properties.get(b"path", b"/sse").decode("utf-8", errors="ignore")
            path = path_raw if path_raw.startswith("/") else f"/{path_raw}"

            # Clean up the service name (e.g. "MyServer._mcp._tcp.local." -> "MyServer")
            service_name = name.replace(f".{type_}", "").strip()
            url = _build_discovered_url(host, info.port, path)
            if _is_valid_mcp_url(url):
                discovered[service_name] = url
            else:
                logging.warning(f"Skipping discovered MCP service with invalid URL: {service_name} -> {url}")

    zc: Zeroconf | None = None
    try:
        zc = Zeroconf()
        if zc is None:
            return {}
        browser = ServiceBrowser(zc, service_type, MCPListener())
        time.sleep(timeout_seconds)
        browser.cancel() # Stop browsing before closing
        return discovered

    except Exception as e:
        logging.warning(f"mDNS scan failed, continuing without autodiscovery: {e}")
        return {}
    finally:
        if zc is not None:
            zc.close()



class McpRegistry(MCPClientRegistry):


    def __init__(self, services: Dict[str, str], autoscan: bool):
        self.is_running = True
        self.__mcp: Dict[str, SyncMCPClient] = {}
        self.services = services
        self.autoscan = autoscan
        logging.info(f"McpRegistry initialized: autoscan={'ON' if autoscan else 'OFF'}, {len(services)} manually configured server(s): {', '.join(services.keys()) if services else 'none'}")
        Thread(target=self.__loop, daemon=True).start()

    def _create_client_with_fallback(self, name: str, url: str) -> SyncMCPClient | None:
        if not _is_valid_mcp_url(url):
            logging.warning(f"Skipping MCP server '{name}' due to invalid URL: {url}")
            return None

        candidate_urls = [url]
        if urlparse(url).scheme == "http":
            candidate_urls.append(_replace_url_scheme(url, "https"))

        for candidate_url in candidate_urls:
            try:
                client = SyncMCPClient(candidate_url)
                client.list_tools()  # Validate endpoint early so bad URLs do not stay in the registry.
                if candidate_url != url:
                    logging.info(f"MCP server '{name}' switched to HTTPS: {candidate_url}")
                return client
            except Exception as e:
                logging.warning(f"Failed to connect MCP server '{name}' at {candidate_url}: {e}")

        return None

    def __loop(self):

        while self.is_running:
            # 1. Process manually configured services
            # list() prevents runtime errors if self.services is modified from another thread
            for name, url in list(self.services.items()):
                if name not in self.__mcp:
                    try:
                        client = self._create_client_with_fallback(name, url)
                        if client is not None:
                            self.__mcp[name] = client
                            logging.info(f"Manually configured MCP server '{name}' added")
                    except Exception as e:
                        # Corrected: Was previously incorrectly logged as "autoscan"
                        logging.warning(f"Error adding manual MCP server '{name}': {e}")

            # 2. Process autoscan services
            if self.autoscan:
                try:
                    scanned_servers = scan_mcp_servers()
                    for name, url in scanned_servers.items():
                        if name in {"OpenAction"}:
                            continue
                        if url in self.__mcp.values():
                            continue

                        if name not in self.__mcp:
                            try:
                                client = self._create_client_with_fallback(name, url)
                                if client is not None:
                                    self.__mcp[name] = client
                                    logging.info(f"Autoscanned MCP server '{name}' added")
                            except Exception as e:
                                logging.warning(f"Error adding autoscanned MCP server '{name}': {e}")
                except Exception as e:
                    # Catches errors if scan_mcp_servers() itself fails
                    logging.warning(f"Error executing MCP autoscan function: {e}")

            # Wait 60 seconds before the next run
            time.sleep(60)

    def get(self, name: str) -> MCPClient:
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


