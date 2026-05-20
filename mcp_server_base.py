import asyncio
import logging
import socket
from abc import ABC
from threading import Thread
from time import sleep
from typing import Dict, Optional

from fastmcp import FastMCP
from zeroconf import IPVersion, ServiceInfo, Zeroconf


logger = logging.getLogger(__name__)


# mDNS service type advertised by every OpenAction MCP server.
MCP_SERVICE_TYPE = "_mcp._tcp.local."

# Default SSE endpoint path published in the mDNS TXT record.
MCP_DEFAULT_PATH = "/sse"


class MDNS:
    """
    Thin wrapper around :mod:`zeroconf` for advertising one (or several)
    MCP servers on the local network.

    A single :class:`MDNS` instance owns one :class:`Zeroconf` socket and
    can register an arbitrary number of services on it. Use
    :meth:`unregister_mdns` to remove a single service or :meth:`shutdown`
    to tear down all advertisements and close the socket.
    """

    def __init__(self) -> None:
        self.registered: Dict[str, ServiceInfo] = {}
        self.zc = Zeroconf(ip_version=IPVersion.V4Only)
        self.service_type = MCP_SERVICE_TYPE
        self.hostname = socket.gethostname()
        self.local_ip = self._detect_local_ip()

    @staticmethod
    def _detect_local_ip() -> str:
        """
        Best-effort detection of the host's primary IPv4 address.

        Uses a connected UDP socket to a public address (no packet is
        actually sent). Falls back to loopback if the host has no
        external route.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"
        finally:
            s.close()

    def register_mdns(self, name: str, port: int) -> None:
        """
        Advertise an MCP service ``name`` on the given ``port`` via mDNS.

        Safe to call multiple times with different names; calling it
        again with the same name silently replaces the previous record.
        """
        try:
            service_name = f"{name}.{self.service_type}"
            service_info = ServiceInfo(
                type_=self.service_type,
                name=service_name,
                addresses=[socket.inet_aton(self.local_ip)],
                port=port,
                properties={
                    "version": "1.0",
                    "path": MCP_DEFAULT_PATH,
                    "server_type": "fastmcp",
                },
                server=f"{self.hostname}.local.",
            )

            logger.info(
                f"mDNS: Registering {service_name} at {self.local_ip}:{port}"
            )
            # Replace any previous registration under the same name.
            self.unregister_mdns(name)
            self.zc.register_service(service_info)
            self.registered[name] = service_info
        except Exception as e:
            logger.error(f"mDNS registration of '{name}' failed: {e}", exc_info=True)

    def unregister_mdns(self, name: str) -> None:
        """Stop advertising a single service. Silently ignores unknown names."""
        service_info = self.registered.pop(name, None)
        if service_info is None:
            return
        try:
            logger.info(f"mDNS: Unregistering service {name}...")
            self.zc.unregister_service(service_info)
        except Exception as e:
            logger.warning(f"mDNS unregistration of '{name}' failed: {e}")

    def shutdown(self) -> None:
        """Unregister every service and close the underlying Zeroconf socket."""
        try:
            self.zc.unregister_all_services()
        except Exception as e:
            logger.warning(f"mDNS unregister_all_services failed: {e}")
        finally:
            self.registered.clear()
            try:
                self.zc.close()
            except Exception as e:
                logger.warning(f"mDNS close failed: {e}")


class McpServer(ABC):
    """
    Abstract base class for FastMCP-backed servers in OpenAction.

    A subclass typically registers its tools/resources on ``self.mcp``
    inside its own ``__init__``. Lifecycle:

      * :meth:`start`          — non-blocking; spawns a daemon thread
                                 running the SSE server and registers
                                 the service via mDNS.
      * :meth:`start_and_wait` — convenience wrapper that blocks the
                                 calling thread until Ctrl-C.
      * :meth:`stop`           — graceful shutdown (idempotent).
    """

    def __init__(self, name: str, port: int, host: str = "0.0.0.0"):
        self.name = name
        self.host = host
        self.port = port
        self.mdns = MDNS()
        self.mcp = FastMCP(self.name)
        self.loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self._thread: Optional[Thread] = None

    # ------------------------------------------------------------------ run

    async def _serve_async(self) -> None:
        """Coroutine running the actual SSE transport (runs on ``self.loop``)."""
        logger.info(
            f"MCP Server '{self.name}' running on "
            f"http://{self.host}:{self.port}{MCP_DEFAULT_PATH}"
        )
        await self.mcp.run_async(transport="sse", host=self.host, port=self.port)

    def _run_loop(self) -> None:
        """Thread entry point: install the loop and drive it until stopped."""
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._serve_async())
        except Exception as e:
            logger.error(f"MCP Server '{self.name}' crashed: {e}", exc_info=True)
        finally:
            try:
                self.loop.close()
            except Exception:
                pass

    # ----------------------------------------------------------------- lifecycle

    def start(self) -> None:
        """Start the MCP server in a background daemon thread (non-blocking)."""
        if self._thread and self._thread.is_alive():
            logger.warning(f"MCP Server '{self.name}' is already running.")
            return
        self.mdns.register_mdns(self.name, self.port)
        self._thread = Thread(
            target=self._run_loop, name=f"mcp-{self.name}", daemon=True
        )
        self._thread.start()

    def start_and_wait(self) -> None:
        """Start the server and block the calling thread until Ctrl-C."""
        try:
            self.start()
            while self._thread and self._thread.is_alive():
                sleep(5)
        except KeyboardInterrupt:
            logger.info("Stopping the server...")
            self.stop()

    # Backwards-compatible alias for the old typo.
    start_ant_wait = start_and_wait

    def stop(self) -> None:
        """
        Stop the server, tear down mDNS advertisements and join the worker
        thread. Safe to call multiple times.
        """

        self.mdns.unregister_mdns(self.name)
        self.mdns.shutdown()

        # Schedule loop.stop() on the loop's own thread. Checking
        # is_running() from the outside is inherently racy, so we just
        # try and swallow the (rare) "loop not running" RuntimeError.
        try:
            self.loop.call_soon_threadsafe(self.loop.stop)
        except RuntimeError:
            pass

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logger.info(f"MCP Server '{self.name}' stopped")
