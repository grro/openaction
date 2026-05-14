import logging
import socket
import asyncio
from abc import ABC
from time import sleep
from threading import Thread
from fastmcp import FastMCP
from typing import Dict
from zeroconf import IPVersion, ServiceInfo, Zeroconf



logger = logging.getLogger(__name__)




class MDNS:
    def __init__(self):
        self.registered: Dict[str, ServiceInfo] = dict()
        self.zc = Zeroconf(ip_version=IPVersion.V4Only)
        self.service_type = "_mcp._tcp.local."
        self.hostname = socket.gethostname()

        # Determine local IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            self.local_ip = s.getsockname()[0]
        except Exception:
            self.local_ip = "127.0.0.1"
        finally:
            s.close()

    def register_mdns(self, name: str, port: int):
        """Registers a service via mDNS."""
        try:
            service_name = f"{name}.{self.service_type}"
            service_info = ServiceInfo(
                type_=self.service_type,
                name=service_name,
                addresses=[socket.inet_aton(self.local_ip)],
                port=port,
                properties={
                    "version": "1.0",
                    "path": "/sse",
                    "server_type": "fastmcp"
                },
                server=f"{self.hostname}.local.",
            )

            logger.info(f"mDNS: Registering {service_name} at {self.local_ip}:{port}")
            self.zc.register_service(service_info)
            self.registered[name] = service_info
        except Exception as e:
            logger.error(f"mDNS Registration failed: {e}")

    def unregister_mdns(self, name: str):
        """Unregisters a specific service without closing the Zeroconf instance."""
        service_info = self.registered.pop(name, None)
        if service_info:
            logger.info(f"mDNS: Unregistering service {name}...")
            self.zc.unregister_service(service_info)

    def shutdown(self):
        """Completely shut down mDNS and close the socket."""
        self.zc.unregister_all_services()
        self.zc.close()





class McpServer(ABC):

    def __init__(self, name: str, port: int, host: str = "0.0.0.0"):
        self.name = name
        self.host = host
        self.port = port
        self.mdns = MDNS()
        self.mcp = FastMCP(self.name)
        self.loop = asyncio.new_event_loop()
        self._thread: Thread | None = None


    async def __run(self) -> None:
        logger.info(f"MCP Server '{self.name}' running on http://{self.host}:{self.port}/sse")
        await self.mcp.run_async(transport="sse", host=self.host, port=self.port)

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.__run())
        finally:
            self.loop.close()

    def start(self):
        """Starts the MCP server in a background thread (non-blocking)."""
        if self._thread and self._thread.is_alive():
            logger.warning(f"MCP Server '{self.name}' is already running.")
            return
        self.mdns.register_mdns(self.name, self.port)
        self._thread = Thread(target=self._run_loop, name=f"mcp-{self.name}", daemon=True)
        self._thread.start()

    def start_ant_wait(self):
        try:
            self.start()
            while True:
                sleep(5)
        except KeyboardInterrupt:
            logger.info('Stopping the server...')
            self.stop()

    def stop(self):
        self.mdns.unregister_mdns(self.name)
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logging.info("MCP Server stopped")

