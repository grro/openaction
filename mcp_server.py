import asyncio
import logging
import socket
from abc import ABC
from threading import Thread
from time import sleep
from typing import Optional

from mcp.server.fastmcp import FastMCP
from zeroconf import IPVersion, ServiceInfo, Zeroconf



class NoAccessLogsFilter(logging.Filter):
    """Filter out HTTP access logs from the logging output."""
    def filter(self, record):
        # Return False if the log message contains typical Uvicorn access patterns
        return "GET /sse" not in record.getMessage() and "POST /messages" not in record.getMessage()

# Apply the filter to the root logger's handlers
for handler in logging.root.handlers:
    handler.addFilter(NoAccessLogsFilter())


class MCPServer(ABC):

    def __init__(self, name: str, port: int):
        self.name = name
        self.port = port
        self.mcp = FastMCP(name, host='0.0.0.0', port=self.port)
        self.new_loop = asyncio.new_event_loop()

        self.zc: Optional[Zeroconf] = None
        self.service_info: Optional[ServiceInfo] = None

        # Silence internal MCP server logs
        logging.getLogger('mcp.server').setLevel(logging.WARNING)

        # Silence Uvicorn HTTP access logs (e.g., POST /messages 202 Accepted)
        access_logger = logging.getLogger("uvicorn.access")
        access_logger.setLevel(logging.WARNING)
        access_logger.propagate = False  # Prevent logs from bubbling up to root logger

        # Silence Uvicorn process startup/shutdown info logs
        error_logger = logging.getLogger("uvicorn.error")
        error_logger.setLevel(logging.WARNING)
        error_logger.propagate = False


    async def __run_async(self):
        await self.mcp.run_sse_async()

    def __start_loop(self, loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def _register_mdns(self):
        try:
            self.zc = Zeroconf(ip_version=IPVersion.V4Only)

            hostname = socket.gethostname()
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                s.connect(("8.8.8.8", 80))
                local_ip = s.getsockname()[0]
            finally:
                s.close()

            service_type = "_mcp._tcp.local."
            service_name = f"{self.name}.{service_type}"

            self.service_info = ServiceInfo(
                type_=service_type,
                name=service_name,
                addresses=[socket.inet_aton(local_ip)],
                port=self.port,
                properties={
                    "version": "1.0",
                    "path": "/sse",
                    "server_type": "fastmcp"
                },
                server=f"{hostname}.local.",
            )

            logging.info(f"mDNS: Registering {service_name} at {local_ip}:{self.port}")
            self.zc.register_service(self.service_info)
        except Exception as e:
            logging.error(f"mDNS Registration failed: {e}")

    def _unregister_mdns(self):
        if self.zc and self.service_info:
            logging.info("mDNS: Unregistering service...")
            self.zc.unregister_service(self.service_info)
            self.zc.close()

    def start(self):
        self._register_mdns()

        t = Thread(target=self.__start_loop, args=(self.new_loop,), daemon=True)
        t.start()
        asyncio.run_coroutine_threadsafe(self.__run_async(), self.new_loop)
        logging.info(f"MCP Server '{self.name}' running on http://0.0.0.0:{self.port}/sse")

    def start_and_wait(self):
        self.start()
        try:
            while True:
                sleep(1)
        except KeyboardInterrupt:
            logging.info("Shutdown signal received...")
        finally:
            self.stop()

    def stop(self):
        self._unregister_mdns()
        self.new_loop.stop()
        logging.info("MCP Server stopped")