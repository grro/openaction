import asyncio
import logging
import threading
from contextlib import AsyncExitStack
from threading import Thread
from time import sleep
from typing import Dict, Optional, Callable, Any, Set
from fastmcp import Client

from api.mcp_adapter import MCPAdapter
from adapter_impl import Registry
from mdns import MDNSScanner
from services import ServiceRegistry, Scanner, ServiceConfig

logger = logging.getLogger(__name__)



class SyncMCPClient(WebhingAdapter):

    def __init__(self, url: str):
        self.url = url





class WebThingServiceScanner(Scanner):

    def scan(self) -> Set[ServiceConfig]:
        return {ServiceConfig(discovered.name, 'WEBTHING', discovered.url, True) for discovered in MDNSScanner().scan("_webthing._tcp.local.")}
