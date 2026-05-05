import logging
from abc import ABC, abstractmethod
from threading import Thread
from time import sleep
from datetime import datetime, timedelta
from typing import Set, Callable

from api.adapter import AdapterRegistry
from mcp_adapter_impl import McpRegistry, SyncMCPClient
from task import TaskAdapter


logger = logging.getLogger(__name__)


class Subscription(ABC):
    def __init__(self, service_name: str, uri: str, task: TaskAdapter):
        self.service_name = service_name
        self.uri = uri
        self.task = task
        self.last_notification = datetime.now() - timedelta(days=99)

    @property
    def is_stale(self) -> bool:
        return datetime.now() > (self.last_notification + timedelta(hours=1))

    @abstractmethod
    def subscribe(self, registry: AdapterRegistry):
        pass

    def _on_update(self):
        try:
            self.task.safe_run()
        except Exception as e:
            logger.warning(f"Error in subscription callback for {self.service_name}: {e}")



class McpSubscription(Subscription):
    def __init__(self, service_name: str, uri: str, task: TaskAdapter):
        super().__init__(service_name, uri, task)

    def subscribe(self, registry: AdapterRegistry):
        mcp_service = registry.get_adapter("mcp_adapter", self.service_name)

        if mcp_service is None:
            logger.warning(f"No MCP service found for subscription: {self.service_name}")
        else:
            try:
                mcp_service.read_resource(self.uri)
                mcp_service.subscribe_resource(self.uri, self._on_update)
                # Update timestamp so it's no longer stale
                self.last_notification = datetime.now()
                logger.debug(f"Successfully subscribed to {self.uri} on {self.service_name}")
            except Exception as e:
                logger.error(f"Failed to call subscribe_resource on {self.service_name}: {e}")



class SubscriptionService:
    def __init__(self, adapter_manager: AdapterRegistry):
        self._is_running = False
        self._adapter_manager = adapter_manager
        self._subscriptions: Set[Subscription] = set()

    def stop(self):
        self._is_running = False
        return self

    def start(self):
        if not self._is_running:
            self._is_running = True
            Thread(target=self._loop, daemon=True).start()
        return self

    def _loop(self):
        while self._is_running:
            try:
                # Copy the set for iteration to avoid "Set changed size during iteration" errors
                current_subscriptions = list(self._subscriptions)
                for subscription in current_subscriptions:
                    if subscription.is_stale:
                        logger.info(f"Subscription for {subscription.service_name} is stale. Retrying...")
                        subscription.subscribe(self._adapter_manager)
            except Exception as e:
                logger.warning(f"Error in SubscriptionService loop: {e}")

            sleep(60)

    def subscribe(self, service: str, prop: str, task: TaskAdapter):
        """Creates and registers a new subscription."""
        subscription = McpSubscription(service, prop, task)
        self._subscriptions.add(subscription)
        # Attempt initial subscription immediately
        subscription.subscribe(self._adapter_manager)