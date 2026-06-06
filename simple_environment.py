import logging
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import List, Optional, Dict

from api.environment import Environment
from api.eventlog import EventLog
from api.store import Store
from simple_store import ScopedStore, SimpleStore



logger = logging.getLogger(__name__)



@dataclass(frozen=True, order=True)
class Event:
    timestamp: datetime
    topic: str
    text: str

    @staticmethod
    def from_str(row: str) -> Optional['Event']:
        try:

            timestamp_str, topic, text = row.split('|')
            timestamp= datetime.strptime(timestamp_str.strip(), "%Y-%m-%dT%H:%M:%S%z")
            return Event(timestamp, topic.strip(), text.strip())
        except Exception as e:
            logger.warning(f"Failed to parse event log row '{row}': {e}")
            return None

    def to_str(self) -> str:
        # Strict formatting for safe database storage (no spaces around |)
        return f"{self.timestamp.strftime("%Y-%m-%dT%H:%M:%S%z")}|{self.topic}|{self.text}"

    def __str__(self) -> str:
        # Beautiful formatting for printing/logging
        return f"{self.timestamp} | {self.topic} | {self.text}"


class SimpleEventLog(EventLog):

    def __init__(self, store: SimpleStore, name: str) -> None:
        self._log_store = ScopedStore(store, '__sys_eventlog_' + name)
        self._revision = 0

    @property
    def revision(self) -> int:
        return self._revision

    def log_event(self, topic: str, text: str, ttl: int = 14 * 24 * 60 * 60) -> None:
        now = datetime.now(UTC)
        event = Event(now, topic, text)
        self._log_store.put(now.isoformat(), event.to_str(), ttl_sec=ttl)
        self._revision += 1

    def events(self) -> List[Event]:
        events_list: List[Event] = []
        for dt in self._log_store.keys():
            raw_data = self._log_store.get(dt)
            if raw_data is not None:
                event = Event.from_str(raw_data)
                if event:
                    events_list.append(event)

        return sorted(events_list, reverse=True)


class EnvironmentImpl(Environment):

    def __init__(self, store: SimpleStore, name: str) -> None:
        self._scoped_store = ScopedStore(store, name)
        self._eventlog = SimpleEventLog(store, name)

    @property
    def revision(self) -> int:
        return self._eventlog.revision

    @property
    def store(self) -> Store:
        return self._scoped_store

    @property
    def eventlog(self) -> EventLog:
        return self._eventlog

    def events(self) -> List[Event]:
        return self._eventlog.events()

    def store_items(self) -> Dict[str, str]:
        return {k: self._scoped_store.get(k) for k in self._scoped_store.keys()}