import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

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

            timestamp, topic, text = row.split('|')
            return Event(datetime.fromisoformat(timestamp.strip()), topic.strip(), text.strip())
        except Exception as e:
            logger.warning(f"Failed to parse event log row '{row}': {e}")
            return None

    def to_str(self) -> str:
        # Strict formatting for safe database storage (no spaces around |)
        return f"{self.timestamp.isoformat()}|{self.topic}|{self.text}"

    def __str__(self) -> str:
        # Beautiful formatting for printing/logging
        return f"{self.timestamp} | {self.topic} | {self.text}"


class SimpleEventLog(EventLog):

    def __init__(self, store: SimpleStore, name: str) -> None:
        self._store = ScopedStore(store, name)
        self._log_store = ScopedStore(store, '__sys_eventlog_' + name)

    def log_event(self, topic: str, text: str, ttl: int = 24 * 60 * 60) -> None:
        now = datetime.now()
        event = Event(now, topic, text)
        # Use .to_str() for storage, NOT str()
        self._log_store.put(now.isoformat(), event.to_str(), ttl_sec=ttl)

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
        self._store = store
        self._eventlog = SimpleEventLog(store, name)

    @property
    def store(self) -> Store:
        return self._store

    @property
    def eventlog(self) -> EventLog:
        return self._eventlog

    def events(self) -> List[Event]:
        return self._eventlog.events()