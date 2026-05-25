from dataclasses import dataclass
from datetime import datetime
from typing import List

from api.environment import Environment
from api.eventlog import EventLog
from api.store import Store
from simple_store import ScopedStore, SimpleStore




@dataclass(frozen=True, order=True)
class Event:
    timestamp: datetime
    topic: str
    text: str

    @staticmethod
    def from_str(row: str) -> 'Event':
        timestamp, topic, text = row.split('|')
        return Event(datetime.fromisoformat(timestamp), topic, text)

    def __str__(self) -> str:
        return f"{self.timestamp} {self.topic} | {self.text}"



class SimpleEventLog(EventLog):

    def __init__(self, store: SimpleStore, name: str) -> None:
        self._store = ScopedStore(store, name)
        self._log_store = ScopedStore(store, '__sys_eventlog_' + name)

    def log_event(self, topic: str, text: str, ttl: int = 24 * 60 * 60) -> None:
        now = datetime.now()
        self._log_store.put(now.isoformat(), str(Event(now, topic, text)), ttl_sec=ttl)

    def events(self) -> List[Event]:
        return sorted([Event.from_str(self._log_store.get(dt)) for dt in self._log_store.keys()], reverse=True)



class EnvironmentImpl(Environment):

    def __init__(self, store: SimpleStore,name: str) -> None:
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
