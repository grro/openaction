import copy
import gzip
import json
import logging
import os
import threading
from datetime import datetime, timedelta
from pathlib import Path
from random import randint
from typing import Any
from platformdirs import site_data_dir

from api.store import Store


logger = logging.getLogger(__name__)


class Entry:
    """A single key-value entry with an optional expiration date."""

    def __init__(self, value: Any, expire_date: datetime):
        self.value = value
        self.expire_date = expire_date

    def is_expired(self) -> bool:
        return datetime.now() > self.expire_date

    def to_dict(self) -> dict[str, Any]:
        return {"value": self.value, "expire_date": self.expire_date.isoformat()}

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Entry":
        return Entry(data["value"], datetime.fromisoformat(data["expire_date"]))

    def __str__(self) -> str:
        return f"{self.value} (ttl={self.expire_date.strftime('%d.%m %H:%M')})"

    __repr__ = __str__


class SimpleStore(Store):
    """
    A lightweight, file-backed key-value store.

    Data is kept in memory and persisted to a gzipped JSON file. Entries support
    an optional time-to-live (TTL); expired entries are filtered on read and
    removed during the next disk sync.

    Writes are flushed immediately by default. If ``sync_period_sec`` is set,
    the store batches writes and only flushes when that interval has elapsed
    since the previous flush.
    """

    def __init__(self, name: str, sync_period_sec: int | None = None, directory: str | None = None):
        self._name = name
        self.sync_period_sec = sync_period_sec
        self._directory = Path(directory) if directory else Path(site_data_dir("simpledb", appauthor=False))
        self._directory.mkdir(parents=True, exist_ok=True)

        self._data: dict[str, Entry] = self._load()
        # Force the first write to happen on the next put/delete
        self._last_time_stored = datetime.now() - timedelta(days=2)
        # Guards _store() so concurrent writers don't race on the same file
        self._write_lock = threading.Lock()
        logger.info(f"simple db: using {self.filename} ({len(self._data)} entries)")

    # ---- Public API --------------------------------------------------------

    @property
    def filename(self) -> Path:
        """Path to the on-disk gzipped JSON file."""
        self._directory.mkdir(parents=True, exist_ok=True)
        return self._directory / f"{self._name}.json.gz"

    def __len__(self) -> int:
        return len(self._data)

    def keys(self) -> list[str]:
        """Return all non-expired keys."""
        return [k for k, e in self._data.items() if not e.is_expired()]

    def values(self) -> list[Any]:
        """Return deep copies of all non-expired values."""
        return [copy.deepcopy(e.value) for e in self._data.values() if not e.is_expired()]

    def has(self, key: str) -> bool:
        entry = self._data.get(key)
        return entry is not None and not entry.is_expired()

    def get(self, key: str, default_value: Any = None) -> Any:
        """Return a deep copy of the value, or ``default_value`` if missing/expired."""
        entry = self._data.get(key)
        if entry is None or entry.is_expired():
            return default_value
        return copy.deepcopy(entry.value)

    def put(self, key: str, value: Any, ttl_sec: int | None = None) -> None:
        """Insert or update a value. If ``ttl_sec`` is None the entry never expires."""
        expire_date = datetime.max if ttl_sec is None else datetime.now() + timedelta(seconds=ttl_sec)

        # Skip the write if nothing actually changed
        existing = self._data.get(key)
        if existing and existing.value == value and existing.expire_date == expire_date:
            return

        self._data[key] = Entry(value, expire_date)
        self._maybe_sync()

    def delete(self, key: str) -> None:
        if self._data.pop(key, None) is not None:
            self._maybe_sync()

    def clear(self) -> None:
        self._data.clear()
        self._store()

    def get_values(self) -> list[Any]:
        """Deprecated: use :meth:`values` instead."""
        logger.warning("Store#get_values is deprecated. Use Store#values instead")
        return self.values()

    # ---- Internals ---------------------------------------------------------

    def _maybe_sync(self) -> None:
        """Flush to disk immediately, or honor ``sync_period_sec`` if set."""
        if self.sync_period_sec is None or \
                datetime.now() >= self._last_time_stored + timedelta(seconds=self.sync_period_sec):
            self._store()
            self._last_time_stored = datetime.now()

    def _remove_expired(self) -> None:
        for key in [k for k, e in self._data.items() if e.is_expired()]:
            del self._data[key]

    def _load(self) -> dict[str, Entry]:
        if not self.filename.is_file():
            return {}
        try:
            with gzip.open(self.filename, "rt", encoding="UTF-8") as f:
                data = json.load(f)
            return {name: Entry.from_dict(raw) for name, raw in data.items()}
        except Exception as error:
            logger.warning(f"Could not load {self.filename}: {error}")
            return {}

    def _store(self) -> None:
        """Atomically write the current state to disk."""
        # Serialize writers so that two threads don't try to replace the same file at once.
        # On Windows this is the typical cause of "WinError 32: file is being used by another process".
        with self._write_lock:
            try:
                self._remove_expired()
            except Exception as error:
                logger.error(f"Error removing expired records: {error}")

            # Write to a temp file first, then atomically move it into place.
            # os.replace() is atomic on POSIX and Windows and allows overwriting an existing target.
            tempname = self.filename.with_suffix(f".{randint(0, 10000)}.temp")
            try:
                data = {name: entry.to_dict() for name, entry in self._data.items()}
                with gzip.open(tempname, "wt", encoding="UTF-8") as f:
                    json.dump(data, f, indent=2)
                os.replace(tempname, self.filename)
            except Exception as error:
                logger.error(f"Failed to store data to {self.filename}: {error}")
            finally:
                if tempname.exists():
                    try:
                        tempname.unlink()
                    except OSError:
                        pass


class ScopedStore(Store):
    """
    A view of a :class:`Store` that transparently prefixes every key with a scope.

    Useful for sharing a single underlying store across multiple components
    without risking key collisions.

    Example::

        store = SimpleStore("shared")
        task1 = ScopedStore(store, "task1")
        task2 = ScopedStore(store, "task2")

        task1.put("status", "running")  # stored as "task1:status"
        task2.put("status", "idle")     # stored as "task2:status"
    """

    def __init__(self, store: SimpleStore, scope: str, separator: str = ":"):
        self._store = store
        self._scope = scope
        self._separator = separator
        self._prefix = f"{scope}{separator}"

    @property
    def scope(self) -> str:
        return self._scope

    def _scoped(self, key: str) -> str:
        return self._prefix + key

    def put(self, key: str, value: Any, ttl_sec: int | None = None) -> None:
        self._store.put(self._scoped(key), value, ttl_sec)

    def get(self, key: str, default_value: Any = None) -> Any:
        return self._store.get(self._scoped(key), default_value)

    def delete(self, key: str) -> None:
        self._store.delete(self._scoped(key))

    def keys(self) -> list[str]:
        """Return all keys in this scope with the scope prefix stripped."""
        return [k[len(self._prefix):] for k in self._store.keys() if k.startswith(self._prefix)]
