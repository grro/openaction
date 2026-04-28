import copy
import gzip
import json
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from random import randint
from typing import Any
from appdirs import site_data_dir
from api.store_service import StoreService


class Entry:
    def __init__(self, value: Any, expire_date: datetime):
        self.expire_date = expire_date
        self.value = value

    def is_expired(self) -> bool:
        return datetime.now() > self.expire_date

    def to_dict(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "expire_date": self.expire_date.isoformat(),
        }

    def __str__(self) -> str:
        return f"{self.value} (ttl={self.expire_date.strftime('%d.%m %H:%M')})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Entry":
        return Entry(data["value"], datetime.fromisoformat(data["expire_date"]))


class Store(StoreService):
    def __init__(self, name: str, sync_period_sec: int | None = None, directory: str | None = None):
        self.sync_period_sec = sync_period_sec
        self._name = name

        if directory is None:
            self._directory = Path(site_data_dir("simpledb", appauthor=False))
        else:
            self._directory = Path(directory)

        self._data: dict[str, Entry] = self._load()
        self._last_time_stored = datetime.now() - timedelta(days=2)
        logging.info(f"simple db: using {self.filename} ({len(self._data)} entries)")

    @property
    def filename(self) -> Path:
        if not self._directory.exists():
            logging.info(f"Directory {self._directory} does not exist. Creating it.")
            self._directory.mkdir(parents=True, exist_ok=True)
        return self._directory / f"{self._name}.json.gz"

    def __len__(self) -> int:
        return len(self._data)

    def keys(self) -> list[str]:
        return [key for key, entry in self._data.items() if not entry.is_expired()]

    def has(self, key: str) -> bool:
        entry = self._data.get(key)
        return entry is not None and not entry.is_expired()

    def put(self, key: str, value: Any, ttl_sec: int | None = None) -> None:
        # compute expire date using datetime.max instead of a fake 2999 string
        if ttl_sec is None:
            expire_date = datetime.max
        else:
            expire_date = datetime.now() + timedelta(seconds=ttl_sec)

        # avoid unnecessary write
        entry = self._data.get(key)
        if entry and entry.value == value and entry.expire_date == expire_date:
            return

        # add and evaluate sync
        self._data[key] = Entry(value, expire_date)
        self._maybe_sync()

    def get(self, key: str, default_value: Any = None) -> Any:
        entry = self._data.get(key)
        if entry is None or entry.is_expired():
            return default_value
        return copy.deepcopy(entry.value)

    def get_values(self) -> list[Any]:
        logging.warning("Store#get_values is deprecated. Use Store#values instead")
        return self.values()

    def values(self) -> list[Any]:
        return [copy.deepcopy(entry.value) for entry in self._data.values() if not entry.is_expired()]

    def delete(self, key: str) -> None:
        if key in self._data:
            del self._data[key]
            self._maybe_sync()

    def clear(self) -> None:
        self._data.clear()
        self._store()

    def _maybe_sync(self) -> None:
        """Helper to evaluate if the store should sync to disk based on sync_period_sec."""
        if self.sync_period_sec is None or datetime.now() >= (
                self._last_time_stored + timedelta(seconds=self.sync_period_sec)
        ):
            self._store()
            self._last_time_stored = datetime.now()

    def _remove_expired(self) -> None:
        expired_keys = [key for key, entry in self._data.items() if entry.is_expired()]
        for key in expired_keys:
            del self._data[key]

    def _load(self) -> dict[str, Entry]:
        if self.filename.is_file():
            try:
                with gzip.open(self.filename, "rt", encoding="UTF-8") as file:
                    data = json.load(file)
                    return {name: Entry.from_dict(entry_data) for name, entry_data in data.items()}
            except Exception as error:
                logging.warning(f"Could not load {self.filename}: {error}")
        return {}

    def _store(self) -> None:
        try:
            self._remove_expired()
        except Exception as error:
            logging.error(f"Error occurred removing expired records: {error}")

        # Construct a temporary file path
        tempname = self.filename.with_suffix(f".{randint(0, 10000)}.temp")

        try:
            data = {name: entry.to_dict() for name, entry in self._data.items()}
            with gzip.open(tempname, "wt", encoding="UTF-8") as tempfile:
                json.dump(data, tempfile, indent=2)

            # Atomic move
            shutil.move(str(tempname), str(self.filename))
        except Exception as error:
            logging.error(f"Failed to store data to {self.filename}: {error}")
        finally:
            if tempname.exists():
                tempname.unlink()



class ScopedStore(StoreService):
    """
    A wrapper around a Store that automatically prefixes all keys with a scope identifier.
    This allows multiple task instances to share a single Store without key conflicts.

    Example:
        store = Store("shared")
        task1_store = ScopedStore(store, "task1")
        task2_store = ScopedStore(store, "task2")

        task1_store['status'] = 'running'  # Actually stores as 'task1:status'
        task2_store['status'] = 'idle'     # Actually stores as 'task2:status'
    """

    def __init__(self, store: Store, scope: str, separator: str = ":"):
        """
        Initialize a scoped store wrapper.

        Args:
            store: The underlying Store instance to wrap.
            scope: The scope/prefix to prepend to all keys.
            separator: The separator between scope and key (default: ":").
        """
        self._store = store
        self._scope = scope
        self._separator = separator

    def _scoped_key(self, key: str) -> str:
        """Generate a scoped key by prepending the scope prefix."""
        return f"{self._scope}{self._separator}{key}"

    @property
    def scope(self) -> str:
        """Return the scope identifier."""
        return self._scope

    def put(self, key: str, value: Any, ttl_sec: int | None = None) -> None:
        """Store a value with the scoped key."""
        self._store.put(self._scoped_key(key), value, ttl_sec)

    def get(self, key: str, default_value: Any = None) -> Any:
        """Retrieve a value by scoped key."""
        return self._store.get(self._scoped_key(key), default_value)

    def delete(self, key: str) -> None:
        """Delete a scoped key."""
        self._store.delete(self._scoped_key(key))

    def keys(self) -> list[str]:
        """Return all keys belonging to this scope, with the scope prefix stripped."""
        prefix = f"{self._scope}{self._separator}"
        return [key[len(prefix):] for key in self._store.keys() if key.startswith(prefix)]
