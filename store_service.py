import gzip
import json
import logging
import os
import shutil
from datetime import datetime, timedelta
from random import randint
from typing import Any

from appdirs import site_data_dir


class Entry:
    def __init__(self, value: Any, expire_date: datetime):
        self.expire_date = expire_date
        self.value = value

    def is_expired(self) -> bool:
        return datetime.now() > self.expire_date

    def to_dict(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "expire_date": self.expire_date.strftime("%Y.%m.%d %H:%M:%S"),
        }

    def __str__(self) -> str:
        return str(self.value) + " (ttl=" + self.expire_date.strftime("%d.%m %H:%M") + ")"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Entry":
        return Entry(data["value"], datetime.strptime(data["expire_date"], "%Y.%m.%d %H:%M:%S"))


class Store:
    def __init__(self, name: str, sync_period_sec: int | None = None, directory: str | None = None):
        self.sync_period_sec = sync_period_sec
        self.__name = name
        if directory is None:
            self.__directory = site_data_dir("simpledb", appauthor=False)
        else:
            self.__directory = directory
        self.__data = self.__load()
        self.__last_time_stored = datetime.now() - timedelta(days=2)
        logging.info("simple db: using " + self.filename + " (" + str(len(self.__data)) + " entries)")

    @property
    def filename(self) -> str:
        if not os.path.exists(self.__directory):
            logging.info("directory " + self.__directory + " does not exits. Creating it")
            os.makedirs(self.__directory)
        return os.path.join(self.__directory, self.__name + ".json.gz")

    def __len__(self) -> int:
        return len(self.__data)

    def keys(self) -> list[str]:
        keys = set()
        for key in list(self.__data.keys()):
            entry = self.__data[key]
            if not entry.is_expired():
                keys.add(key)
        return list(keys)

    def has(self, key: str) -> bool:
        return key in self.keys()

    def put(self, key: str, value: Any, ttl_sec: int | None = None) -> None:
        # compute expire date
        if ttl_sec is None:
            expire_date = datetime.strptime("2999-01-01", "%Y-%m-%d")
        else:
            expire_date = datetime.now() + timedelta(seconds=ttl_sec)

        # avoid unnecessary write
        entry = self.__data.get(key, None)
        if entry is not None:
            if entry.value == value and entry.expire_date == expire_date:
                return

        # add and store
        self.__data[key] = Entry(value, expire_date)
        if self.sync_period_sec is None or datetime.now() >= (
            self.__last_time_stored + timedelta(seconds=self.sync_period_sec)
        ):
            self.__store()
            self.__last_time_stored = datetime.now()

    def __copy(self, data: Any) -> Any:
        return json.loads(json.dumps(data))

    def get(self, key: str, default_value: Any = None) -> Any:
        entry = self.__data.get(key, None)
        if entry is None or entry.is_expired():
            return default_value
        return self.__copy(entry.value)

    def get_values(self) -> list[Any]:
        logging.warning("Store#get_values is deprecated. Use Store#values instead")
        return self.values()

    def values(self) -> list[Any]:
        values: list[Any] = []
        for key in list(self.__data.keys()):
            entry = self.__data[key]
            if not entry.is_expired():
                values.append(self.__copy(entry.value))
        return values

    def delete(self, key: str) -> None:
        if key in self.__data.keys():
            del self.__data[key]
            if self.sync_period_sec is None or datetime.now() >= (
                self.__last_time_stored + timedelta(seconds=self.sync_period_sec)
            ):
                self.__store()
                self.__last_time_stored = datetime.now()

    def clear(self) -> None:
        self.__data = {}
        self.__store()

    def __remove_expired(self) -> None:
        for key in list(self.__data.keys()):
            entry = self.__data[key]
            if entry.is_expired():
                del self.__data[key]

    def __load(self) -> dict[str, Entry]:
        if os.path.isfile(self.filename):
            with gzip.open(self.filename, "rb") as file:
                try:
                    json_data = file.read()
                    data = json.loads(json_data.decode("UTF-8"))
                    return {name: Entry.from_dict(data[name]) for name in data.keys()}
                except Exception as error:
                    logging.warning("could not load " + self.filename + " " + str(error))
        return {}

    def __store(self) -> None:
        try:
            self.__remove_expired()
        except Exception as error:
            logging.info("error occurred removing expired records " + str(error))

        tempname = self.filename + "." + str(randint(0, 10000)) + ".temp"
        try:
            data = {name: self.__data[name].to_dict() for name in self.__data.keys()}
            with gzip.open(tempname, "wb") as tempfile:
                tempfile.write(json.dumps(data, indent=2).encode("UTF-8"))
            shutil.move(tempname, self.filename)
        finally:
            os.remove(tempname) if os.path.exists(tempname) else None

    def __str__(self) -> str:
        return "\n".join(
            [
                str(name)
                + ": "
                + str(self.__data[name].value)
                + " (ttl="
                + self.__data[name].expire_date.strftime("%d.%m %H:%M")
                + ")"
                for name in self.__data.keys()
            ]
        )

    def __repr__(self) -> str:
        return self.__str__()

    def __getitem__(self, key: str) -> Any:
        entry = self.__data.get(key)
        if entry is None or entry.is_expired():
            raise KeyError(key)
        return self.__copy(entry.value)

    def __setitem__(self, key: str, value: Any) -> None:
        self.put(key, value)

    def __delitem__(self, key: str) -> None:
        if key not in self.__data or self.__data[key].is_expired():
            raise KeyError(key)
        self.delete(key)

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and self.has(key)



class ScopedStore:
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
        self.__store = store
        self.__scope = scope
        self.__separator = separator

    def _scoped_key(self, key: str) -> str:
        """Generate a scoped key by prepending the scope prefix."""
        return f"{self.__scope}{self.__separator}{key}"

    @property
    def scope(self) -> str:
        """Return the scope identifier."""
        return self.__scope

    def keys(self) -> list[str]:
        """Return keys within this scope, with scope prefix removed."""
        prefix = self._scoped_key("")
        scoped_keys = self.__store.keys()
        return [k[len(prefix):] for k in scoped_keys if k.startswith(prefix)]

    def has(self, key: str) -> bool:
        """Check if a scoped key exists."""
        return self.__store.has(self._scoped_key(key))

    def put(self, key: str, value: Any, ttl_sec: int | None = None) -> None:
        """Store a value with the scoped key."""
        self.__store.put(self._scoped_key(key), value, ttl_sec)

    def get(self, key: str, default_value: Any = None) -> Any:
        """Retrieve a value by scoped key."""
        vl = self.__store.get(self._scoped_key(key), default_value)
        return vl

    def values(self) -> list[Any]:
        """Return all values within this scope."""
        return [self.__store.get(self._scoped_key(k)) for k in self.keys()]

    def delete(self, key: str) -> None:
        """Delete a scoped key."""
        self.__store.delete(self._scoped_key(key))

    def clear(self) -> None:
        """Clear all values within this scope."""
        for key in list(self.keys()):
            self.delete(key)

    def __len__(self) -> int:
        """Return the number of keys in this scope."""
        return len(self.keys())

    def __getitem__(self, key: str) -> Any:
        return self.__store[self._scoped_key(key)]

    def __setitem__(self, key: str, value: Any) -> None:
        self.__store[self._scoped_key(key)] = value

    def __delitem__(self, key: str) -> None:
        del self.__store[self._scoped_key(key)]

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and self.has(key)

    def __str__(self) -> str:
        items = [(k, self.__store.get(self._scoped_key(k))) for k in self.keys()]
        return f"ScopedStore(scope='{self.__scope}')\n" + "\n".join(
            f"  {k}: {v}" for k, v in items
        )

    def __repr__(self) -> str:
        return self.__str__()

