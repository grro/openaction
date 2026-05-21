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
    """
    A single value held by :class:`SimpleStore`, together with its expiration date.

    Entries are the internal storage unit of :class:`SimpleStore`. They wrap the
    user-provided value and the absolute point in time at which the value should
    be considered stale. An entry with ``expire_date == datetime.max`` effectively
    never expires.

    The class is JSON-friendly: :meth:`to_dict` / :meth:`from_dict` provide a
    round-trippable representation used when the store is persisted to disk.
    """

    def __init__(self, value: Any, expire_date: datetime):
        """
        Args:
            value: Arbitrary, JSON-serializable payload to store.
            expire_date: Absolute expiration timestamp. Use ``datetime.max`` for
                "no expiration".
        """
        self.value = value
        self.expire_date = expire_date

    def is_expired(self) -> bool:
        """Return ``True`` if the current local time is past :attr:`expire_date`."""
        return datetime.now() > self.expire_date

    def to_dict(self) -> dict[str, Any]:
        """Serialize this entry to a plain dict suitable for JSON encoding."""
        return {"value": self.value, "expire_date": self.expire_date.isoformat()}

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Entry":
        """Recreate an :class:`Entry` from its :meth:`to_dict` representation."""
        return Entry(data["value"], datetime.fromisoformat(data["expire_date"]))

    def __str__(self) -> str:
        return f"{self.value} (ttl={self.expire_date.strftime('%d.%m %H:%M')})"

    __repr__ = __str__


class SimpleStore(Store):
    """
    A lightweight, thread-safe, file-backed key-value store.

    Behavior:
        * All data lives in memory in a ``dict[str, Entry]`` and is persisted to
          a single gzipped JSON file located at :attr:`filename`.
        * Each entry may carry a TTL. Expired entries are hidden from reads
          (:meth:`get`, :meth:`keys`, :meth:`values`, :meth:`has`) and physically
          removed during the next disk flush.
        * Writes are flushed to disk immediately by default. When
          ``sync_period_sec`` is set, multiple writes within that interval are
          coalesced into a single flush to reduce disk I/O.
        * Disk writes are atomic: data is first written to a temporary file in
          the same directory and then moved into place via :func:`os.replace`,
          which is atomic on both POSIX and Windows.
        * A lock serializes concurrent flushes from different threads, avoiding
          race conditions on the underlying file (notably the Windows
          "file is being used by another process" error).

    Notes:
        Values returned by :meth:`get` and :meth:`values` are deep-copied so that
        callers can mutate them without affecting the stored state.
    """

    def __init__(self, name: str, sync_period_sec: int | None = None, directory: str | None = None):
        """
        Args:
            name: Logical store name. The on-disk file is ``<name>.json.gz``.
            sync_period_sec: If provided, batches writes and only flushes once
                this many seconds have elapsed since the last flush. ``None``
                (the default) flushes after every mutating operation.
            directory: Directory in which the data file lives. Defaults to the
                platform-specific user data directory (see :mod:`platformdirs`).
                Created automatically if missing.
        """
        self._name = name
        self.sync_period_sec = sync_period_sec
        self._directory = Path(directory) if directory else Path(site_data_dir("simpledb", appauthor=False))
        self._directory.mkdir(parents=True, exist_ok=True)

        self._data: dict[str, Entry] = self._load()
        # Force the first write to happen on the next put/delete, regardless of
        # how sync_period_sec is configured.
        self._last_time_stored = datetime.now() - timedelta(days=2)
        # Serializes _store() so concurrent writers don't race on the same file.
        self._write_lock = threading.Lock()
        logger.info(f"simple db: using {self.filename} ({len(self._data)} entries)")

    # ---- Public API --------------------------------------------------------

    @property
    def filename(self) -> Path:
        """
        Absolute path of the gzipped JSON file backing this store.

        The parent directory is (re-)created on access, so the property is safe
        to call even if the directory has been removed at runtime.
        """
        self._directory.mkdir(parents=True, exist_ok=True)
        return self._directory / f"{self._name}.json.gz"

    def __len__(self) -> int:
        """Return the total number of entries, including expired ones not yet purged."""
        return len(self._data)

    def keys(self) -> list[str]:
        """Return all keys whose entries have not yet expired."""
        return [k for k, e in self._data.items() if not e.is_expired()]

    def values(self) -> list[Any]:
        """Return deep copies of all non-expired values."""
        return [copy.deepcopy(e.value) for e in self._data.values() if not e.is_expired()]

    def has(self, key: str) -> bool:
        """Return ``True`` if ``key`` exists and its entry has not expired."""
        entry = self._data.get(key)
        return entry is not None and not entry.is_expired()

    def get(self, key: str, default_value: Any = None) -> Any:
        """
        Return a deep copy of the value for ``key``.

        Returns ``default_value`` if the key is missing or its entry has expired.
        Because the result is deep-copied, callers can freely mutate it without
        affecting the stored state.
        """
        entry = self._data.get(key)
        if entry is None or entry.is_expired():
            return default_value
        return copy.deepcopy(entry.value)

    def put(self, key: str, value: Any, ttl_sec: int | None = None) -> None:
        """
        Insert or update the entry for ``key``.

        Args:
            key: The key to write.
            value: The new value. Should be JSON-serializable so it survives the
                next disk flush.
            ttl_sec: Optional time-to-live in seconds. ``None`` (the default)
                means the entry never expires.

        If the new value and expiration are identical to the current entry the
        call is a no-op and no disk flush is triggered.
        """
        expire_date = datetime.max if ttl_sec is None else datetime.now() + timedelta(seconds=ttl_sec)

        # Skip the write if nothing actually changed.
        existing = self._data.get(key)
        if existing and existing.value == value and existing.expire_date == expire_date:
            return

        self._data[key] = Entry(value, expire_date)
        self._maybe_sync()

    def delete(self, key: str) -> None:
        """Remove the entry for ``key`` if it exists. No-op otherwise."""
        if self._data.pop(key, None) is not None:
            self._maybe_sync()

    def clear(self) -> None:
        """Remove all entries and immediately flush the empty state to disk."""
        self._data.clear()
        self._store()

    def get_values(self) -> list[Any]:
        """Deprecated alias for :meth:`values`."""
        logger.warning("Store#get_values is deprecated. Use Store#values instead")
        return self.values()

    # ---- Internals ---------------------------------------------------------

    def _maybe_sync(self) -> None:
        """
        Flush to disk depending on the configured sync policy.

        When ``sync_period_sec`` is ``None`` every call triggers a flush.
        Otherwise the call only flushes if the configured interval has elapsed
        since the previous successful flush.
        """
        if self.sync_period_sec is None or \
                datetime.now() >= self._last_time_stored + timedelta(seconds=self.sync_period_sec):
            self._store()
            self._last_time_stored = datetime.now()

    def _remove_expired(self) -> None:
        """Physically drop all expired entries from the in-memory dict."""
        for key in [k for k, e in self._data.items() if e.is_expired()]:
            del self._data[key]

    def _load(self) -> dict[str, Entry]:
        """
        Load and decode the on-disk state.

        Returns an empty dict if the file does not exist or cannot be parsed.
        Errors are logged at WARNING level and otherwise swallowed so that a
        corrupted state file cannot prevent the application from starting.
        """
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
        """
        Atomically persist the current in-memory state to disk.

        The implementation:
            1. Acquires :attr:`_write_lock` so concurrent flushes serialize
               instead of racing on the same target file (a common cause of
               ``WinError 32`` on Windows).
            2. Drops expired entries before serializing.
            3. Writes the gzipped JSON payload to a uniquely-named temp file
               in the same directory.
            4. Replaces the target file via :func:`os.replace`, which is atomic
               on both POSIX and Windows and is allowed to overwrite an
               existing destination.
            5. Best-effort removes the temp file if any preceding step failed.
        """
        # Serialize writers so that two threads don't try to replace the same
        # file at once. On Windows this is the typical cause of
        # "WinError 32: file is being used by another process".
        with self._write_lock:
            try:
                self._remove_expired()
            except Exception as error:
                logger.error(f"Error removing expired records: {error}")

            # Write to a temp file first, then atomically move it into place.
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
    A view onto a :class:`SimpleStore` that transparently namespaces every key.

    Each operation prepends ``"<scope><separator>"`` to the key before
    delegating to the wrapped store. This allows multiple unrelated components
    to share a single underlying :class:`SimpleStore` without risking key
    collisions, while still presenting a clean, scope-local API to their
    callers.

    Example::

        store = SimpleStore("shared")
        task1 = ScopedStore(store, "task1")
        task2 = ScopedStore(store, "task2")

        task1.put("status", "running")  # stored as "task1:status"
        task2.put("status", "idle")     # stored as "task2:status"

        task1.keys()  # -> ["status"]   (prefix is stripped on read)
    """

    def __init__(self, store: SimpleStore, scope: str, separator: str = ":"):
        """
        Args:
            store: The underlying store that physically holds the data.
            scope: Namespace prefix applied to every key.
            separator: Character(s) inserted between the scope and the user
                key. Defaults to ``":"``.
        """
        self._store = store
        self._scope = scope
        self._separator = separator
        self._prefix = f"{scope}{separator}"

    @property
    def scope(self) -> str:
        """The namespace prefix used by this view (without the separator)."""
        return self._scope

    def _scoped(self, key: str) -> str:
        """Return the fully-qualified key as stored in the underlying store."""
        return self._prefix + key

    def put(self, key: str, value: Any, ttl_sec: int | None = None) -> None:
        """Write ``value`` for ``key`` in this scope. See :meth:`SimpleStore.put`."""
        self._store.put(self._scoped(key), value, ttl_sec)

    def get(self, key: str, default_value: Any = None) -> Any:
        """Read the value for ``key`` in this scope. See :meth:`SimpleStore.get`."""
        return self._store.get(self._scoped(key), default_value)

    def delete(self, key: str) -> None:
        """Delete ``key`` from this scope. No-op if the key is missing."""
        self._store.delete(self._scoped(key))

    def keys(self) -> list[str]:
        """Return all keys belonging to this scope, with the scope prefix stripped."""
        return [k[len(self._prefix):] for k in self._store.keys() if k.startswith(self._prefix)]
