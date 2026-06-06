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

    An ``Entry`` is the unit of storage inside :class:`SimpleStore`. The
    ``value`` may be any JSON-serializable Python object (str, int, float,
    bool, None, list, dict). The ``expire_date`` is an absolute point in
    time after which the entry is considered stale and will be filtered
    out from reads and dropped during the next disk sync.

    Use :attr:`datetime.max` as ``expire_date`` to indicate that the
    entry should never expire.
    """

    def __init__(self, value: Any, expire_date: datetime):
        """
        Args:
            value: The payload to store. Must be JSON-serializable.
            expire_date: Absolute timestamp after which the entry is expired.
        """
        self.value = value
        self.expire_date = expire_date

    def is_expired(self) -> bool:
        """Return ``True`` if the current wall-clock time is past :attr:`expire_date`."""
        return datetime.now() > self.expire_date

    def to_dict(self) -> dict[str, Any]:
        """Serialize the entry to a JSON-friendly ``dict`` (used by :meth:`SimpleStore._store`)."""
        return {"value": self.value, "expire_date": self.expire_date.isoformat()}

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Entry":
        """Inverse of :meth:`to_dict`; reconstruct an ``Entry`` from its JSON form."""
        return Entry(data["value"], datetime.fromisoformat(data["expire_date"]))

    def __str__(self) -> str:
        return f"{self.value} (ttl={self.expire_date.strftime('%d.%m %H:%M')})"

    __repr__ = __str__


class SimpleStore(Store):
    """
    A lightweight, thread-safe, file-backed key-value store with per-entry TTL.

    Design
    ------
    * **In-memory first:** All data lives in a plain ``dict`` for O(1) access.
      The on-disk file is treated purely as a persistence snapshot.
    * **Gzipped JSON on disk:** State is serialized to ``<name>.json.gz`` inside
      the configured directory. Using gzip keeps the footprint small for
      stores dominated by repetitive keys/strings.
    * **Atomic writes:** Each flush writes to a temporary sibling file and
      then uses :func:`os.replace` to swap it into place. This avoids
      partially-written files on crash and works on both POSIX and Windows.
    * **Thread-safe persistence:** A :class:`threading.Lock` serializes the
      actual file replacement, which prevents the typical
      ``WinError 32: file is being used by another process`` race when
      multiple threads flush concurrently.
    * **TTL semantics:** Expired entries are hidden from all read methods
      immediately and physically removed during the next :meth:`_store`
      cycle.

    Write batching
    --------------
    By default every mutation (:meth:`put`, :meth:`delete`) is flushed to
    disk immediately. If you pass ``sync_period_sec`` to the constructor,
    flushes are coalesced: a mutation only triggers a write once that many
    seconds have elapsed since the previous flush. This is useful for
    high-throughput workloads where you can tolerate losing the last few
    seconds of writes on a crash.

    Storage location
    ----------------
    If ``directory`` is omitted, the platform-appropriate user data
    directory is used (via :func:`platformdirs.site_data_dir` under the
    application name ``"simpledb"``).

    Example
    -------
    >>> store = SimpleStore("my_app")
    >>> store.put("greeting", "hello", ttl_sec=60)
    >>> store.get("greeting")
    'hello'
    """

    def __init__(self, name: str, sync_period_sec: int | None = None, directory: str | None = None):
        """
        Args:
            name: Logical store name. Used as the on-disk file stem
                (``<name>.json.gz``). Pick something filesystem-safe.
            sync_period_sec: Optional minimum interval between disk flushes.
                ``None`` (the default) flushes after every mutation. Any
                positive integer enables write batching with that many
                seconds of granularity.
            directory: Optional absolute path to the storage directory.
                Defaults to the OS user data directory for application
                ``"simpledb"``. The directory is created if missing.
        """
        self._name = name
        self.sync_period_sec = sync_period_sec
        self._directory = Path(directory) if directory else Path(site_data_dir("simpledb", appauthor=False))
        self._directory.mkdir(parents=True, exist_ok=True)

        self._data: dict[str, Entry] = self._load()
        # Backdate the last flush so the very next mutation will write,
        # regardless of `sync_period_sec`.
        self._last_time_stored = datetime.now() - timedelta(days=2)
        # Guards _store() so concurrent writers don't race on the same file.
        self._write_lock = threading.Lock()
        logger.info(f"simple db: using {self.filename} ({len(self._data)} entries)")

    # ---- Public API --------------------------------------------------------

    @property
    def filename(self) -> Path:
        """
        Absolute path of the gzipped JSON file backing this store.

        The directory is (re-)created on access so callers may safely use
        this property even after the directory has been removed
        externally.
        """
        self._directory.mkdir(parents=True, exist_ok=True)
        return self._directory / f"{self._name}.json.gz"


    def __len__(self) -> int:
        """
        Return the total number of stored entries, **including expired ones**.

        Expired entries are pruned lazily on the next flush, so this count
        can temporarily exceed ``len(self.keys())``.
        """
        return len(self._data)

    def keys(self) -> list[str]:
        """Return a snapshot list of all non-expired keys."""
        return [k for k, e in self._data.items() if not e.is_expired()]

    def values(self) -> list[Any]:
        """
        Return deep copies of all non-expired values.

        Values are copied so that callers cannot mutate the in-memory
        store by modifying the returned objects.
        """
        return [copy.deepcopy(e.value) for e in self._data.values() if not e.is_expired()]

    def has(self, key: str) -> bool:
        """Return ``True`` if ``key`` exists and has not expired."""
        entry = self._data.get(key)
        return entry is not None and not entry.is_expired()

    def get(self, key: str, default_value: Any = None) -> Any:
        """
        Look up ``key`` and return a deep copy of its value.

        Args:
            key: The key to look up.
            default_value: Returned verbatim if the key is missing or
                its entry has expired.

        Returns:
            A deep copy of the stored value, or ``default_value``.
        """
        entry = self._data.get(key)
        if entry is None or entry.is_expired():
            return default_value
        return copy.deepcopy(entry.value)

    def put(self, key: str, value: Any, ttl_sec: int | None = None) -> None:
        """
        Insert or update an entry.

        Args:
            key: The key to write.
            value: Any JSON-serializable value.
            ttl_sec: Time-to-live in seconds. ``None`` (the default)
                stores the entry without expiration.

        Notes:
            If the new ``value`` and ``ttl_sec`` are identical to what is
            already stored, this call is a no-op (no disk write).
        """
        expire_date = datetime.max if ttl_sec is None else datetime.now() + timedelta(seconds=ttl_sec)

        # Skip the write if nothing actually changed.
        existing = self._data.get(key)
        if existing and existing.value == value and existing.expire_date == expire_date:
            return

        self._data[key] = Entry(value, expire_date)
        self._maybe_sync()

    def delete(self, key: str) -> None:
        """Remove ``key`` if present. A no-op if the key does not exist."""
        if self._data.pop(key, None) is not None:
            self._maybe_sync()

    def clear(self) -> None:
        """Remove **all** entries and flush the empty state to disk immediately."""
        self._data.clear()
        self._store()

    def get_values(self) -> list[Any]:
        """Deprecated alias of :meth:`values`. Will be removed in a future release."""
        logger.warning("Store#get_values is deprecated. Use Store#values instead")
        return self.values()

    # ---- Internals ---------------------------------------------------------

    def _maybe_sync(self) -> None:
        """
        Flush to disk respecting the optional ``sync_period_sec`` budget.

        When batching is disabled (``sync_period_sec is None``) every call
        triggers a write. Otherwise a write is only performed once the
        configured interval has elapsed since the previous flush.
        """
        if self.sync_period_sec is None or \
                datetime.now() >= self._last_time_stored + timedelta(seconds=self.sync_period_sec):
            self._store()
            self._last_time_stored = datetime.now()

    def _remove_expired(self) -> None:
        """Drop every entry whose TTL has elapsed. Called right before each flush."""
        for key in [k for k, e in self._data.items() if e.is_expired()]:
            del self._data[key]

    def _load(self) -> dict[str, Entry]:
        """
        Read and deserialize the backing file.

        Returns an empty dict if the file does not exist or cannot be
        parsed; in the latter case a warning is logged but the error is
        otherwise swallowed so the store remains usable.
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
        Atomically persist the current in-memory state.

        Steps:
            1. Acquire :attr:`_write_lock` to serialize concurrent writers.
            2. Prune expired entries so they do not get persisted.
            3. Write the JSON payload to a uniquely-named temp file in the
               same directory (so :func:`os.replace` stays on the same
               filesystem).
            4. :func:`os.replace` the temp file onto the target path. This
               operation is atomic on both POSIX and Windows and is the
               only step that other readers can observe.
            5. Clean up the temp file if it still exists after a failure.

        Errors are logged and suppressed so a transient I/O failure does
        not crash the calling thread.
        """
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
    A namespacing view over an existing :class:`Store`.

    ``ScopedStore`` is a thin decorator that prefixes every key with a
    fixed ``scope`` string (separated by ``separator``, default ``":"``)
    before delegating to the underlying store. It owns no state of its
    own and performs no I/O beyond what the wrapped store does.

    Typical use case: give each component, task, or tenant its own
    isolated namespace inside a single shared :class:`SimpleStore`
    instance, without having to manage prefixes manually at every call
    site.

    Example
    -------
    >>> backing = SimpleStore("shared")
    >>> task1 = ScopedStore(backing, "task1")
    >>> task2 = ScopedStore(backing, "task2")
    >>> task1.put("status", "running")   # physically stored as "task1:status"
    >>> task2.put("status", "idle")      # physically stored as "task2:status"
    >>> task1.get("status")
    'running'
    >>> task1.keys()
    ['status']

    Note
    ----
    Keys returned by :meth:`keys` have the scope prefix stripped, so
    consumers see the same view they would get from a dedicated store.
    """

    def __init__(self, store: SimpleStore, scope: str, separator: str = ":"):
        """
        Args:
            store: The underlying store that actually persists data.
            scope: The namespace prefix applied to every key.
            separator: Character(s) inserted between scope and key.
                Defaults to ``":"``. Pick something that cannot appear in
                your real keys to keep :meth:`keys` unambiguous.
        """
        self._store = store
        self._scope = scope
        self._revision = 0
        self._separator = separator
        self._prefix = f"{scope}{separator}"

    @property
    def revision(self) -> int:
        return self._revision

    @property
    def scope(self) -> str:
        """The scope/prefix associated with this view."""
        return self._scope

    def _scoped(self, key: str) -> str:
        """Return ``key`` decorated with this store's scope prefix."""
        return self._prefix + key

    def put(self, key: str, value: Any, ttl_sec: int | None = None) -> None:
        """See :meth:`SimpleStore.put`; the key is transparently prefixed."""
        self._store.put(self._scoped(key), value, ttl_sec)
        self._revision = self._revision + 1

    def get(self, key: str, default_value: Any = None) -> Any:
        """See :meth:`SimpleStore.get`; the key is transparently prefixed."""
        return self._store.get(self._scoped(key), default_value)

    def delete(self, key: str) -> None:
        """See :meth:`SimpleStore.delete`; the key is transparently prefixed."""
        self._store.delete(self._scoped(key))
        self._revision = self._revision + 1

    def keys(self) -> list[str]:
        """Return all keys belonging to this scope, with the scope prefix stripped."""
        return [k[len(self._prefix):] for k in self._store.keys() if k.startswith(self._prefix)]
