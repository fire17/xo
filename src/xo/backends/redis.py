from __future__ import annotations

import contextlib
import hashlib
import ipaddress
import secrets
import socket
import threading
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Final
from urllib.parse import parse_qs, unquote, urlsplit

from ..codec import DEFAULT_CODEC, Codec
from ..events import Event, EventGroup
from ..exceptions import CommitOutcomeUnknown, ConflictError, PersistenceError, RecoveryRequired
from ..wire import (
    SCHEMA_VERSION,
    commit_envelope,
    decode_envelope,
    encode_envelope,
    item_from_envelope,
)

_DEFAULT_URL: Final = "redis://127.0.0.1:6379/0"
_MAX_NAMESPACE_BYTES: Final = 256

_INIT_SCRIPT: Final = """
local ht = redis.call('TYPE', KEYS[1])['ok']
local mt = redis.call('TYPE', KEYS[2])['ok']
if ht ~= 'none' and ht ~= 'string' then return {'wrongtype', 'head'} end
if mt ~= 'none' and mt ~= 'hash' then return {'wrongtype', 'meta'} end
local schema = redis.call('HGET', KEYS[2], 'schema')
local epoch = redis.call('HGET', KEYS[2], 'epoch')
if schema and schema ~= ARGV[1] then return {'schema', schema} end
if epoch and ARGV[3] ~= '' and epoch ~= ARGV[3] then return {'epoch', epoch} end
if not epoch then epoch = ARGV[2] end
if not schema then schema = ARGV[1] end
redis.call('HSET', KEYS[2], 'schema', schema, 'epoch', epoch)
if not redis.call('GET', KEYS[1]) then redis.call('SET', KEYS[1], '0') end
return {'ok', epoch, redis.call('GET', KEYS[1])}
""".strip()

_COMMIT_SCRIPT: Final = """
local ht = redis.call('TYPE', KEYS[1])['ok']
local lt = redis.call('TYPE', KEYS[2])['ok']
local st = redis.call('TYPE', KEYS[3])['ok']
local mt = redis.call('TYPE', KEYS[5])['ok']
local ot = redis.call('TYPE', KEYS[6])['ok']
if ht ~= 'none' and ht ~= 'string' then return {'wrongtype', 'head'} end
if lt ~= 'none' and lt ~= 'stream' then return {'wrongtype', 'log'} end
if st ~= 'none' and st ~= 'string' then return {'wrongtype', 'snapshot'} end
if mt ~= 'hash' then return {'wrongtype', 'meta'} end
if ot ~= 'none' and ot ~= 'hash' then return {'wrongtype', 'origins'} end
local schema = redis.call('HGET', KEYS[5], 'schema')
local epoch = redis.call('HGET', KEYS[5], 'epoch')
if schema ~= ARGV[1] then return {'schema', schema or ''} end
if epoch ~= ARGV[2] then return {'epoch', epoch or ''} end
local head = tonumber(redis.call('GET', KEYS[1]) or '0')
if head ~= tonumber(ARGV[3]) then return {'conflict', tostring(head)} end
if tonumber(ARGV[4]) ~= head + 1 then return {'revision', tostring(head)} end
local stream_id = ARGV[4] .. '-0'
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[8], stream_id,
  'b', ARGV[5], 'r', ARGV[4], 'i', ARGV[6], 'e', ARGV[2], 'h', ARGV[7])
redis.call('SET', KEYS[1], ARGV[4])
if ARGV[9] ~= '' then redis.call('SET', KEYS[3], ARGV[9]) end
redis.call('HSET', KEYS[5], 'head_hash', ARGV[7])
redis.call('HSET', KEYS[6], ARGV[10], ARGV[4])
redis.call('PUBLISH', KEYS[4], ARGV[4])
return {'ok', stream_id}
""".strip()

_SNAPSHOT_SCRIPT: Final = """
local head = tonumber(redis.call('GET', KEYS[1]) or '0')
if head ~= tonumber(ARGV[1]) then return {'conflict', tostring(head)} end
local schema = redis.call('HGET', KEYS[3], 'schema')
local epoch = redis.call('HGET', KEYS[3], 'epoch')
if schema ~= ARGV[2] then return {'schema', schema or ''} end
if epoch ~= ARGV[3] then return {'epoch', epoch or ''} end
redis.call('SET', KEYS[2], ARGV[4])
return {'ok'}
""".strip()


class RedisBackendError(PersistenceError):
    """A definite Redis backend or namespace failure."""


class RedisProtocolError(RedisBackendError):
    """Redis returned malformed or internally inconsistent state."""


class RedisUnavailable(RedisBackendError):
    """Redis could not be reached within the configured finite attempts."""


@dataclass(frozen=True, slots=True)
class RedisEndpoint:
    host: str | None
    port: int | None
    path: str | None
    database: int
    username: str | None
    password: str | None

    @classmethod
    def parse(cls, url: str) -> RedisEndpoint:
        parsed = urlsplit(url)
        if parsed.scheme == "redis":
            host = parsed.hostname or "127.0.0.1"
            if not _is_loopback(host):
                raise ValueError("XO v1 Redis endpoints must be loopback or Unix sockets")
            try:
                port = parsed.port or 6379
            except ValueError as error:
                raise ValueError(f"invalid Redis port: {error}") from error
            database = _database(parsed.path.lstrip("/") or "0")
            if parsed.query or parsed.fragment:
                raise ValueError("Redis TCP URL cannot contain query or fragment components")
            return cls(
                host,
                port,
                None,
                database,
                None if parsed.username is None else unquote(parsed.username),
                None if parsed.password is None else unquote(parsed.password),
            )
        if parsed.scheme in {"unix", "redis+unix"}:
            if parsed.netloc not in {"", "localhost"}:
                raise ValueError("Unix Redis URL must not name a remote host")
            path = unquote(parsed.path)
            if not path or not path.startswith("/"):
                raise ValueError("Unix Redis URL requires an absolute socket path")
            query = parse_qs(parsed.query, strict_parsing=True)
            unknown = set(query) - {"db", "password", "username"}
            if unknown:
                raise ValueError(f"unsupported Unix Redis URL options: {sorted(unknown)!r}")
            return cls(
                None,
                None,
                path,
                _database(query.get("db", ["0"])[-1]),
                query.get("username", [None])[-1],
                query.get("password", [None])[-1],
            )
        raise ValueError("Redis URL scheme must be redis, unix, or redis+unix")


@dataclass(frozen=True, slots=True)
class RedisLimits:
    max_frame_bytes: int = 16 * 1024 * 1024
    max_array_items: int = 100_000
    max_nesting: int = 32
    log_retention: int = 10_000
    catchup_batch: int = 256
    max_catchup_batches: int = 64
    dedupe_size: int = 65_536

    def __post_init__(self) -> None:
        for name in (
            "max_frame_bytes",
            "max_array_items",
            "max_nesting",
            "log_retention",
            "catchup_batch",
            "max_catchup_batches",
            "dedupe_size",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive and bounded")


class RedisBackend:
    """Strict stdlib RESP Redis CAS persistence and namespace replication.

    Construction is inert. ``prepare`` owns the command connection, while
    ``start_listener`` explicitly starts the one non-daemon Pub/Sub listener.
    Redis Streams remain authoritative; Pub/Sub is only a low-latency wake-up.
    """

    strict = True

    def __init__(
        self,
        url: str = _DEFAULT_URL,
        *,
        namespace: str | None = None,
        epoch: str | None = None,
        codec: Codec = DEFAULT_CODEC,
        schema_version: int = SCHEMA_VERSION,
        connect_timeout: float = 2.0,
        operation_timeout: float = 2.0,
        close_timeout: float = 3.0,
        poll_interval: float = 1.0,
        reconnect_attempts: int = 5,
        reconnect_initial: float = 0.05,
        reconnect_max: float = 1.0,
        query_attempts: int = 2,
        snapshot_every: int = 512,
        limits: RedisLimits | None = None,
        socket_factory: Callable[[], socket.socket] | None = None,
        on_listener_error: Callable[[BaseException], object] | None = None,
        strict: bool = True,
    ) -> None:
        if not strict:
            raise ValueError("RedisBackend is strict; volatile fallback is not supported")
        for name, value in (
            ("connect_timeout", connect_timeout),
            ("operation_timeout", operation_timeout),
            ("close_timeout", close_timeout),
            ("poll_interval", poll_interval),
            ("reconnect_initial", reconnect_initial),
            ("reconnect_max", reconnect_max),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be positive and finite")
        if reconnect_attempts < 0:
            raise ValueError("reconnect_attempts cannot be negative")
        if query_attempts <= 0:
            raise ValueError("query_attempts must be positive and bounded")
        if snapshot_every <= 0:
            raise ValueError("snapshot_every must be positive and bounded")
        if schema_version <= 0:
            raise ValueError("schema_version must be positive")
        if epoch is not None and (not epoch or len(epoch.encode("utf-8")) > 256):
            raise ValueError("epoch must be a non-empty string of at most 256 bytes")
        self.url = url
        self.endpoint = RedisEndpoint.parse(url)
        self.codec = codec
        self.schema_version = schema_version
        self.connect_timeout = connect_timeout
        self.operation_timeout = operation_timeout
        self.close_timeout = close_timeout
        self.poll_interval = poll_interval
        self.reconnect_attempts = reconnect_attempts
        self.reconnect_initial = reconnect_initial
        self.reconnect_max = reconnect_max
        self.query_attempts = query_attempts
        self.snapshot_every = snapshot_every
        self.limits = limits or RedisLimits()
        self._socket_factory = socket_factory
        self._on_listener_error = on_listener_error
        self._namespace = None if namespace is None else _namespace(namespace)
        self._expected_epoch = epoch
        self._epoch: str | None = None
        self._keys: _NamespaceKeys | None = None
        self._command = self._connection()
        self._pubsub: _RESPConnection | None = None
        self._lifecycle_lock = threading.RLock()
        self._state_lock = threading.Lock()
        self._catchup_lock = threading.Lock()
        self._stop = threading.Event()
        self._listener: threading.Thread | None = None
        self._listener_error: BaseException | None = None
        self._start_requested = False
        self._closed = False
        self._remote_sink: Callable[[Event | EventGroup], object] | None = None
        self._snapshot_provider: Callable[[], object] | None = None
        self._snapshot_sink: Callable[[object], object] | None = None
        self._remote_revision = 0
        self._local_origin_id: int | None = None
        self._seen: set[int] = set()
        self._seen_order: deque[int] = deque()

    @property
    def namespace(self) -> str | None:
        return self._namespace

    @property
    def epoch(self) -> str | None:
        return self._epoch

    @property
    def remote_revision(self) -> int:
        with self._state_lock:
            return self._remote_revision

    @property
    def listener_running(self) -> bool:
        thread = self._listener
        return thread is not None and thread.is_alive()

    @property
    def listener_error(self) -> BaseException | None:
        return self._listener_error

    def bind(self, namespace: str) -> None:
        namespace = _namespace(namespace)
        with self._lifecycle_lock:
            self._ensure_open()
            if self._namespace is not None and self._namespace != namespace:
                raise RedisBackendError(
                    f"backend is bound to {self._namespace!r}, not {namespace!r}"
                )
            if self._namespace is None:
                self._namespace = namespace

    def prepare(self) -> None:
        """Open and authenticate the command connection; start no thread."""
        with self._lifecycle_lock:
            self._ensure_open()
            try:
                self._command.connect()
            except (_TransportFailure, _RESPError) as error:
                raise RedisUnavailable("Redis command connection failed") from error
            if self._namespace is not None:
                self._initialize_namespace()

    def set_remote_sink(
        self,
        sink: Callable[[Event | EventGroup], object],
        *,
        revision: int = 0,
        origin_id: int | None = None,
        snapshot_sink: Callable[[object], object] | None = None,
    ) -> None:
        if not callable(sink):
            raise TypeError("remote sink must be callable")
        if revision < 0:
            raise ValueError("remote revision cannot be negative")
        with self._lifecycle_lock:
            self._ensure_open()
            if self.listener_running:
                raise RuntimeError("remote sink cannot change while the listener is running")
            self._remote_sink = sink
            self._snapshot_sink = snapshot_sink
            with self._state_lock:
                self._remote_revision = revision
                self._local_origin_id = origin_id

    def set_snapshot_provider(self, provider: Callable[[], object] | None) -> None:
        if provider is not None and not callable(provider):
            raise TypeError("snapshot provider must be callable")
        with self._lifecycle_lock:
            self._ensure_open()
            self._snapshot_provider = provider

    def start(self) -> None:
        """Lifecycle hook: start sync only when a sink was explicitly configured."""
        with self._lifecycle_lock:
            self._ensure_open()
            self._start_requested = True
            self.prepare()
            if self._remote_sink is not None and self._namespace is not None:
                self._start_listener_locked()

    def start_listener(
        self,
        sink: Callable[[Event | EventGroup], object] | None = None,
        *,
        revision: int | None = None,
        origin_id: int | None = None,
        snapshot_sink: Callable[[object], object] | None = None,
    ) -> None:
        """Explicitly start the owned replication listener exactly once."""
        with self._lifecycle_lock:
            self._ensure_open()
            if sink is not None:
                self.set_remote_sink(
                    sink,
                    revision=self.remote_revision if revision is None else revision,
                    origin_id=origin_id,
                    snapshot_sink=snapshot_sink,
                )
            elif revision is not None:
                raise ValueError("revision can only be supplied with a sink")
            if self._remote_sink is None:
                raise RuntimeError("set_remote_sink is required before start_listener")
            if self._namespace is None:
                raise RuntimeError("bind a namespace before start_listener")
            self._start_requested = True
            self.prepare()
            self._start_listener_locked()

    def commit(self, item: Event | EventGroup) -> str:
        """Atomically CAS head, append the complete unit, snapshot, and wake peers."""
        first = _first(item)
        if first.revision != first.base_revision + 1:
            raise RedisBackendError("commit revision must be exactly base_revision + 1")
        namespace = _unit_namespace(item)
        identity = _identity(item)
        body = encode_envelope(commit_envelope(item, message_id=identity), codec=self.codec)
        if len(body) > self.limits.max_frame_bytes:
            raise RedisBackendError(
                f"commit envelope exceeds {self.limits.max_frame_bytes} bytes"
            )
        digest = hashlib.sha256(body).hexdigest()
        with self._lifecycle_lock:
            self._ensure_open()
            self.bind(namespace)
            self._initialize_namespace()
            assert self._keys is not None and self._epoch is not None
            snapshot_body = self._snapshot_before_commit(first.base_revision)
            args: tuple[object, ...] = (
                "EVAL",
                _COMMIT_SCRIPT,
                6,
                self._keys.head,
                self._keys.log,
                self._keys.snapshot,
                self._keys.channel,
                self._keys.meta,
                self._keys.origins,
                self.schema_version,
                self._epoch,
                first.base_revision,
                first.revision,
                body,
                format(identity, "x"),
                digest,
                self.limits.log_retention,
                snapshot_body,
                format(first.origin_id, "x"),
            )
            try:
                reply = self._command.execute(*args)
            except _TransportFailure as error:
                if error.sent:
                    raise CommitOutcomeUnknown(
                        "Redis may have accepted the commit; reconcile the same event identity"
                    ) from error
                raise RedisUnavailable("Redis commit was not sent") from error
            except _RESPError as error:
                raise RedisBackendError(f"Redis rejected commit: {error}") from error
            stream_id = self._commit_reply(reply, first.base_revision)
            with self._state_lock:
                self._remember_locked(identity)
                self._remote_revision = first.revision
            if self._start_requested and self._remote_sink is not None:
                self._start_listener_locked()
            return stream_id

    def reconcile(self, item: Event | EventGroup) -> bool:
        """Resolve an unknown outcome by revision, identity, and canonical body hash."""
        first = _first(item)
        namespace = _unit_namespace(item)
        identity = _identity(item)
        body = encode_envelope(commit_envelope(item, message_id=identity), codec=self.codec)
        digest = hashlib.sha256(body).hexdigest()
        with self._lifecycle_lock:
            self._ensure_open()
            self.bind(namespace)
            try:
                self._initialize_namespace()
                assert self._keys is not None
                head = self._head()
                if head == first.base_revision:
                    return False
                if head < first.revision:
                    raise RecoveryRequired(
                        f"Redis head {head} is behind candidate revision {first.revision}"
                    )
                entries = _stream_entries(
                    self._query(
                        "XRANGE",
                        self._keys.log,
                        f"{first.revision}-0",
                        f"{first.revision}-0",
                        "COUNT",
                        1,
                    )
                )
            except (RedisBackendError, _RESPError, _TransportFailure) as error:
                raise CommitOutcomeUnknown(
                    "Redis outcome remains unreadable; do not retry with a fresh identity"
                ) from error
            if not entries:
                raise CommitOutcomeUnknown(
                    "candidate revision was trimmed before its identity could be reconciled"
                )
            entry = entries[0]
            actual_identity = _field_text(entry.fields, "i")
            actual_digest = _field_text(entry.fields, "h")
            if actual_identity == format(identity, "x"):
                if actual_digest != digest:
                    raise RecoveryRequired(
                        "same Redis revision and identity has a different canonical body hash"
                    )
                with self._state_lock:
                    self._remember_locked(identity)
                    self._remote_revision = max(self._remote_revision, first.revision)
                return True
            raise ConflictError(
                f"Redis revision {first.revision} belongs to commit {actual_identity}, "
                f"not {format(identity, 'x')}"
            )

    def store_snapshot(self, snapshot: object, *, revision: int) -> None:
        """Store a snapshot only while Redis is still at that revision."""
        if revision < 0:
            raise ValueError("snapshot revision cannot be negative")
        with self._lifecycle_lock:
            self._ensure_open()
            if self._namespace is None:
                raise RuntimeError("bind a namespace before storing a snapshot")
            self._initialize_namespace()
            assert self._keys is not None and self._epoch is not None
            body = self._encode_snapshot(snapshot, revision)
            reply = self._query(
                "EVAL",
                _SNAPSHOT_SCRIPT,
                3,
                self._keys.head,
                self._keys.snapshot,
                self._keys.meta,
                revision,
                self.schema_version,
                self._epoch,
                body,
            )
            parts = _reply_parts(reply, minimum=1)
            if parts[0] == "ok":
                return
            self._raise_status(parts[0], parts[1:])

    def catch_up(self) -> int:
        """Synchronously deliver bounded, contiguous log batches to the remote sink."""
        with self._catchup_lock:
            return self._catch_up_locked()

    def close(self) -> None:
        """Bounded, idempotent release of listener and both owned connections."""
        with self._lifecycle_lock:
            if not self._closed:
                self._closed = True
                self._stop.set()
                pubsub = self._pubsub
                if pubsub is not None:
                    pubsub.close()
                self._command.close()
            thread = self._listener
        if thread is not None and thread is not threading.current_thread() and thread.is_alive():
            thread.join(self.close_timeout)
        if thread is not None and thread.is_alive():
            raise RedisBackendError(
                f"Redis listener did not stop within {self.close_timeout} seconds"
            )

    def _connection(self) -> _RESPConnection:
        return _RESPConnection(
            self.endpoint,
            connect_timeout=self.connect_timeout,
            operation_timeout=self.operation_timeout,
            max_frame_bytes=self.limits.max_frame_bytes,
            max_array_items=self.limits.max_array_items,
            max_nesting=self.limits.max_nesting,
            socket_factory=self._socket_factory,
        )

    def _ensure_open(self) -> None:
        if self._closed:
            raise RedisBackendError("Redis backend is closed")

    def _initialize_namespace(self) -> None:
        if self._epoch is not None:
            return
        if self._namespace is None:
            raise RuntimeError("Redis namespace is not bound")
        candidate = self._expected_epoch or secrets.token_hex(16)
        keys = _NamespaceKeys.for_namespace(self._namespace)
        reply = self._query(
            "EVAL",
            _INIT_SCRIPT,
            2,
            keys.head,
            keys.meta,
            self.schema_version,
            candidate,
            self._expected_epoch or "",
        )
        parts = _reply_parts(reply, minimum=1)
        if parts[0] != "ok":
            self._raise_status(parts[0], parts[1:])
        if len(parts) != 3:
            raise RedisProtocolError("namespace initialization returned malformed metadata")
        _nonnegative_integer(parts[2], "Redis head")
        self._keys = keys
        self._epoch = parts[1]

    def _snapshot_before_commit(self, base_revision: int) -> bytes:
        provider = self._snapshot_provider
        if provider is None or base_revision % self.snapshot_every:
            return b""
        try:
            return self._encode_snapshot(provider(), base_revision)
        except RedisBackendError:
            raise
        except BaseException as error:
            raise RedisBackendError(
                "snapshot provider failed before Redis commit; commit was not sent"
            ) from error

    def _encode_snapshot(self, snapshot: object, revision: int) -> bytes:
        if isinstance(snapshot, Mapping):
            observed_namespace = snapshot.get("namespace")
            observed_revision = snapshot.get("revision")
            if observed_namespace is not None and observed_namespace != self._namespace:
                raise RedisBackendError("snapshot namespace does not match Redis namespace")
            if observed_revision is not None and observed_revision != revision:
                raise RedisBackendError(
                    f"snapshot revision {observed_revision!r} does not match {revision}"
                )
        body = self.codec.dumps(
            {
                "schema": "xo.redis.snapshot",
                "version": 1,
                "namespace": self._namespace,
                "schema_version": self.schema_version,
                "epoch": self._epoch,
                "revision": revision,
                "snapshot": snapshot,
            }
        )
        if len(body) > self.limits.max_frame_bytes:
            raise RedisBackendError(f"snapshot exceeds {self.limits.max_frame_bytes} bytes")
        return body

    def _decode_snapshot(self, body: bytes, *, head: int) -> tuple[int, object]:
        try:
            value = self.codec.loads(body)
        except BaseException as error:
            raise RedisProtocolError("Redis snapshot is not valid tagged JSON") from error
        if not isinstance(value, Mapping):
            raise RedisProtocolError("Redis snapshot wrapper must be an object")
        expected = {
            "schema": "xo.redis.snapshot",
            "version": 1,
            "namespace": self._namespace,
            "schema_version": self.schema_version,
            "epoch": self._epoch,
        }
        for key, required in expected.items():
            if value.get(key) != required:
                raise RecoveryRequired(
                    f"Redis snapshot {key} {value.get(key)!r} does not match {required!r}"
                )
        revision = value.get("revision")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
            raise RedisProtocolError("Redis snapshot revision is invalid")
        if revision > head:
            raise RecoveryRequired(f"Redis snapshot revision {revision} is ahead of head {head}")
        if "snapshot" not in value:
            raise RedisProtocolError("Redis snapshot payload is missing")
        return revision, value["snapshot"]

    def _commit_reply(self, reply: object, base_revision: int) -> str:
        parts = _reply_parts(reply, minimum=1)
        if parts[0] == "ok" and len(parts) == 2:
            return parts[1]
        if parts[0] == "conflict":
            observed = parts[1] if len(parts) > 1 else "unknown"
            raise ConflictError(
                f"Redis head {observed} does not match base revision {base_revision}"
            )
        self._raise_status(parts[0], parts[1:])
        raise AssertionError("unreachable")

    def _raise_status(self, status: str, detail: list[str]) -> None:
        observed = detail[0] if detail else "unknown"
        if status in {"conflict", "revision"}:
            raise ConflictError(f"Redis revision conflict; observed head {observed}")
        if status == "schema":
            raise RecoveryRequired(
                f"Redis schema {observed!r} does not match {self.schema_version}"
            )
        if status == "epoch":
            raise RecoveryRequired(
                f"Redis namespace epoch {observed!r} does not match {self._epoch!r}"
            )
        raise RedisBackendError(f"Redis namespace rejected operation: {status} {observed}")

    def _query(self, *parts: object) -> object:
        last: BaseException | None = None
        for _ in range(self.query_attempts):
            try:
                return self._command.execute(*parts)
            except (_TransportFailure, _RESPError) as error:
                last = error
                self._command.close_socket()
        raise RedisUnavailable(
            f"Redis query failed after {self.query_attempts} finite attempts"
        ) from last

    def _head(self) -> int:
        assert self._keys is not None
        reply = self._query("GET", self._keys.head)
        return 0 if reply is None else _nonnegative_integer(_text(reply), "Redis head")

    def _catch_up_locked(self) -> int:
        sink = self._remote_sink
        if sink is None:
            raise RuntimeError("set_remote_sink is required before catch_up")
        if self._namespace is None:
            raise RuntimeError("bind a namespace before catch_up")
        self._initialize_namespace()
        assert self._keys is not None and self._epoch is not None
        resnapshotted = False
        for _ in range(self.limits.max_catchup_batches):
            head = self._head()
            with self._state_lock:
                revision = self._remote_revision
            if revision == head:
                return revision
            if revision > head:
                raise RecoveryRequired(f"local revision {revision} is ahead of Redis head {head}")
            entries = _stream_entries(
                self._query(
                    "XRANGE",
                    self._keys.log,
                    f"({revision}-0",
                    "+",
                    "COUNT",
                    self.limits.catchup_batch,
                )
            )
            if not entries or entries[0].revision != revision + 1:
                if resnapshotted:
                    raise RecoveryRequired(
                        f"Redis log remains non-contiguous after resnapshot at {revision}"
                    )
                self._install_snapshot(head=head, minimum_revision=revision)
                resnapshotted = True
                continue
            for entry in entries:
                with self._state_lock:
                    expected = self._remote_revision + 1
                if entry.revision != expected:
                    raise RecoveryRequired(
                        f"Redis revision gap: expected {expected}, observed {entry.revision}"
                    )
                self._deliver_entry(entry, sink)
                if entry.revision >= head:
                    return entry.revision
        raise RecoveryRequired(
            f"catch-up exceeded {self.limits.max_catchup_batches} bounded batches"
        )

    def _install_snapshot(self, *, head: int, minimum_revision: int) -> None:
        assert self._keys is not None
        sink = self._snapshot_sink
        if sink is None:
            raise RecoveryRequired(
                "Redis log gap requires a configured snapshot sink; no local fallback was used"
            )
        reply = self._query("GET", self._keys.snapshot)
        if reply is None:
            raise RecoveryRequired("Redis log gap has no durable snapshot")
        revision, snapshot = self._decode_snapshot(_bytes(reply, "snapshot"), head=head)
        if revision < minimum_revision:
            raise RecoveryRequired(
                f"Redis snapshot revision {revision} is older than local {minimum_revision}"
            )
        try:
            sink(snapshot)
        except BaseException as error:
            raise RedisBackendError("snapshot sink failed; local state was not advanced") from error
        with self._state_lock:
            self._remote_revision = revision
            self._seen.clear()
            self._seen_order.clear()

    def _deliver_entry(
        self, entry: _StreamEntry, sink: Callable[[Event | EventGroup], object]
    ) -> None:
        if _field_text(entry.fields, "e") != self._epoch:
            raise RecoveryRequired("Redis stream entry belongs to another namespace epoch")
        body = _field_bytes(entry.fields, "b")
        digest = hashlib.sha256(body).hexdigest()
        if _field_text(entry.fields, "h") != digest:
            raise RecoveryRequired("Redis stream entry canonical hash does not match its body")
        try:
            item = item_from_envelope(
                decode_envelope(body, codec=self.codec), namespace=self._namespace
            )
        except BaseException as error:
            raise RedisProtocolError("Redis stream contains an invalid commit envelope") from error
        if not isinstance(item, Event | EventGroup):
            raise RedisProtocolError("Redis authoritative log contains a derived projection")
        first = _first(item)
        identity = _identity(item)
        if first.revision != entry.revision:
            raise RedisProtocolError("Redis stream id and event revision disagree")
        if _field_text(entry.fields, "i") != format(identity, "x"):
            raise RedisProtocolError("Redis stream identity and event identity disagree")
        with self._state_lock:
            duplicate = identity in self._seen
            own_origin = self._local_origin_id == first.origin_id
        if not duplicate and not own_origin:
            try:
                sink(item)
            except BaseException as error:
                raise RedisBackendError("remote sink rejected a contiguous Redis commit") from error
        with self._state_lock:
            self._remember_locked(identity)
            self._remote_revision = entry.revision

    def _remember_locked(self, identity: int) -> None:
        if identity in self._seen:
            return
        self._seen.add(identity)
        self._seen_order.append(identity)
        while len(self._seen_order) > self.limits.dedupe_size:
            self._seen.discard(self._seen_order.popleft())

    def _start_listener_locked(self) -> None:
        if self.listener_running:
            return
        self._stop.clear()
        self._listener_error = None
        thread = threading.Thread(
            target=self._listener_main,
            name=f"xo-redis-{self._namespace}",
            daemon=False,
        )
        self._listener = thread
        thread.start()

    def _listener_main(self) -> None:
        failures = 0
        backoff = self.reconnect_initial
        while not self._stop.is_set():
            connection = self._connection()
            with self._state_lock:
                self._pubsub = connection
            try:
                assert self._keys is not None
                _validate_subscribe(
                    connection.execute("SUBSCRIBE", self._keys.channel), self._keys.channel
                )
                self.catch_up()
                while not self._stop.is_set():
                    try:
                        message = connection.read_response()
                    except TimeoutError:
                        message = None
                    if message is not None:
                        _validate_pubsub_message(message, self._keys.channel)
                    self.catch_up()
                return
            except (_TransportFailure, RedisUnavailable):
                failures += 1
                if self._stop.is_set():
                    return
                if failures > self.reconnect_attempts:
                    self._record_listener_error(
                        RedisUnavailable(
                            f"Redis listener exhausted {self.reconnect_attempts} reconnect attempts"
                        )
                    )
                    return
                if self._stop.wait(backoff):
                    return
                backoff = min(backoff * 2, self.reconnect_max)
            except BaseException as error:
                if not self._stop.is_set():
                    self._record_listener_error(error)
                return
            finally:
                connection.close()
                with self._state_lock:
                    if self._pubsub is connection:
                        self._pubsub = None

    def _record_listener_error(self, error: BaseException) -> None:
        self._listener_error = error
        if self._on_listener_error is not None:
            with contextlib.suppress(BaseException):
                self._on_listener_error(error)


@dataclass(frozen=True, slots=True)
class _NamespaceKeys:
    head: str
    log: str
    snapshot: str
    channel: str
    meta: str
    origins: str

    @classmethod
    def for_namespace(cls, namespace: str) -> _NamespaceKeys:
        prefix = f"xo:{{{namespace}}}"
        return cls(
            f"{prefix}:head",
            f"{prefix}:log",
            f"{prefix}:snap",
            f"{prefix}:tx",
            f"{prefix}:meta",
            f"{prefix}:origins",
        )


@dataclass(frozen=True, slots=True)
class _StreamEntry:
    stream_id: str
    revision: int
    fields: Mapping[str, bytes]


class _RESPError(Exception):
    pass


class _TransportFailure(Exception):
    def __init__(self, message: str, *, sent: bool) -> None:
        self.sent = sent
        super().__init__(message)


class _RESPConnection:
    def __init__(
        self,
        endpoint: RedisEndpoint,
        *,
        connect_timeout: float,
        operation_timeout: float,
        max_frame_bytes: int,
        max_array_items: int,
        max_nesting: int,
        socket_factory: Callable[[], socket.socket] | None,
    ) -> None:
        self.endpoint = endpoint
        self.connect_timeout = connect_timeout
        self.operation_timeout = operation_timeout
        self.max_frame_bytes = max_frame_bytes
        self.max_array_items = max_array_items
        self.max_nesting = max_nesting
        self.socket_factory = socket_factory
        self._socket: socket.socket | None = None
        self._reader: _RESPReader | None = None
        self._lock = threading.Lock()
        self._closed = False

    def connect(self) -> None:
        with self._lock:
            self._connect_locked()

    def execute(self, *parts: object) -> object:
        command = _encode_command(
            parts,
            max_frame_bytes=self.max_frame_bytes,
            max_array_items=self.max_array_items,
        )
        with self._lock:
            self._connect_locked()
            assert self._socket is not None and self._reader is not None
            try:
                self._socket.sendall(command)
            except OSError as error:
                self._close_socket_locked()
                raise _TransportFailure(str(error), sent=True) from error
            try:
                return self._reader.read_response()
            except TimeoutError as error:
                self._close_socket_locked()
                raise _TransportFailure(
                    "Redis operation timed out after send", sent=True
                ) from error
            except (OSError, EOFError) as error:
                self._close_socket_locked()
                raise _TransportFailure(str(error), sent=True) from error

    def read_response(self) -> object:
        with self._lock:
            if self._socket is None or self._reader is None:
                raise _TransportFailure("Redis Pub/Sub connection is not open", sent=False)
            try:
                return self._reader.read_response()
            except TimeoutError:
                raise
            except (OSError, EOFError) as error:
                self._close_socket_locked()
                raise _TransportFailure(str(error), sent=False) from error

    def close_socket(self) -> None:
        with self._lock:
            self._close_socket_locked()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._close_socket_locked()

    def _connect_locked(self) -> None:
        if self._closed:
            raise _TransportFailure("Redis connection is closed", sent=False)
        if self._socket is not None:
            return
        try:
            sock = self.socket_factory() if self.socket_factory is not None else self._new_socket()
            sock.settimeout(self.operation_timeout)
            reader = _RESPReader(
                sock,
                max_frame_bytes=self.max_frame_bytes,
                max_array_items=self.max_array_items,
                max_nesting=self.max_nesting,
            )
        except OSError as error:
            raise _TransportFailure(str(error), sent=False) from error
        self._socket = sock
        self._reader = reader
        try:
            if self.endpoint.password is not None:
                if self.endpoint.username is None:
                    self._execute_connected(("AUTH", self.endpoint.password))
                else:
                    self._execute_connected(
                        ("AUTH", self.endpoint.username, self.endpoint.password)
                    )
            elif self.endpoint.username is not None:
                raise _TransportFailure("Redis username requires a password", sent=False)
            if self.endpoint.database:
                self._execute_connected(("SELECT", self.endpoint.database))
        except BaseException:
            self._close_socket_locked()
            raise

    def _execute_connected(self, parts: tuple[object, ...]) -> object:
        assert self._socket is not None and self._reader is not None
        try:
            self._socket.sendall(
                _encode_command(
                    parts,
                    max_frame_bytes=self.max_frame_bytes,
                    max_array_items=self.max_array_items,
                )
            )
            return self._reader.read_response()
        except _RESPError:
            raise
        except TimeoutError as error:
            raise _TransportFailure("Redis setup timed out", sent=False) from error
        except (OSError, EOFError) as error:
            raise _TransportFailure(str(error), sent=False) from error

    def _new_socket(self) -> socket.socket:
        if self.endpoint.path is not None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(self.connect_timeout)
            sock.connect(self.endpoint.path)
            return sock
        assert self.endpoint.host is not None and self.endpoint.port is not None
        return socket.create_connection(
            (self.endpoint.host, self.endpoint.port), timeout=self.connect_timeout
        )

    def _close_socket_locked(self) -> None:
        sock = self._socket
        self._socket = None
        self._reader = None
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(OSError):
                sock.close()


class _RESPReader:
    def __init__(
        self,
        sock: socket.socket,
        *,
        max_frame_bytes: int,
        max_array_items: int,
        max_nesting: int,
    ) -> None:
        self.sock = sock
        self.max_frame_bytes = max_frame_bytes
        self.max_array_items = max_array_items
        self.max_nesting = max_nesting
        self.buffer = bytearray()

    def read_response(self) -> object:
        return self._parse(depth=0, budget=[0])

    def _parse(self, *, depth: int, budget: list[int]) -> object:
        if depth > self.max_nesting:
            raise RedisProtocolError("RESP nesting exceeds configured limit")
        prefix = self._take(1, budget)
        if prefix == b"+":
            return self._line(budget)
        if prefix == b"-":
            raise _RESPError(self._line(budget).decode("utf-8", "replace"))
        if prefix == b":":
            return _signed_integer(self._line(budget), "RESP integer")
        if prefix == b"$":
            length = _signed_integer(self._line(budget), "RESP bulk length")
            if length == -1:
                return None
            if length < 0 or length > self.max_frame_bytes:
                raise RedisProtocolError("RESP bulk length exceeds configured limit")
            value = self._take(length, budget)
            if self._take(2, budget) != b"\r\n":
                raise RedisProtocolError("RESP bulk string is not CRLF terminated")
            return value
        if prefix == b"*":
            count = _signed_integer(self._line(budget), "RESP array length")
            if count == -1:
                return None
            if count < 0 or count > self.max_array_items:
                raise RedisProtocolError("RESP array length exceeds configured limit")
            return [self._parse(depth=depth + 1, budget=budget) for _ in range(count)]
        raise RedisProtocolError(f"unsupported RESP prefix {prefix!r}")

    def _line(self, budget: list[int]) -> bytes:
        while True:
            index = self.buffer.find(b"\r\n")
            if index >= 0:
                value = bytes(self.buffer[:index])
                del self.buffer[: index + 2]
                self._charge(index + 2, budget)
                return value
            if len(self.buffer) >= self.max_frame_bytes:
                raise RedisProtocolError("RESP line exceeds configured limit")
            self._receive()

    def _take(self, length: int, budget: list[int]) -> bytes:
        while len(self.buffer) < length:
            self._receive()
        value = bytes(self.buffer[:length])
        del self.buffer[:length]
        self._charge(length, budget)
        return value

    def _receive(self) -> None:
        remaining = self.max_frame_bytes - len(self.buffer)
        if remaining <= 0:
            raise RedisProtocolError("RESP buffered data exceeds configured limit")
        chunk = self.sock.recv(min(65_536, remaining))
        if not chunk:
            raise EOFError("Redis closed the connection")
        self.buffer.extend(chunk)

    def _charge(self, size: int, budget: list[int]) -> None:
        budget[0] += size
        if budget[0] > self.max_frame_bytes:
            raise RedisProtocolError("RESP response exceeds configured frame limit")


def _encode_command(
    parts: tuple[object, ...], *, max_frame_bytes: int, max_array_items: int
) -> bytes:
    if not parts or len(parts) > max_array_items:
        raise RedisProtocolError("Redis command argument count is invalid")
    encoded = [f"*{len(parts)}\r\n".encode("ascii")]
    size = len(encoded[0])
    for part in parts:
        if isinstance(part, bytes | bytearray | memoryview):
            value = bytes(part)
        elif isinstance(part, str):
            value = part.encode("utf-8")
        elif isinstance(part, int) and not isinstance(part, bool):
            value = str(part).encode("ascii")
        else:
            raise TypeError(f"unsupported Redis command argument: {type(part).__qualname__}")
        item = f"${len(value)}\r\n".encode("ascii") + value + b"\r\n"
        size += len(item)
        if size > max_frame_bytes:
            raise RedisProtocolError("Redis command exceeds configured frame limit")
        encoded.append(item)
    return b"".join(encoded)


def _stream_entries(value: object) -> list[_StreamEntry]:
    if not isinstance(value, list):
        raise RedisProtocolError("XRANGE reply must be an array")
    result: list[_StreamEntry] = []
    for raw in value:
        if not isinstance(raw, list) or len(raw) != 2:
            raise RedisProtocolError("Redis stream entry must contain id and fields")
        stream_id = _text(raw[0])
        revision_text, separator, sequence = stream_id.partition("-")
        if separator != "-" or sequence != "0":
            raise RedisProtocolError(f"unexpected XO Redis stream id {stream_id!r}")
        revision = _nonnegative_integer(revision_text, "stream revision")
        fields_raw = raw[1]
        if not isinstance(fields_raw, list) or len(fields_raw) % 2:
            raise RedisProtocolError("Redis stream fields must be name/value pairs")
        fields: dict[str, bytes] = {}
        for index in range(0, len(fields_raw), 2):
            name = _text(fields_raw[index])
            if name in fields:
                raise RedisProtocolError(f"duplicate Redis stream field {name!r}")
            fields[name] = _bytes(fields_raw[index + 1], name)
        for required in {"b", "r", "i", "e", "h"}:
            if required not in fields:
                raise RedisProtocolError(f"Redis stream entry missing {required!r}")
        if _nonnegative_integer(_field_text(fields, "r"), "field revision") != revision:
            raise RedisProtocolError("Redis stream revision field does not match its id")
        result.append(_StreamEntry(stream_id, revision, fields))
    return result


def _validate_subscribe(value: object, channel: str) -> None:
    if not isinstance(value, list) or len(value) != 3:
        raise RedisProtocolError("Redis SUBSCRIBE acknowledgement is malformed")
    if _text(value[0]) != "subscribe" or _text(value[1]) != channel:
        raise RedisProtocolError("Redis SUBSCRIBE acknowledged the wrong channel")


def _validate_pubsub_message(value: object, channel: str) -> None:
    if not isinstance(value, list) or len(value) != 3:
        raise RedisProtocolError("Redis Pub/Sub message is malformed")
    kind = _text(value[0])
    if kind not in {"message", "subscribe"}:
        raise RedisProtocolError(f"unexpected Redis Pub/Sub message kind {kind!r}")
    if _text(value[1]) != channel:
        raise RedisProtocolError("Redis Pub/Sub message used the wrong namespace channel")


def _first(item: Event | EventGroup) -> Event:
    return item.events[0] if isinstance(item, EventGroup) else item


def _unit_namespace(item: Event | EventGroup) -> str:
    namespace = _first(item).namespace
    if isinstance(item, EventGroup) and any(
        event.namespace != namespace for event in item.events
    ):
        raise RedisBackendError("event group crosses Redis namespaces")
    return namespace


def _identity(item: Event | EventGroup) -> int:
    return item.transaction_id if isinstance(item, EventGroup) else item.event_id


def _reply_parts(value: object, *, minimum: int) -> list[str]:
    if not isinstance(value, list) or len(value) < minimum:
        raise RedisProtocolError("Redis Lua reply is malformed")
    return [_text(part) for part in value]


def _field_text(fields: Mapping[str, bytes], name: str) -> str:
    try:
        return fields[name].decode("utf-8")
    except (KeyError, UnicodeDecodeError) as error:
        raise RedisProtocolError(f"Redis stream field {name!r} is invalid") from error


def _field_bytes(fields: Mapping[str, bytes], name: str) -> bytes:
    try:
        return fields[name]
    except KeyError as error:
        raise RedisProtocolError(f"Redis stream field {name!r} is missing") from error


def _text(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as error:
            raise RedisProtocolError("Redis text is not valid UTF-8") from error
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    raise RedisProtocolError(f"Redis value is not text: {type(value).__qualname__}")


def _bytes(value: object, name: str) -> bytes:
    if isinstance(value, bytes):
        return value
    raise RedisProtocolError(f"Redis {name} must be bytes")


def _signed_integer(value: bytes, name: str) -> int:
    try:
        text = value.decode("ascii")
        if not text or text in {"+", "-"}:
            raise ValueError
        return int(text)
    except (UnicodeDecodeError, ValueError) as error:
        raise RedisProtocolError(f"{name} is invalid") from error


def _nonnegative_integer(value: str, name: str) -> int:
    if not value.isdigit():
        raise RedisProtocolError(f"{name} must be a non-negative integer")
    return int(value)


def _database(value: str) -> int:
    if not value.isdigit():
        raise ValueError("Redis database must be a non-negative integer")
    database = int(value)
    if database > 2**31 - 1:
        raise ValueError("Redis database is out of range")
    return database


def _namespace(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("Redis namespace must be a non-empty string")
    encoded = value.encode("utf-8")
    if len(encoded) > _MAX_NAMESPACE_BYTES:
        raise ValueError(f"Redis namespace exceeds {_MAX_NAMESPACE_BYTES} bytes")
    if any(character in value for character in "{}\x00\r\n"):
        raise ValueError("Redis namespace contains an unsafe key/hash-tag character")
    return value


def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


__all__ = [
    "RedisBackend",
    "RedisBackendError",
    "RedisEndpoint",
    "RedisLimits",
    "RedisProtocolError",
    "RedisUnavailable",
]
