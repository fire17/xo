from __future__ import annotations

import base64
import contextlib
import hashlib
import hmac
import ipaddress
import math
import queue
import secrets
import socket
import struct
import threading
import time
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Final

from ..capabilities import BuildContext, CapabilitySpec, NullCapability, Observer
from ..codec import Codec
from ..events import DerivedEvent, Event, EventGroup, Operation
from ..exceptions import ConflictError, MissingPath, ProtocolError
from ..path import Path, is_prefix, validate_path
from ..wire import (
    PROTOCOL_VERSION,
    SCHEMA_VERSION,
    Envelope,
    commit_envelope,
    decode_envelope,
    derived_envelope,
    encode_envelope,
)

_GUID: Final = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
_ALLOWED_CLIENT_KINDS: Final = frozenset(
    {"hello", "sub", "unsub", "set", "delete", "ack", "ping", "pong"}
)


class WebSocketProtocolError(ProtocolError):
    code = "xo.protocol.malformed"

    def __init__(self, message: str, *, code: str | None = None, close_code: int = 1002) -> None:
        self.code = code or type(self).code
        self.close_code = close_code
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class WebSocketLimits:
    max_frame_bytes: int = 8 * 1024 * 1024
    max_http_bytes: int = 16 * 1024
    max_clients: int = 64
    max_queue: int = 256
    max_log_events: int = 10_000
    max_path_segments: int = 64
    max_segment_bytes: int = 256
    handshake_timeout: float = 5.0
    read_timeout: float = 30.0
    write_timeout: float = 5.0
    close_timeout: float = 2.0

    def __post_init__(self) -> None:
        integers = {
            "max_frame_bytes": self.max_frame_bytes,
            "max_http_bytes": self.max_http_bytes,
            "max_clients": self.max_clients,
            "max_queue": self.max_queue,
            "max_log_events": self.max_log_events,
            "max_path_segments": self.max_path_segments,
            "max_segment_bytes": self.max_segment_bytes,
        }
        for name, value in integers.items():
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("handshake_timeout", "read_timeout", "write_timeout", "close_timeout"):
            value = getattr(self, name)
            if (
                not isinstance(value, int | float)
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be a positive finite duration")


@dataclass(frozen=True, slots=True)
class BrowserWrite:
    operation: Operation
    path: Path
    value: object
    expected_revision: int
    client_origin_id: str


WriteCallback = Callable[[BrowserWrite], Event | EventGroup]


class WebSocketBridge(NullCapability, Observer):
    """Dependency-free, loopback-only RFC 6455 bridge for one XO namespace."""

    def __init__(
        self,
        context: BuildContext,
        *,
        host: str,
        port: int,
        token: str,
        writable_prefixes: tuple[Path, ...],
        write_callback: WriteCallback | None,
        allowed_origins: tuple[str, ...],
        path: str,
        limits: WebSocketLimits,
    ) -> None:
        self.context = context
        self.host = host
        self.port = port
        self.token = token
        self.writable_prefixes = writable_prefixes
        self.write_callback = write_callback
        self.allowed_origins = allowed_origins
        self.path = path
        self.limits = limits
        self._codec: Codec = context.root._root.codec
        self._lock = threading.RLock()
        self._listener: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._clients: set[_Connection] = set()
        self._log: deque[Event | EventGroup] = deque()
        self._seen: set[int] = set()
        self._closed = False
        self._started = False
        self._address: tuple[str, int] | None = None

    @property
    def address(self) -> tuple[str, int]:
        with self._lock:
            if self._address is None:
                raise RuntimeError("WebSocket bridge has not started")
            return self._address

    @property
    def url(self) -> str:
        host, port = self.address
        bracketed = f"[{host}]" if ":" in host else host
        return f"ws://{bracketed}:{port}{self.path}"

    def prepare(self) -> None:
        _validate_loopback(self.host)
        if (
            not isinstance(self.port, int)
            or isinstance(self.port, bool)
            or not 0 <= self.port <= 65535
        ):
            raise ValueError("port must be between 0 and 65535")
        if not self.path.startswith("/") or "\r" in self.path or "\n" in self.path:
            raise ValueError("WebSocket path must be an absolute HTTP path")
        if len(self.token.encode("utf-8")) < 32:
            raise ValueError("WebSocket token must contain at least 32 UTF-8 bytes")
        if self.writable_prefixes and self.write_callback is None:
            self.write_callback = self._write_root

    def start(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("cannot start a closed WebSocket bridge")
            if self._started:
                return
            family = socket.AF_INET6 if ":" in self.host else socket.AF_INET
            listener = socket.socket(family, socket.SOCK_STREAM)
            try:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind((self.host, self.port))
                listener.listen(min(self.limits.max_clients, socket.SOMAXCONN))
                listener.settimeout(min(0.25, self.limits.close_timeout))
            except BaseException:
                listener.close()
                raise
            bound = listener.getsockname()
            self._listener = listener
            self._address = (str(bound[0]), int(bound[1]))
            self._started = True
            thread = threading.Thread(
                target=self._accept_loop,
                name=f"xo-websocket-{self.context.namespace}",
                daemon=True,
            )
            self._accept_thread = thread
            thread.start()

    def observe(self, item: Event | EventGroup) -> None:
        first = _first_event(item)
        if first.namespace != self.context.namespace:
            raise WebSocketProtocolError(
                "observer received another namespace",
                code="xo.protocol.namespace_mismatch",
            )
        identifier = first.event_id
        with self._lock:
            if self._closed or identifier in self._seen:
                return
            if len(self._log) >= self.limits.max_log_events:
                expired = self._log.popleft()
                self._seen.discard(_first_event(expired).event_id)
            self._log.append(item)
            self._seen.add(identifier)
            clients = tuple(self._clients)
        for client in clients:
            client.publish_commit(item)

    def publish_derived(self, event: DerivedEvent) -> None:
        """Publish a projection without applying it to the source tree or replay log."""
        if event.namespace != self.context.namespace:
            raise WebSocketProtocolError(
                "derived event belongs to another namespace",
                code="xo.protocol.namespace_mismatch",
            )
        with self._lock:
            clients = tuple(self._clients)
        for client in clients:
            client.publish_derived(event)

    observe_derived = publish_derived
    def _write_root(self, request: BrowserWrite) -> Event | EventGroup:
        node = self.context.root.at(request.path)
        if request.operation is Operation.SET_VALUE:
            return node.set(request.value, expected_revision=request.expected_revision)
        return node.delete(expected_revision=request.expected_revision)


    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            listener, self._listener = self._listener, None
            clients = tuple(self._clients)
            accept_thread = self._accept_thread
        if listener is not None:
            with contextlib.suppress(OSError):
                listener.close()
        for client in clients:
            client.close()
        deadline = time.monotonic() + self.limits.close_timeout
        if accept_thread is not None and accept_thread is not threading.current_thread():
            accept_thread.join(max(0.0, deadline - time.monotonic()))
        for client in clients:
            client.join(max(0.0, deadline - time.monotonic()))

    def _accept_loop(self) -> None:
        while True:
            with self._lock:
                listener = self._listener
                if self._closed or listener is None:
                    return
            try:
                connection, address = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            try:
                remote = ipaddress.ip_address(str(address[0]))
            except ValueError:
                connection.close()
                continue
            with self._lock:
                unavailable = len(self._clients) >= self.limits.max_clients
                if self._closed or not remote.is_loopback or unavailable:
                    connection.close()
                    continue
                client = _Connection(self, connection)
                self._clients.add(client)
            client.start()

    def _drop(self, client: _Connection) -> None:
        with self._lock:
            self._clients.discard(client)

    def _subscription_plan(
        self,
        client: _Connection,
        prefixes: tuple[Path, ...],
        since_revision: int,
        request_id: int,
    ) -> None:
        root = self.context.root
        # The root lock fences commits while the bridge lock fences observer log insertion.
        # This makes the snapshot/replay watermark and live-subscription seam atomic.
        with root._root.lock, self._lock:
            if self._closed:
                return
            client._prefixes = prefixes
            client._subscribed = True
            snapshot = root.snapshot()
            head = root.revision
            records = tuple(self._log)
            replay = _replay_after(records, since_revision, head)
            if since_revision != 0 and replay is not None:
                for item in replay:
                    client.publish_commit(item)
                client.enqueue(
                    Envelope(
                        "ack",
                        client.next_message_id(),
                        self.context.namespace,
                        {"mode": "catchup", "revision": head, "count": len(replay)},
                        reply_to=request_id,
                    )
                )
                return
            if since_revision not in (0, head):
                client.enqueue_error(
                    "xo.resync_required",
                    "requested revision is no longer contiguous in the replay log",
                    request_id=request_id,
                    retryable=True,
                    detail={"since_revision": since_revision, "head_revision": head},
                )
            projected = dict(snapshot)
            projected["root"] = _project_image(snapshot["root"], prefixes)
            projected["head_revision"] = head
            client.enqueue(
                Envelope(
                    "snapshot",
                    client.next_message_id(),
                    self.context.namespace,
                    projected,
                    reply_to=request_id,
                )
            )
            client.enqueue(
                Envelope(
                    "ack",
                    client.next_message_id(),
                    self.context.namespace,
                    {"mode": "snapshot", "revision": head, "count": 0},
                    reply_to=request_id,
                )
            )


def websocket(
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    token: str | None = None,
    writable: tuple[object, ...] = (),
    write_callback: WriteCallback | None = None,
    allowed_origins: tuple[str, ...] = (),
    path: str = "/xo",
    limits: WebSocketLimits | None = None,
    key: str = "websocket",
) -> CapabilitySpec:
    """Create an inert WebSocket capability; sockets open only when the root starts."""
    _validate_loopback(host)
    if not isinstance(port, int) or isinstance(port, bool) or not 0 <= port <= 65535:
        raise ValueError("port must be between 0 and 65535")
    if not path.startswith("/") or "\r" in path or "\n" in path:
        raise ValueError("WebSocket path must be an absolute HTTP path")
    canonical_writable = tuple(_path_from_wire(value) for value in writable)
    # When no callback is supplied, the bridge writes through the root's canonical
    # mutation pipeline. An explicit callback remains available for custom policy.
    secret = secrets.token_hex(32) if token is None else token
    selected_limits = limits or WebSocketLimits()
    configuration = {
        "host": host,
        "port": port,
        "path": path,
        "writable": canonical_writable,
        "allowed_origins": allowed_origins,
    }
    return CapabilitySpec(
        key=key,
        factory=lambda context: WebSocketBridge(
            context,
            host=host,
            port=port,
            token=secret,
            writable_prefixes=canonical_writable,
            write_callback=write_callback,
            allowed_origins=allowed_origins,
            path=path,
            limits=selected_limits,
        ),
        provides=frozenset({"projection", "websocket"}),
        after=frozenset({"durability", "history"}),
        configuration=configuration,
    )


class _Connection:
    def __init__(self, bridge: WebSocketBridge, sock: socket.socket) -> None:
        self.bridge = bridge
        self.sock = sock
        self._queue: queue.Queue[bytes] = queue.Queue(maxsize=bridge.limits.max_queue)
        self._closed = threading.Event()
        self._send_lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name="xo-websocket-client", daemon=True)
        self._writer: threading.Thread | None = None
        self._upgraded = False
        self._welcomed = False
        self._subscribed = False
        self._prefixes: tuple[Path, ...] = ()
        self._role = "observer"
        self._origin_id = ""
        self._incoming_id = 0
        self._outgoing_id = 0

    def start(self) -> None:
        self._thread.start()

    def join(self, timeout: float) -> None:
        if self._thread is not threading.current_thread():
            self._thread.join(max(0.0, timeout))
        writer = self._writer
        if writer is not None and writer is not threading.current_thread():
            writer.join(max(0.0, timeout))

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        with contextlib.suppress(OSError):
            self.sock.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(OSError):
            self.sock.close()

    def next_message_id(self) -> int:
        self._outgoing_id += 1
        return self._outgoing_id

    def enqueue(self, envelope: Envelope) -> bool:
        if self._closed.is_set():
            return False
        try:
            payload = encode_envelope(envelope, codec=self.bridge._codec)
        except BaseException:
            self.close()
            return False
        if len(payload) > self.bridge.limits.max_frame_bytes:
            self.close()
            return False
        try:
            self._queue.put_nowait(payload)
        except queue.Full:
            self.close()
            return False
        return True

    def enqueue_error(
        self,
        code: str,
        message: str,
        *,
        request_id: int | None = None,
        retryable: bool = False,
        detail: Mapping[str, object] | None = None,
    ) -> None:
        self.enqueue(
            Envelope(
                "error",
                self.next_message_id(),
                self.bridge.context.namespace,
                {
                    "code": code,
                    "message": message,
                    "retryable": retryable,
                    "detail": dict(detail or {}),
                },
                reply_to=request_id,
            )
        )

    def publish_commit(self, item: Event | EventGroup) -> None:
        if not self._welcomed or not self._subscribed or self._closed.is_set():
            return
        event = _last_event(item)
        if _item_matches(item, self._prefixes):
            self.enqueue(commit_envelope(item, message_id=self.next_message_id()))
        else:
            self.enqueue(
                Envelope(
                    "ack",
                    self.next_message_id(),
                    self.bridge.context.namespace,
                    {"mode": "watermark", "revision": event.revision},
                )
            )

    def publish_derived(self, event: DerivedEvent) -> None:
        if (
            self._welcomed
            and self._subscribed
            and not self._closed.is_set()
            and any(is_prefix(prefix, event.path) for prefix in self._prefixes)
        ):
            self.enqueue(derived_envelope(event, message_id=self.next_message_id()))

    def _run(self) -> None:
        try:
            self.sock.settimeout(self.bridge.limits.handshake_timeout)
            self._upgrade()
            self._upgraded = True
            self.sock.settimeout(self.bridge.limits.write_timeout)
            self._writer = threading.Thread(
                target=self._write_loop,
                name="xo-websocket-writer",
                daemon=True,
            )
            self._writer.start()
            self.sock.settimeout(self.bridge.limits.read_timeout)
            self._session()
        except WebSocketProtocolError as error:
            if self._upgraded and not self._closed.is_set():
                self._fail(error)
        except (EOFError, OSError, TimeoutError):
            pass
        except BaseException as error:
            if self._upgraded and not self._closed.is_set():
                failure = WebSocketProtocolError(
                    f"internal bridge failure: {type(error).__name__}"
                )
                self._fail(failure)
        finally:
            self.close()
            self.bridge._drop(self)

    def _upgrade(self) -> None:
        request = bytearray()
        while not request.endswith(b"\r\n\r\n"):
            if len(request) >= self.bridge.limits.max_http_bytes:
                raise WebSocketProtocolError("HTTP upgrade headers exceed limit")
            byte = self.sock.recv(1)
            if not byte:
                raise EOFError
            request.extend(byte)
        try:
            lines = request.decode("iso-8859-1").split("\r\n")
            method, target, version = lines[0].split(" ", 2)
        except (UnicodeDecodeError, ValueError) as error:
            raise WebSocketProtocolError("malformed HTTP upgrade request") from error
        if method != "GET" or target != self.bridge.path or version != "HTTP/1.1":
            raise WebSocketProtocolError("invalid WebSocket upgrade target")
        headers: dict[str, str] = {}
        for line in lines[1:]:
            if not line:
                continue
            if ":" not in line:
                raise WebSocketProtocolError("malformed HTTP header")
            name, value = line.split(":", 1)
            lowered = name.strip().lower()
            if lowered in headers:
                raise WebSocketProtocolError("duplicate HTTP upgrade header")
            headers[lowered] = value.strip()
        values = headers.get("connection", "").split(",")
        connection_tokens = {value.strip().lower() for value in values}
        if (
            headers.get("upgrade", "").lower() != "websocket"
            or "upgrade" not in connection_tokens
        ):
            raise WebSocketProtocolError("request is not a WebSocket upgrade")
        if headers.get("sec-websocket-version") != "13":
            raise WebSocketProtocolError("unsupported WebSocket version")
        key = headers.get("sec-websocket-key", "")
        try:
            decoded_key = base64.b64decode(key, validate=True)
        except ValueError as error:
            raise WebSocketProtocolError("invalid WebSocket key") from error
        if len(decoded_key) != 16:
            raise WebSocketProtocolError("invalid WebSocket key length")
        origin = headers.get("origin")
        if self.bridge.allowed_origins and origin not in self.bridge.allowed_origins:
            raise WebSocketProtocolError(
                "WebSocket origin is not allowed",
                code="xo.auth.invalid",
                close_code=1008,
            )
        digest = hashlib.sha1((key + _GUID).encode("ascii")).digest()
        accept = base64.b64encode(digest).decode("ascii")
        response = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n"
            "\r\n"
        ).encode("ascii")
        self.sock.sendall(response)

    def _session(self) -> None:
        first = self._receive_message()
        envelope = self._decode(first)
        if envelope.kind != "hello":
            raise WebSocketProtocolError("first message must be hello")
        self._hello(envelope)
        while not self._closed.is_set():
            data = self._receive_message()
            envelope = self._decode(data)
            self._handle(envelope)

    def _decode(self, data: bytes) -> Envelope:
        try:
            envelope = decode_envelope(data, codec=self.bridge._codec)
        except BaseException as error:
            raise WebSocketProtocolError(f"invalid XO envelope: {error}") from error
        if envelope.namespace != self.bridge.context.namespace:
            raise WebSocketProtocolError(
                "envelope namespace does not match this bridge",
                code="xo.protocol.namespace_mismatch",
                close_code=1008,
            )
        if envelope.message_id <= self._incoming_id:
            raise WebSocketProtocolError("message ids must increase monotonically")
        self._incoming_id = envelope.message_id
        if envelope.kind not in _ALLOWED_CLIENT_KINDS:
            raise WebSocketProtocolError(f"unsupported client message kind: {envelope.kind!r}")
        return envelope

    def _hello(self, envelope: Envelope) -> None:
        payload = _mapping(envelope.payload, "hello payload")
        protocol = _nonnegative_int(payload.get("protocol"), "protocol")
        schema = _nonnegative_int(payload.get("schema"), "schema")
        if protocol != PROTOCOL_VERSION or schema != SCHEMA_VERSION:
            raise WebSocketProtocolError(
                "unsupported XO protocol or schema",
                code="xo.protocol.version",
                close_code=1002,
            )
        token = payload.get("token")
        if not isinstance(token, str) or not hmac.compare_digest(token, self.bridge.token):
            raise WebSocketProtocolError(
                "invalid WebSocket token", code="xo.auth.invalid", close_code=1008
            )
        role = payload.get("role", "observer")
        if role not in ("observer", "writer"):
            raise WebSocketProtocolError(
                "invalid WebSocket role", code="xo.auth.invalid", close_code=1008
            )
        if role == "writer" and (
            not self.bridge.writable_prefixes or self.bridge.write_callback is None
        ):
            raise WebSocketProtocolError(
                "browser writes are disabled", code="xo.auth.invalid", close_code=1008
            )
        origin_id = payload.get("origin_id")
        if not isinstance(origin_id, str) or not origin_id or any(
            character not in "0123456789abcdefABCDEF" for character in origin_id
        ):
            raise WebSocketProtocolError("origin_id must be a hexadecimal string")
        self._role = role
        self._origin_id = origin_id
        self._welcomed = True
        self.enqueue(
            Envelope(
                "welcome",
                self.next_message_id(),
                self.bridge.context.namespace,
                {
                    "protocol": PROTOCOL_VERSION,
                    "schema": SCHEMA_VERSION,
                    "origin_id": format(self.bridge.context.root.origin_id, "x"),
                    "head_revision": self.bridge.context.root.revision,
                    "limits": {
                        "max_frame_bytes": self.bridge.limits.max_frame_bytes,
                        "max_inflight": self.bridge.limits.max_queue,
                        "max_stream_queue": self.bridge.limits.max_queue,
                        "max_path_segments": self.bridge.limits.max_path_segments,
                    },
                },
                reply_to=envelope.message_id,
            )
        )

    def _handle(self, envelope: Envelope) -> None:
        if envelope.deadline is not None and envelope.deadline < time.time():
            self.enqueue_error(
                "xo.deadline_exceeded",
                "request deadline has passed",
                request_id=envelope.message_id,
            )
            return
        if envelope.kind == "sub":
            payload = _mapping(envelope.payload, "subscription")
            prefixes_value = payload.get("prefixes", [[]])
            if not isinstance(prefixes_value, list) or not prefixes_value:
                raise WebSocketProtocolError("subscription prefixes must be a non-empty list")
            prefixes = tuple(self._validated_path(value) for value in prefixes_value)
            since = _nonnegative_int(payload.get("since_revision", 0), "since_revision")
            materialize = payload.get("materialize", [])
            if not isinstance(materialize, list):
                raise WebSocketProtocolError("materialize must be a list")
            if materialize:
                self.enqueue_error(
                    "xo.formula.not_materialized",
                    "this bridge accepts derived projections only from publish_derived",
                    request_id=envelope.message_id,
                )
            self.bridge._subscription_plan(self, prefixes, since, envelope.message_id)
            return
        if envelope.kind == "unsub":
            self._subscribed = False
            self._prefixes = ()
            self.enqueue(
                Envelope(
                    "ack",
                    self.next_message_id(),
                    self.bridge.context.namespace,
                    {"mode": "unsubscribed", "revision": self.bridge.context.root.revision},
                    reply_to=envelope.message_id,
                )
            )
            return
        if envelope.kind in ("set", "delete"):
            self._write(envelope)
            return
        if envelope.kind == "ping":
            self.enqueue(
                Envelope(
                    "pong",
                    self.next_message_id(),
                    self.bridge.context.namespace,
                    envelope.payload,
                    reply_to=envelope.message_id,
                )
            )
            return
        if envelope.kind in ("ack", "pong"):
            return
        raise WebSocketProtocolError(f"message kind is invalid after hello: {envelope.kind!r}")

    def _write(self, envelope: Envelope) -> None:
        if self._role != "writer" or self.bridge.write_callback is None:
            raise WebSocketProtocolError(
                "browser is not authorized to write",
                code="xo.auth.invalid",
                close_code=1008,
            )
        payload = _mapping(envelope.payload, "write request")
        path = self._validated_path(payload.get("path"))
        if not any(is_prefix(prefix, path) for prefix in self.bridge.writable_prefixes):
            raise WebSocketProtocolError(
                "path is outside writable prefixes",
                code="xo.auth.invalid",
                close_code=1008,
            )
        expected = _nonnegative_int(payload.get("expected_revision"), "expected_revision")
        if envelope.kind == "set":
            if "value" not in payload:
                raise WebSocketProtocolError("set request is missing value")
            operation = Operation.SET_VALUE
            value = payload["value"]
            try:
                self.bridge._codec.dumps(value)
            except BaseException as error:
                self.enqueue_error(
                    "xo.codec.unsupported_type",
                    str(error),
                    request_id=envelope.message_id,
                )
                return
        else:
            operation = Operation.DELETE_SUBTREE
            value = None
        request = BrowserWrite(operation, path, value, expected, self._origin_id)
        try:
            result = self.bridge.write_callback(request)
            if not isinstance(result, Event | EventGroup):
                raise TypeError("write callback must return Event or EventGroup")
            first = _first_event(result)
            if first.namespace != self.bridge.context.namespace:
                raise TypeError("write callback returned another namespace")
        except ConflictError as error:
            self.enqueue_error(
                "xo.conflict",
                str(error),
                request_id=envelope.message_id,
                retryable=True,
                detail={
                    "expected_revision": expected,
                    "head_revision": self.bridge.context.root.revision,
                },
            )
            return
        except MissingPath as error:
            self.enqueue_error("xo.not_found", str(error), request_id=envelope.message_id)
            return
        except BaseException as error:
            self.enqueue_error(
                "xo.internal",
                f"{type(error).__name__}: {error}",
                request_id=envelope.message_id,
            )
            return
        self.enqueue(
            Envelope(
                "ack",
                self.next_message_id(),
                self.bridge.context.namespace,
                {
                    "mode": "write",
                    "revision": first.revision,
                    "event_id": format(first.event_id, "x"),
                },
                reply_to=envelope.message_id,
            )
        )

    def _validated_path(self, value: object) -> Path:
        path = _path_from_wire(value)
        if len(path) > self.bridge.limits.max_path_segments:
            raise WebSocketProtocolError(
                "path contains too many segments", code="xo.path.invalid", close_code=1008
            )
        oversized = any(
            len(segment.encode("utf-8")) > self.bridge.limits.max_segment_bytes
            for segment in path
        )
        if oversized:
            raise WebSocketProtocolError(
                "path segment exceeds byte limit", code="xo.path.invalid", close_code=1008
            )
        return path

    def _receive_message(self) -> bytes:
        while True:
            fin, opcode, payload = self._read_frame()
            if opcode == 0x8:
                if len(payload) == 1:
                    raise WebSocketProtocolError("malformed close frame")
                return self._peer_closed(payload)
            if opcode == 0x9:
                self._send_control(0xA, payload)
                continue
            if opcode == 0xA:
                continue
            if opcode != 0x1 or not fin:
                raise WebSocketProtocolError("only complete text messages are accepted")
            try:
                payload.decode("utf-8")
            except UnicodeDecodeError as error:
                raise WebSocketProtocolError("WebSocket text is not valid UTF-8") from error
            return payload

    def _peer_closed(self, payload: bytes) -> bytes:
        if not self._closed.is_set():
            self._send_control(0x8, payload[:125])
        raise EOFError

    def _read_frame(self) -> tuple[bool, int, bytes]:
        first, second = _recv_exact(self.sock, 2)
        fin = bool(first & 0x80)
        if first & 0x70:
            raise WebSocketProtocolError("reserved WebSocket bits are not supported")
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        if not masked:
            raise WebSocketProtocolError("client WebSocket frames must be masked")
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", _recv_exact(self.sock, 2))[0]
            if length < 126:
                raise WebSocketProtocolError("non-minimal WebSocket frame length")
        elif length == 127:
            raw = _recv_exact(self.sock, 8)
            if raw[0] & 0x80:
                raise WebSocketProtocolError("invalid 64-bit WebSocket frame length")
            length = struct.unpack("!Q", raw)[0]
            if length < 65536:
                raise WebSocketProtocolError("non-minimal WebSocket frame length")
        if opcode >= 0x8 and (not fin or length > 125):
            raise WebSocketProtocolError("invalid WebSocket control frame")
        if opcode not in (0x1, 0x8, 0x9, 0xA):
            raise WebSocketProtocolError("unsupported WebSocket opcode")
        if length > self.bridge.limits.max_frame_bytes:
            raise WebSocketProtocolError(
                "WebSocket frame exceeds configured limit",
                code="xo.protocol.frame_too_large",
                close_code=1009,
            )
        mask = _recv_exact(self.sock, 4)
        payload = bytearray(_recv_exact(self.sock, length))
        for index in range(length):
            payload[index] ^= mask[index & 3]
        return fin, opcode, bytes(payload)

    def _write_loop(self) -> None:
        try:
            while not self._closed.is_set():
                try:
                    timeout = min(0.25, self.bridge.limits.close_timeout)
                    payload = self._queue.get(timeout=timeout)
                except queue.Empty:
                    continue
                self._send_data(payload)
        except (OSError, TimeoutError):
            self.close()

    def _send_data(self, payload: bytes) -> None:
        header = _frame_header(0x1, len(payload))
        with self._send_lock:
            self.sock.settimeout(self.bridge.limits.write_timeout)
            self.sock.sendall(header + payload)

    def _send_control(self, opcode: int, payload: bytes) -> None:
        if len(payload) > 125:
            payload = payload[:125]
        with self._send_lock:
            self.sock.settimeout(self.bridge.limits.write_timeout)
            self.sock.sendall(_frame_header(opcode, len(payload)) + payload)

    def _fail(self, error: WebSocketProtocolError) -> None:
        try:
            payload = encode_envelope(
                Envelope(
                    "error",
                    self.next_message_id(),
                    self.bridge.context.namespace,
                    {
                        "code": error.code,
                        "message": str(error),
                        "retryable": False,
                        "detail": {},
                    },
                ),
                codec=self.bridge._codec,
            )
            if len(payload) <= self.bridge.limits.max_frame_bytes:
                self._send_data(payload)
            reason = error.code.encode("utf-8")[:123]
            self._send_control(0x8, struct.pack("!H", error.close_code) + reason)
        except (OSError, TimeoutError):
            return


def _validate_loopback(host: str) -> None:
    try:
        address = ipaddress.ip_address(host)
    except ValueError as error:
        raise ValueError("WebSocket host must be a literal loopback address") from error
    if not address.is_loopback:
        raise ValueError("WebSocket bridge refuses non-loopback binds")


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError
        data.extend(chunk)
    return bytes(data)


def _frame_header(opcode: int, length: int) -> bytes:
    first = 0x80 | opcode
    if length < 126:
        return bytes((first, length))
    if length <= 0xFFFF:
        return bytes((first, 126)) + struct.pack("!H", length)
    return bytes((first, 127)) + struct.pack("!Q", length)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise WebSocketProtocolError(f"{name} must be an object with string keys")
    return value


def _nonnegative_int(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise WebSocketProtocolError(f"{name} must be a non-negative integer")
    return value


def _path_from_wire(value: object) -> Path:
    if not isinstance(value, list | tuple) or not all(
        isinstance(segment, str) for segment in value
    ):
        raise WebSocketProtocolError(
            "path must be a list of strings", code="xo.path.invalid", close_code=1008
        )
    try:
        return validate_path(tuple(value))
    except ValueError as error:
        raise WebSocketProtocolError(str(error), code="xo.path.invalid", close_code=1008) from error


def _first_event(item: Event | EventGroup) -> Event:
    return item.events[0] if isinstance(item, EventGroup) else item

def _last_event(item: Event | EventGroup) -> Event:
    return item.events[-1] if isinstance(item, EventGroup) else item


def _item_matches(item: Event | EventGroup, prefixes: tuple[Path, ...]) -> bool:
    events = item.events if isinstance(item, EventGroup) else (item,)
    return any(is_prefix(prefix, event.path) for prefix in prefixes for event in events)


def _replay_after(
    records: tuple[Event | EventGroup, ...],
    since_revision: int,
    head_revision: int,
) -> tuple[Event | EventGroup, ...] | None:
    if since_revision > head_revision:
        return None
    if since_revision == head_revision:
        return ()
    selected = tuple(item for item in records if _last_event(item).revision > since_revision)
    expected = since_revision + 1
    for item in selected:
        events = item.events if isinstance(item, EventGroup) else (item,)
        first = events[0]
        last = events[-1]
        if first.revision != expected or last.revision < first.revision:
            return None
        if any(event.revision != first.revision for event in events):
            return None
        expected = last.revision + 1
    return selected if expected - 1 == head_revision else None


def _project_image(image: object, prefixes: tuple[Path, ...]) -> dict[str, object]:
    def visit(value: object, path: Path, unrestricted: bool) -> dict[str, object] | None:
        node = _mapping(value, "snapshot node")
        raw_children = node.get("$children")
        if not isinstance(raw_children, list):
            raise WebSocketProtocolError("snapshot node children must be a list")
        inside = unrestricted or any(is_prefix(prefix, path) for prefix in prefixes)
        ancestor = inside or any(is_prefix(path, prefix) for prefix in prefixes)
        if not ancestor:
            return None
        result: dict[str, object] = {"$children": []}
        if inside and "$value" in node:
            result["$value"] = node["$value"]
        children: list[list[object]] = []
        for entry in raw_children:
            if not isinstance(entry, list) or len(entry) != 2 or not isinstance(entry[0], str):
                raise WebSocketProtocolError("invalid snapshot child")
            child_path = (*path, entry[0])
            child = visit(entry[1], child_path, inside)
            if child is not None:
                children.append([entry[0], child])
        result["$children"] = children
        return result

    projected = visit(image, (), False)
    return {"$children": []} if projected is None else projected
