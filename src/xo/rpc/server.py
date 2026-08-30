from __future__ import annotations

import contextlib
import errno
import os
import socket
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path as FilePath

from ..capabilities import BuildContext, CapabilitySpec
from ..exceptions import BackpressureError, DeadlineExceeded, ProtocolError
from ..service import ServiceRegistry
from ..wire import Envelope
from .protocol import (
    DEFAULT_CREDIT,
    DEFAULT_MAX_FRAME_BYTES,
    DEFAULT_MAX_INFLIGHT,
    DEFAULT_MAX_STREAM_QUEUE,
    DEFAULT_TIMEOUT,
    MalformedMessage,
    MessageIds,
    NamespaceMismatch,
    bounded_credit,
    make_codec,
    parse_endpoint,
    positive_finite,
    positive_integer,
    recv_frame,
    send_frame,
    wire_path,
)


@dataclass(slots=True)
class _StreamState:
    credit: int
    condition: threading.Condition = field(default_factory=threading.Condition)
    cancelled: bool = False

    def add_credit(self, amount: int, maximum: int) -> None:
        with self.condition:
            if self.cancelled:
                return
            if self.credit + amount > maximum:
                raise BackpressureError(f"stream credit exceeds bound ({maximum})")
            self.credit += amount
            self.condition.notify()

    def cancel(self) -> None:
        with self.condition:
            self.cancelled = True
            self.condition.notify_all()

    def take(self, deadline: float | None) -> bool:
        with self.condition:
            while self.credit == 0 and not self.cancelled:
                remaining = None if deadline is None else deadline - time.time()
                if remaining is not None and remaining <= 0:
                    raise DeadlineExceeded("RPC stream deadline exceeded")
                self.condition.wait(remaining)
            if self.cancelled:
                return False
            self.credit -= 1
            return True


class RPCServer:
    """Root-scoped RPC capability serving the shared Service registry."""

    __slots__ = ("context", "server")

    def __init__(
        self,
        context: BuildContext,
        address: str | tuple[str, int],
        **options: object,
    ) -> None:
        self.context = context
        services = context.services
        service_capability = services["service"]
        registry = service_capability.registry
        namespace = context.namespace
        self.server = Server(registry, address, namespace=namespace, **options)

    @property
    def address(self) -> str | tuple[str, int]:
        return self.server.address

    @property
    def active_connections(self) -> int:
        return self.server.active_connections

    def prepare(self) -> None:
        pass

    def start(self) -> None:
        self.server.start()

    def close(self) -> None:
        self.server.close()


def rpc_server(
    address: str | tuple[str, int],
    *,
    key: str = "rpc",
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    max_inflight: int = DEFAULT_MAX_INFLIGHT,
    default_credit: int = DEFAULT_CREDIT,
    max_stream_queue: int = DEFAULT_MAX_STREAM_QUEUE,
    io_timeout: float = DEFAULT_TIMEOUT,
    close_timeout: float = 2.0,
) -> CapabilitySpec:
    """Create an inert RPC capability sharing the root service registry."""

    endpoint = parse_endpoint(address)
    options = {
        "max_frame_bytes": max_frame_bytes,
        "max_inflight": max_inflight,
        "default_credit": default_credit,
        "max_stream_queue": max_stream_queue,
        "io_timeout": io_timeout,
        "close_timeout": close_timeout,
    }
    return CapabilitySpec(
        key=key,
        factory=lambda context: RPCServer(context, address, **options),
        provides=frozenset({"rpc"}),
        requires=frozenset({"service"}),
        after=frozenset({"service"}),
        configuration={
            "address": address,
            **options,
            "family": "unix" if endpoint.is_unix else "tcp",
        },
    )


class Server:
    """Bounded stdlib RPC server dispatching only through a ServiceRegistry."""

    __slots__ = (
        "_accept_thread",
        "_closing",
        "_codec",
        "_connections",
        "_listener",
        "_lock",
        "_owns_unix_path",
        "close_timeout",
        "default_credit",
        "endpoint",
        "io_timeout",
        "max_frame_bytes",
        "max_inflight",
        "max_stream_queue",
        "namespace",
        "registry",
    )

    def __init__(
        self,
        registry: ServiceRegistry,
        address: str | tuple[str, int],
        *,
        namespace: str = "default",
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        max_inflight: int = DEFAULT_MAX_INFLIGHT,
        default_credit: int = DEFAULT_CREDIT,
        max_stream_queue: int = DEFAULT_MAX_STREAM_QUEUE,
        io_timeout: float = DEFAULT_TIMEOUT,
        close_timeout: float = 2.0,
    ) -> None:
        if not isinstance(registry, ServiceRegistry):
            raise TypeError("Server registry must be a ServiceRegistry")
        if not namespace or "\x00" in namespace:
            raise ValueError("namespace must be a non-empty, NUL-free string")
        positive_integer(max_inflight, "max_inflight")
        positive_integer(max_stream_queue, "max_stream_queue")
        positive_integer(default_credit, "default_credit")
        if default_credit > max_stream_queue:
            raise ValueError("default_credit must fit max_stream_queue")
        positive_finite(io_timeout, "io_timeout")
        positive_finite(close_timeout, "close_timeout")
        self.registry = registry
        self.namespace = namespace
        self.endpoint = parse_endpoint(address)
        self.max_frame_bytes = max_frame_bytes
        self.max_inflight = max_inflight
        self.default_credit = default_credit
        self.max_stream_queue = max_stream_queue
        self.io_timeout = io_timeout
        self.close_timeout = close_timeout
        self._codec = make_codec(max_frame_bytes)
        self._listener: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._connections: set[_Connection] = set()
        self._lock = threading.Lock()
        self._closing = threading.Event()
        self._owns_unix_path = False

    @property
    def address(self) -> str | tuple[str, int]:
        listener = self._listener
        if listener is None:
            return self.endpoint.address
        return listener.getsockname()

    @property
    def active_connections(self) -> int:
        with self._lock:
            return len(self._connections)

    def start(self) -> Server:
        with self._lock:
            if self._listener is not None:
                return self
            if self._closing.is_set():
                raise RuntimeError("closed RPC server cannot be restarted")
            listener = socket.socket(self.endpoint.family, socket.SOCK_STREAM)
            listener.settimeout(0.2)
            try:
                if self.endpoint.is_unix:
                    path = str(self.endpoint.address)
                    parent = FilePath(path).parent
                    if not parent.exists():
                        raise FileNotFoundError(f"Unix socket directory does not exist: {parent}")
                    listener.bind(path)
                    os.chmod(path, 0o600)
                    self._owns_unix_path = True
                else:
                    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    listener.bind(self.endpoint.address)
                listener.listen(min(self.max_inflight, socket.SOMAXCONN))
            except BaseException:
                listener.close()
                raise
            self._listener = listener
            thread = threading.Thread(target=self._accept_loop, name="xo-rpc-accept")
            self._accept_thread = thread
            thread.start()
        return self

    def serve_forever(self) -> None:
        self.start()
        thread = self._accept_thread
        if thread is not None:
            thread.join()

    def close(self) -> None:
        self._closing.set()
        with self._lock:
            listener, self._listener = self._listener, None
            connections = tuple(self._connections)
        if listener is not None:
            listener.close()
        for connection in connections:
            connection.close()
        deadline = time.monotonic() + self.close_timeout
        thread = self._accept_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(max(0.0, deadline - time.monotonic()))
        for connection in connections:
            connection.join(max(0.0, deadline - time.monotonic()))
        self._remove_unix_path()

    def __enter__(self) -> Server:
        return self.start()

    def __exit__(self, *_: object) -> None:
        self.close()

    def _accept_loop(self) -> None:
        while not self._closing.is_set():
            listener = self._listener
            if listener is None:
                return
            try:
                sock, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError as error:
                if self._closing.is_set() or error.errno in {errno.EBADF, errno.EINVAL}:
                    return
                continue
            sock.settimeout(self.io_timeout)
            connection = _Connection(self, sock)
            with self._lock:
                if self._closing.is_set():
                    sock.close()
                    return
                self._connections.add(connection)
            connection.start()

    def _retire(self, connection: _Connection) -> None:
        with self._lock:
            self._connections.discard(connection)

    def _remove_unix_path(self) -> None:
        if not self._owns_unix_path:
            return
        self._owns_unix_path = False
        path = str(self.endpoint.address)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)


class _Connection:
    __slots__ = (
        "closing",
        "ids",
        "inflight",
        "last_mid",
        "send_lock",
        "server",
        "sock",
        "state_lock",
        "streams",
        "thread",
        "workers",
    )

    def __init__(self, server: Server, sock: socket.socket) -> None:
        self.server = server
        self.sock = sock
        self.ids = MessageIds()
        self.thread = threading.Thread(target=self._run, name="xo-rpc-connection")
        self.send_lock = threading.Lock()
        self.state_lock = threading.Lock()
        self.inflight = threading.BoundedSemaphore(server.max_inflight)
        self.streams: dict[int, _StreamState] = {}
        self.workers: set[threading.Thread] = set()
        self.closing = threading.Event()
        self.last_mid = 0

    def start(self) -> None:
        self.thread.start()

    def join(self, timeout: float) -> None:
        if self.thread is not threading.current_thread():
            self.thread.join(timeout)

    def close(self) -> None:
        if self.closing.is_set():
            return
        self.closing.set()
        with self.state_lock:
            streams = tuple(self.streams.values())
        for stream in streams:
            stream.cancel()
        with contextlib.suppress(OSError):
            self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()

    def _run(self) -> None:
        try:
            while not self.closing.is_set():
                message = recv_frame(
                    self.sock,
                    codec=self.server._codec,
                    max_frame_bytes=self.server.max_frame_bytes,
                )
                if message.namespace != self.server.namespace:
                    raise NamespaceMismatch(
                        f"RPC namespace {message.namespace!r} does not match "
                        f"{self.server.namespace!r}"
                    )
                if message.message_id <= self.last_mid:
                    raise MalformedMessage("RPC message IDs must increase monotonically")
                self.last_mid = message.message_id
                self._dispatch(message)
        except (EOFError, OSError, TimeoutError, ProtocolError):
            pass
        finally:
            self.close()
            with self.state_lock:
                workers = tuple(self.workers)
            deadline = time.monotonic() + self.server.close_timeout
            for worker in workers:
                if worker is not threading.current_thread():
                    worker.join(max(0.0, deadline - time.monotonic()))
            self.server._retire(self)

    def _dispatch(self, message: Envelope) -> None:
        if message.kind == "credit":
            self._credit(message)
            return
        if message.kind == "cancel":
            self._cancel(message)
            return
        if message.kind not in {"ping", "describe", "call"}:
            raise MalformedMessage(f"client cannot send {message.kind!r}")
        if not self.inflight.acquire(blocking=False):
            self._error(
                message.message_id,
                "xo.limit.concurrency",
                f"at most {self.server.max_inflight} requests may be in flight",
                retryable=True,
                detail={"limit": self.server.max_inflight},
            )
            return
        worker = threading.Thread(
            target=self._handle,
            args=(message,),
            name=f"xo-rpc-call-{message.message_id}",
        )
        with self.state_lock:
            self.workers.add(worker)
        worker.start()

    def _handle(self, message: Envelope) -> None:
        try:
            self._check_deadline(message.deadline)
            if message.kind == "ping":
                self._send("pong", message.message_id, dict(message.payload))
            elif message.kind == "describe":
                paths = [list(path) for path in sorted(self.server.registry.functions)]
                self._send("result", message.message_id, {"value": paths})
            else:
                self._call(message)
        except DeadlineExceeded as error:
            self._error(message.message_id, error.code, str(error))
        except BaseException as error:
            code = getattr(error, "code", "xo.internal")
            self._error(message.message_id, code, f"{type(error).__name__}: {error}")
        finally:
            self.inflight.release()
            with self.state_lock:
                self.workers.discard(threading.current_thread())

    def _call(self, message: Envelope) -> None:
        payload = message.payload
        assert isinstance(payload, dict)
        path = wire_path(payload["path"])
        args = payload["args"]
        kwargs = payload["kwargs"]
        assert isinstance(args, list) and isinstance(kwargs, dict)
        result = self.server.registry.call(path, *args, **kwargs)
        if not isinstance(result, Iterator):
            self._send("result", message.message_id, {"value": result})
            return
        initial_credit = bounded_credit(payload.get("credit", self.server.default_credit))
        if initial_credit > self.server.max_stream_queue:
            raise BackpressureError(
                f"initial stream credit exceeds bound ({self.server.max_stream_queue})"
            )
        state = _StreamState(initial_credit)
        with self.state_lock:
            self.streams[message.message_id] = state
        self._send("start", message.message_id, {"streaming": True})
        self._stream(message, result, state)

    def _stream(self, message: Envelope, iterator: Iterator[object], state: _StreamState) -> None:
        count = 0
        reason = "complete"
        failure: BaseException | None = None
        try:
            while True:
                self._check_deadline(message.deadline)
                if not state.take(message.deadline):
                    reason = "cancelled"
                    break
                try:
                    value = next(iterator)
                except StopIteration:
                    break
                if state.cancelled:
                    reason = "cancelled"
                    break
                self._send("chunk", message.message_id, {"seq": count, "value": value})
                count += 1
        except DeadlineExceeded as error:
            reason, failure = "deadline_exceeded", error
        except BaseException as error:
            reason, failure = "error", error
        finally:
            try:
                close = getattr(iterator, "close", None)
                if close is not None:
                    close()
            except BaseException as error:
                reason, failure = "error", failure or error
            with self.state_lock:
                self.streams.pop(message.message_id, None)
        if failure is not None:
            code = getattr(failure, "code", "xo.internal")
            self._error(message.message_id, code, f"{type(failure).__name__}: {failure}")
        self._send("end", message.message_id, {"reason": reason, "count": count})

    def _credit(self, message: Envelope) -> None:
        assert message.reply_to is not None and isinstance(message.payload, dict)
        amount = bounded_credit(message.payload["credit"])
        with self.state_lock:
            state = self.streams.get(message.reply_to)
        if state is None:
            return
        try:
            state.add_credit(amount, self.server.max_stream_queue)
        except BackpressureError as error:
            state.cancel()
            self._error(message.reply_to, error.code, str(error), retryable=True)

    def _cancel(self, message: Envelope) -> None:
        assert message.reply_to is not None
        with self.state_lock:
            state = self.streams.get(message.reply_to)
        if state is not None:
            state.cancel()

    def _check_deadline(self, deadline: float | None) -> None:
        if deadline is not None and time.time() >= deadline:
            raise DeadlineExceeded("RPC deadline exceeded")

    def _send(self, kind: str, rid: int, payload: dict[str, object]) -> None:
        if self.closing.is_set():
            return
        message = Envelope(kind, self.ids.take(), self.server.namespace, payload, reply_to=rid)
        with self.send_lock:
            send_frame(
                self.sock,
                message,
                codec=self.server._codec,
                max_frame_bytes=self.server.max_frame_bytes,
            )

    def _error(
        self,
        rid: int,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        detail: dict[str, object] | None = None,
    ) -> None:
        self._send(
            "error",
            rid,
            {
                "code": code,
                "message": message,
                "retryable": retryable,
                "detail": detail or {},
            },
        )
