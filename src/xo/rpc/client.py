from __future__ import annotations

import contextlib
import queue
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Self

from ..exceptions import (
    BackpressureError,
    CancelledError,
    ClosedError,
    DeadlineExceeded,
    ProtocolError,
)
from ..path import Path, parse_path
from ..service import ServiceNotFound
from ..wire import Envelope
from .protocol import (
    DEFAULT_CREDIT,
    DEFAULT_MAX_FRAME_BYTES,
    DEFAULT_MAX_STREAM_QUEUE,
    DEFAULT_TIMEOUT,
    ConcurrencyLimitError,
    ConnectionLost,
    MalformedMessage,
    MessageIds,
    NamespaceMismatch,
    RemoteError,
    RemoteInternalError,
    make_codec,
    parse_endpoint,
    path_payload,
    recv_frame,
    send_frame,
)


@dataclass(slots=True)
class _Pending:
    messages: queue.Queue[Envelope | BaseException]
    terminal: threading.Event = field(default_factory=threading.Event)


class Stream:
    """Credit-backed remote stream; close cancels and waits for server cleanup."""

    __slots__ = ("_client", "_closed", "_deadline", "_error", "_pending", "_request_id")

    def __init__(
        self,
        client: Client,
        request_id: int,
        pending: _Pending,
        deadline: float,
    ) -> None:
        self._client = client
        self._request_id = request_id
        self._pending = pending
        self._deadline = deadline
        self._closed = False
        self._error: BaseException | None = None

    def __iter__(self) -> Stream:
        return self

    def __next__(self) -> object:
        if self._closed:
            raise StopIteration
        message = self._receive()
        if isinstance(message, BaseException):
            self._finish()
            raise message
        if message.kind == "chunk":
            payload = message.payload
            assert isinstance(payload, dict)
            self._client._control("credit", self._request_id, {"credit": 1})
            return payload["value"]
        if message.kind == "error":
            self._error = _remote_exception(message)
            return self.__next__()
        if message.kind == "end":
            payload = message.payload
            assert isinstance(payload, dict)
            reason = payload["reason"]
            error = self._error
            self._finish()
            if error is not None:
                raise error
            if reason == "deadline_exceeded":
                raise DeadlineExceeded("RPC stream deadline exceeded")
            if reason == "error":
                raise RemoteInternalError("RPC stream failed without a typed error")
            raise StopIteration
        self._finish()
        raise MalformedMessage(f"unexpected {message.kind!r} in stream")

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._client._control(
                "cancel",
                self._request_id,
                {"reason": "consumer stopped"},
            )
            while not self._pending.terminal.is_set():
                message = self._receive()
                if isinstance(message, BaseException):
                    break
                if message.kind == "error":
                    self._error = _remote_exception(message)
                elif message.kind == "end":
                    break
        finally:
            self._finish()

    def __enter__(self) -> Stream:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _receive(self) -> Envelope | BaseException:
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise DeadlineExceeded("RPC stream deadline exceeded")
        try:
            return self._pending.messages.get(timeout=remaining)
        except queue.Empty as error:
            raise DeadlineExceeded("RPC stream deadline exceeded") from error

    def _finish(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._client._retire(self._request_id)


class Client:
    """Multiplexed RPC client with attribute-built service paths."""

    __slots__ = (
        "_closed",
        "_codec",
        "_ids",
        "_path",
        "_pending",
        "_reader",
        "_send_lock",
        "_socket",
        "_state_lock",
        "close_timeout",
        "default_credit",
        "endpoint",
        "max_frame_bytes",
        "max_stream_queue",
        "namespace",
        "timeout",
    )

    def __init__(
        self,
        address: str | tuple[str, int],
        *,
        namespace: str = "default",
        timeout: float = DEFAULT_TIMEOUT,
        close_timeout: float = 2.0,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        default_credit: int = DEFAULT_CREDIT,
        max_stream_queue: int = DEFAULT_MAX_STREAM_QUEUE,
        _root: Client | None = None,
        _path: Path = (),
    ) -> None:
        if _root is not None:
            for name in Client.__slots__:
                if name != "_path":
                    setattr(self, name, getattr(_root, name))
            self._path = _path
            return
        if not namespace or "\x00" in namespace:
            raise ValueError("namespace must be a non-empty, NUL-free string")
        if timeout <= 0 or close_timeout <= 0:
            raise ValueError("RPC timeouts must be positive")
        if not 0 < default_credit <= max_stream_queue:
            raise ValueError("default_credit must fit max_stream_queue")
        self.endpoint = parse_endpoint(address)
        self.namespace = namespace
        self.timeout = timeout
        self.close_timeout = close_timeout
        self.max_frame_bytes = max_frame_bytes
        self.default_credit = default_credit
        self.max_stream_queue = max_stream_queue
        self._codec = make_codec(max_frame_bytes)
        sock = socket.socket(self.endpoint.family, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.settimeout(min(timeout, 0.2))
        try:
            sock.connect(self.endpoint.address)
        except BaseException:
            sock.close()
            raise
        self._socket = sock
        self._ids = MessageIds()
        self._send_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._pending: dict[int, _Pending] = {}
        self._closed = threading.Event()
        self._path = ()
        self._reader = threading.Thread(target=self._read_loop, name="xo-rpc-client")
        self._reader.start()

    def __getattr__(self, segment: str) -> Client:
        if segment.startswith("_"):
            raise AttributeError(segment)
        return Client(
            self.endpoint.address,
            _root=self,
            _path=(*self._path, segment),
        )

    def __call__(self, *args: object, **kwargs: object) -> object:
        if not self._path:
            raise TypeError("RPC Client root is not directly callable")
        return self.call(self._path, *args, **kwargs)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def ping(self, *, timeout: float | None = None) -> bool:
        result = self._request("ping", {}, timeout=timeout)
        if not isinstance(result, Envelope) or result.kind != "pong":
            raise MalformedMessage("ping received an invalid response")
        self._retire(result.reply_to or 0)
        return True

    def describe(self, *, timeout: float | None = None) -> tuple[Path, ...]:
        result = self._request("describe", {}, timeout=timeout)
        if not isinstance(result, Envelope) or result.kind != "result":
            raise MalformedMessage("describe received an invalid response")
        payload = result.payload
        assert isinstance(payload, dict)
        value = payload["value"]
        if not isinstance(value, list):
            raise MalformedMessage("describe result must be a list")
        paths = tuple(parse_path(path) for path in value)
        self._retire(result.reply_to or 0)
        return paths

    def call(
        self,
        path: str | tuple[str, ...],
        *args: object,
        timeout: float | None = None,
        **kwargs: object,
    ) -> object:
        canonical = parse_path(path)
        payload = {
            "path": path_payload(canonical),
            "args": list(args),
            "kwargs": kwargs,
            "credit": self.default_credit,
        }
        result = self._request("call", payload, timeout=timeout)
        if isinstance(result, Stream):
            return result
        if not isinstance(result, Envelope) or result.kind != "result":
            raise MalformedMessage("call received an invalid response")
        response = result.payload
        assert isinstance(response, dict)
        self._retire(result.reply_to or 0)
        return response["value"]

    def close(self) -> None:
        if self._closed.is_set():
            return
        with self._state_lock:
            request_ids = tuple(self._pending)
        for request_id in request_ids:
            try:
                self._control("cancel", request_id, {"reason": "client closing"})
            except (OSError, ProtocolError):
                break
        deadline = time.monotonic() + self.close_timeout
        while time.monotonic() < deadline:
            with self._state_lock:
                if not self._pending:
                    break
            time.sleep(0.001)
        self._closed.set()
        with contextlib.suppress(OSError):
            self._socket.shutdown(socket.SHUT_RDWR)
        self._socket.close()
        if self._reader is not threading.current_thread():
            self._reader.join(max(0.0, deadline - time.monotonic()))
        self._fail_all(ConnectionLost("RPC client closed"))

    def _request(
        self,
        kind: str,
        payload: dict[str, object],
        *,
        timeout: float | None,
    ) -> Envelope | Stream:
        if self._closed.is_set():
            raise ClosedError("RPC client is closed")
        duration = self.timeout if timeout is None else timeout
        if duration <= 0:
            raise ValueError("RPC timeout must be positive")
        request_id = self._ids.take()
        pending = _Pending(queue.Queue(maxsize=self.max_stream_queue + 3))
        with self._state_lock:
            self._pending[request_id] = pending
        wall_deadline = time.time() + duration
        monotonic_deadline = time.monotonic() + duration
        message = Envelope(
            kind,
            request_id,
            self.namespace,
            payload,
            deadline=wall_deadline,
        )
        try:
            self._send(message)
            remaining = monotonic_deadline - time.monotonic()
            response = pending.messages.get(timeout=max(0.0, remaining))
        except queue.Empty as error:
            self._control("cancel", request_id, {"reason": "client deadline"})
            self._retire(request_id)
            raise DeadlineExceeded("RPC deadline exceeded") from error
        except BaseException:
            self._retire(request_id)
            raise
        if isinstance(response, BaseException):
            self._retire(request_id)
            raise response
        if response.kind == "error":
            self._retire(request_id)
            raise _remote_exception(response)
        if response.kind == "start":
            return Stream(self, request_id, pending, monotonic_deadline)
        return response

    def _control(self, kind: str, request_id: int, payload: dict[str, object]) -> None:
        if self._closed.is_set():
            return
        self._send(
            Envelope(
                kind,
                self._ids.take(),
                self.namespace,
                payload,
                reply_to=request_id,
            )
        )

    def _send(self, message: Envelope) -> None:
        with self._send_lock:
            send_frame(
                self._socket,
                message,
                codec=self._codec,
                max_frame_bytes=self.max_frame_bytes,
            )

    def _read_loop(self) -> None:
        try:
            while not self._closed.is_set():
                try:
                    message = recv_frame(
                        self._socket,
                        codec=self._codec,
                        max_frame_bytes=self.max_frame_bytes,
                    )
                except TimeoutError:
                    continue
                if message.namespace != self.namespace:
                    raise NamespaceMismatch(
                        f"RPC namespace {message.namespace!r} does not match {self.namespace!r}"
                    )
                if message.reply_to is None:
                    raise MalformedMessage("server response is missing rid")
                with self._state_lock:
                    pending = self._pending.get(message.reply_to)
                if pending is None:
                    continue
                try:
                    pending.messages.put_nowait(message)
                except queue.Full as error:
                    raise BackpressureError("RPC client stream queue reached its bound") from error
                if message.kind == "end":
                    pending.terminal.set()
        except (EOFError, OSError, ProtocolError) as error:
            if not self._closed.is_set():
                self._fail_all(ConnectionLost(f"RPC connection lost: {error}"))
        finally:
            self._closed.set()

    def _fail_all(self, error: BaseException) -> None:
        with self._state_lock:
            pending_items = tuple(self._pending.values())
            self._pending.clear()
        for pending in pending_items:
            with contextlib.suppress(queue.Full):
                pending.messages.put_nowait(error)
            pending.terminal.set()

    def _retire(self, request_id: int) -> None:
        with self._state_lock:
            self._pending.pop(request_id, None)


def _remote_exception(message: Envelope) -> BaseException:
    payload = message.payload
    assert isinstance(payload, dict)
    code = str(payload["code"])
    text = str(payload["message"])
    if code == "xo.not_found":
        return ServiceNotFound(text)
    if code in {"xo.deadline", "xo.deadline_exceeded"}:
        return DeadlineExceeded(text)
    if code == "xo.cancelled":
        return CancelledError(text)
    if code == "xo.backpressure":
        return BackpressureError(text)
    if code == "xo.limit.concurrency":
        return ConcurrencyLimitError(
            text,
            retryable=bool(payload["retryable"]),
            detail=dict(payload["detail"]),
        )
    if code == "xo.internal":
        return RemoteInternalError(text, detail=dict(payload["detail"]))
    return RemoteError(
        text,
        code=code,
        retryable=bool(payload["retryable"]),
        detail=dict(payload["detail"]),
    )
