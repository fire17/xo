from __future__ import annotations

import ipaddress
import math
import socket
import struct
import threading
from dataclasses import dataclass
from typing import Final
from urllib.parse import unquote, urlsplit

from ..codec import Codec, CodecLimits
from ..exceptions import CodecError, ProtocolError
from ..path import Path, validate_path
from ..wire import Envelope, WireError, decode_envelope, encode_envelope

PROTOCOL_VERSION: Final = 1
DEFAULT_MAX_FRAME_BYTES: Final = 8 * 1024 * 1024
DEFAULT_MAX_INFLIGHT: Final = 256
DEFAULT_CREDIT: Final = 64
DEFAULT_MAX_STREAM_QUEUE: Final = 256
DEFAULT_TIMEOUT: Final = 30.0
MAX_PATH_SEGMENTS: Final = 64
MAX_SEGMENT_BYTES: Final = 256
_HEADER = struct.Struct(">I")

REQUEST_KINDS: Final = frozenset({"ping", "describe", "call", "credit", "cancel"})
RESPONSE_KINDS: Final = frozenset({"pong", "result", "start", "chunk", "end", "error"})
KINDS: Final = REQUEST_KINDS | RESPONSE_KINDS
_RID_REQUIRED: Final = RESPONSE_KINDS | {"credit", "cancel"}


@dataclass(frozen=True, slots=True)
class Endpoint:
    family: socket.AddressFamily
    address: str | tuple[str, int]

    @property
    def is_unix(self) -> bool:
        return self.family == socket.AF_UNIX


class RemoteError(ProtocolError):
    code = "xo.remote"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        retryable: bool = False,
        detail: dict[str, object] | None = None,
    ) -> None:
        self.code = code or type(self).code
        self.retryable = retryable
        self.detail = detail or {}
        super().__init__(message)


class RemoteInternalError(RemoteError):
    code = "xo.internal"


class ConcurrencyLimitError(RemoteError):
    code = "xo.limit.concurrency"


class ConnectionLost(RemoteError):
    code = "xo.connection_lost"


class MalformedMessage(ProtocolError):
    code = "xo.protocol.malformed"


class FrameTooLarge(ProtocolError):
    code = "xo.limit.frame"


class NamespaceMismatch(ProtocolError):
    code = "xo.protocol.namespace_mismatch"


class MessageIds:
    __slots__ = ("_lock", "_next")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next = 1

    def take(self) -> int:
        with self._lock:
            value = self._next
            self._next += 1
            return value


def parse_endpoint(address: str | tuple[str, int]) -> Endpoint:
    if isinstance(address, tuple):
        if len(address) != 2:
            raise ValueError("TCP address must be a (host, port) pair")
        return _tcp_endpoint(address[0], address[1])
    if not isinstance(address, str):
        raise TypeError("RPC address must be a URI or (host, port) pair")
    if address.startswith("unix://"):
        parsed = urlsplit(address)
        if parsed.netloc not in {"", "localhost"} or parsed.query or parsed.fragment:
            raise ValueError("Unix RPC address must be unix:///absolute/path")
        path = unquote(parsed.path)
        if not path or not path.startswith("/") or "\x00" in path:
            raise ValueError("Unix RPC address must contain an absolute, NUL-free path")
        return Endpoint(socket.AF_UNIX, path)
    if address.startswith("tcp://"):
        parsed = urlsplit(address)
        if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ValueError("TCP RPC address must be tcp://loopback:port")
        try:
            port = parsed.port
        except ValueError as error:
            raise ValueError("invalid TCP RPC port") from error
        if parsed.hostname is None or port is None:
            raise ValueError("TCP RPC address must include host and port")
        return _tcp_endpoint(parsed.hostname, port)
    if address.startswith("/"):
        if "\x00" in address:
            raise ValueError("Unix RPC path cannot contain NUL")
        return Endpoint(socket.AF_UNIX, address)
    raise ValueError("RPC address must use unix:///path or tcp://loopback:port")


def _tcp_endpoint(host: object, port: object) -> Endpoint:
    if not isinstance(host, str) or not isinstance(port, int) or isinstance(port, bool):
        raise ValueError("TCP address requires a string host and integer port")
    if not 0 <= port <= 65535:
        raise ValueError("TCP RPC port must be between 0 and 65535")
    normalized = host.strip("[]").lower()
    if normalized == "localhost":
        normalized = "127.0.0.1"
    try:
        ip = ipaddress.ip_address(normalized)
    except ValueError as error:
        raise ValueError("RPC v1 accepts only literal loopback hosts or localhost") from error
    if not ip.is_loopback:
        raise ValueError("RPC v1 refuses non-loopback TCP addresses")
    family = socket.AF_INET6 if ip.version == 6 else socket.AF_INET
    return Endpoint(family, (str(ip), port))


def make_codec(max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES) -> Codec:
    if max_frame_bytes <= 0 or max_frame_bytes > 0xFFFFFFFF:
        raise ValueError("max_frame_bytes must be between 1 and 2^32-1")
    return Codec(limits=CodecLimits(max_bytes=max_frame_bytes))


def encode_frame(envelope: Envelope, *, codec: Codec, max_frame_bytes: int) -> bytes:
    validated = validate_envelope(envelope)
    try:
        canonical = codec.loads(encode_envelope(validated, codec=codec))
        body = codec.dumps({"protocol": PROTOCOL_VERSION, "envelope": canonical})
    except (CodecError, WireError) as error:
        raise MalformedMessage(str(error)) from error
    if not body or len(body) > max_frame_bytes:
        raise FrameTooLarge(f"RPC frame exceeds {max_frame_bytes} bytes")
    return _HEADER.pack(len(body)) + body


def send_frame(
    sock: socket.socket,
    envelope: Envelope,
    *,
    codec: Codec,
    max_frame_bytes: int,
) -> None:
    sock.sendall(encode_frame(envelope, codec=codec, max_frame_bytes=max_frame_bytes))


def recv_frame(sock: socket.socket, *, codec: Codec, max_frame_bytes: int) -> Envelope:
    header = _recv_exact(sock, _HEADER.size)
    (size,) = _HEADER.unpack(header)
    if size == 0 or size > max_frame_bytes:
        raise FrameTooLarge(f"invalid RPC frame length {size}; maximum is {max_frame_bytes}")
    body = _recv_exact(sock, size)
    try:
        wrapper = codec.loads(body)
        if not isinstance(wrapper, dict) or set(wrapper) != {"protocol", "envelope"}:
            raise MalformedMessage("RPC frame must contain a versioned envelope")
        if wrapper["protocol"] != PROTOCOL_VERSION:
            raise MalformedMessage(
                f"unsupported RPC protocol version: {wrapper['protocol']!r}"
            )
        canonical = codec.dumps(wrapper["envelope"])
        envelope = decode_envelope(canonical, codec=codec)
    except (CodecError, WireError) as error:
        raise MalformedMessage(str(error)) from error
    return validate_envelope(envelope)


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise EOFError("RPC peer closed the connection")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def validate_envelope(message: Envelope) -> Envelope:
    if not isinstance(message, Envelope):
        raise MalformedMessage("RPC envelope has the wrong type")
    kind = message.kind
    if kind not in KINDS:
        raise MalformedMessage(f"unknown RPC message kind: {kind!r}")
    _nonnegative_int(message.message_id, "mid", positive=True)
    namespace = message.namespace
    if (
        not isinstance(namespace, str)
        or not namespace
        or "\x00" in namespace
        or len(namespace.encode("utf-8")) > MAX_SEGMENT_BYTES
    ):
        raise MalformedMessage("RPC namespace must be a non-empty bounded string")
    if not isinstance(message.payload, dict):
        raise MalformedMessage("RPC payload must be an object")
    if kind in _RID_REQUIRED:
        _nonnegative_int(message.reply_to, "rid", positive=True)
    elif message.reply_to is not None:
        raise MalformedMessage(f"{kind} must not carry rid")
    if message.deadline is not None:
        deadline = message.deadline
        if not math.isfinite(deadline) or deadline <= 0:
            raise MalformedMessage("RPC deadline must be a positive finite unix timestamp")
    if message.trace_id is not None and not isinstance(message.trace_id, str):
        raise MalformedMessage("RPC trace id must be a string")
    _validate_payload(kind, message.payload)
    return message


def _validate_payload(kind: str, payload: dict[str, object]) -> None:
    if kind in {"ping", "pong"}:
        return
    if kind == "describe":
        if payload:
            raise MalformedMessage("describe payload must be empty")
        return
    if kind == "call":
        if not set(payload) <= {"path", "args", "kwargs", "credit"}:
            raise MalformedMessage("call payload contains unknown fields")
        wire_path(payload.get("path"))
        if not isinstance(payload.get("args"), list):
            raise MalformedMessage("call args must be a list")
        kwargs = payload.get("kwargs")
        if not isinstance(kwargs, dict) or any(not isinstance(key, str) for key in kwargs):
            raise MalformedMessage("call kwargs must be an object with string keys")
        if "credit" in payload:
            bounded_credit(payload["credit"])
        return
    if kind == "credit":
        if set(payload) != {"credit"}:
            raise MalformedMessage("credit payload must contain only credit")
        bounded_credit(payload["credit"])
        return
    if kind == "cancel":
        if not set(payload) <= {"reason"}:
            raise MalformedMessage("cancel payload contains unknown fields")
        if "reason" in payload and not isinstance(payload["reason"], str):
            raise MalformedMessage("cancel reason must be a string")
        return
    if kind == "result":
        if set(payload) != {"value"}:
            raise MalformedMessage("result payload must contain value")
        return
    if kind == "start":
        if payload != {"streaming": True}:
            raise MalformedMessage("start payload must declare streaming=true")
        return
    if kind == "chunk":
        if set(payload) != {"seq", "value"}:
            raise MalformedMessage("chunk payload must contain seq and value")
        _nonnegative_int(payload["seq"], "seq")
        return
    if kind == "end":
        if set(payload) != {"reason", "count"}:
            raise MalformedMessage("end payload must contain reason and count")
        if payload["reason"] not in {"complete", "cancelled", "deadline_exceeded", "error"}:
            raise MalformedMessage("invalid stream end reason")
        _nonnegative_int(payload["count"], "count")
        return
    if kind == "error":
        if set(payload) != {"code", "message", "retryable", "detail"}:
            raise MalformedMessage("error payload has invalid fields")
        if not isinstance(payload["code"], str) or not isinstance(payload["message"], str):
            raise MalformedMessage("error code and message must be strings")
        if not isinstance(payload["retryable"], bool) or not isinstance(payload["detail"], dict):
            raise MalformedMessage("invalid typed error fields")


def wire_path(value: object) -> Path:
    if not isinstance(value, list):
        raise MalformedMessage("RPC path must be a list of strings")
    if not value:
        raise MalformedMessage("RPC service path cannot be empty")
    if len(value) > MAX_PATH_SEGMENTS:
        raise MalformedMessage(f"RPC path exceeds {MAX_PATH_SEGMENTS} segments")
    for segment in value:
        if (
            not isinstance(segment, str)
            or not segment
            or "\x00" in segment
            or len(segment.encode("utf-8")) > MAX_SEGMENT_BYTES
        ):
            raise MalformedMessage("RPC path contains an invalid segment")
    try:
        return validate_path(tuple(value))
    except ValueError as error:
        raise MalformedMessage(str(error)) from error


def path_payload(path: Path) -> list[str]:
    return list(wire_path(list(path)))


def bounded_credit(value: object) -> int:
    credit = _nonnegative_int(value, "credit", positive=True)
    if credit > DEFAULT_MAX_STREAM_QUEUE:
        raise MalformedMessage(f"credit exceeds {DEFAULT_MAX_STREAM_QUEUE}")
    return credit


def _nonnegative_int(value: object, name: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < (1 if positive else 0):
        qualifier = "positive" if positive else "non-negative"
        raise MalformedMessage(f"{name} must be a {qualifier} integer")
    return value
