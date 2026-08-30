from __future__ import annotations

import socket
import struct
import threading
import time

import pytest

from xo.rpc import Client, FrameTooLarge, RemoteInternalError, Server
from xo.rpc.protocol import DEFAULT_MAX_FRAME_BYTES, make_codec, recv_frame
from xo.service import ServiceNotFound, ServiceRegistry


def _registry() -> ServiceRegistry:
    registry = ServiceRegistry()

    @registry.expose("image.generate")
    def generate(prompt: str, *, style: str = "plain") -> dict[str, str]:
        return {"prompt": prompt, "style": style}

    @registry.expose("count")
    def count(stop: int):
        yield from range(stop)

    return registry


def test_dynamic_proxy_ping_describe_and_allow_list(tmp_path) -> None:
    address = f"unix:///tmp/xo-rpc-{tmp_path.name}-proxy.sock"
    with (
        Server(_registry(), address, namespace="images"),
        Client(address, namespace="images") as client,
    ):
        assert client.ping()
        assert client.describe() == (("count",), ("image", "generate"))
        assert client.image.generate("sunrise", style="vibrant") == {
            "prompt": "sunrise",
            "style": "vibrant",
        }
        with pytest.raises(ServiceNotFound):
            client.image.private("secret")


def test_stream_credit_and_early_cancel_cleanup_before_terminal(tmp_path) -> None:
    registry = ServiceRegistry()
    cleaned = threading.Event()

    @registry.expose("numbers")
    def numbers():
        try:
            yield from range(1_000)
        finally:
            cleaned.set()

    address = f"unix:///tmp/xo-rpc-{tmp_path.name}-stream.sock"
    with (
        Server(registry, address, namespace="stream", default_credit=1) as server,
        Client(address, namespace="stream", default_credit=1) as client,
    ):
        with client.numbers() as stream:
            assert next(stream) == 0
        assert cleaned.wait(1)
        assert server.active_connections == 1


def test_stream_producer_error_is_typed_and_retires(tmp_path) -> None:
    registry = ServiceRegistry()

    @registry.expose("broken")
    def broken():
        yield "first"
        raise RuntimeError("boom")

    address = f"unix:///tmp/xo-rpc-{tmp_path.name}-errors.sock"
    with (
        Server(registry, address, namespace="errors"),
        Client(address, namespace="errors") as client,
    ):
        stream = client.broken()
        assert next(stream) == "first"
        with pytest.raises(RemoteInternalError, match="RuntimeError: boom"):
            next(stream)


def test_expired_call_is_rejected_without_invocation(tmp_path) -> None:
    registry = ServiceRegistry()
    invoked = threading.Event()

    @registry.expose("work")
    def work():
        invoked.set()

    address = f"unix:///tmp/xo-rpc-{tmp_path.name}-deadlines.sock"
    with (
        Server(registry, address, namespace="deadlines"),
        Client(address, namespace="deadlines") as client,
    ):
        with pytest.raises(ValueError):
            client.work(timeout=0)
        assert not invoked.is_set()


def test_non_loopback_addresses_are_refused() -> None:
    with pytest.raises(ValueError, match="non-loopback"):
        Server(ServiceRegistry(), ("192.0.2.1", 9000))
    with pytest.raises(ValueError, match="non-loopback"):
        Client("tcp://192.0.2.1:9000")


def test_oversized_frame_fails_before_body_allocation() -> None:
    left, right = socket.socketpair()
    try:
        right.sendall(struct.pack(">I", DEFAULT_MAX_FRAME_BYTES + 1))
        with pytest.raises(FrameTooLarge):
            recv_frame(
                left,
                codec=make_codec(DEFAULT_MAX_FRAME_BYTES),
                max_frame_bytes=DEFAULT_MAX_FRAME_BYTES,
            )
    finally:
        left.close()
        right.close()


def test_malformed_frame_closes_connection(tmp_path) -> None:
    socket_path = f"/tmp/xo-rpc-{tmp_path.name}-malformed.sock"
    address = f"unix://{socket_path}"
    with Server(_registry(), address, namespace="malformed"):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.connect(socket_path)
        sock.sendall(struct.pack(">I", 1) + b"{")
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline and sock.recv(1) != b"":
            pass
        assert sock.recv(1) == b""
        sock.close()
