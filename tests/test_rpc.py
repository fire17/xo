from __future__ import annotations

import socket
import struct
import threading
import time

import pytest

from xo import XO, rpc_server, service
from xo.rpc import Client, FrameTooLarge, RemoteInternalError, RPCServer, Server
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

def test_rpc_server_is_a_root_scoped_capability(tmp_path) -> None:
    address = f"unix:///tmp/xo-rpc-{tmp_path.name}-capability.sock"
    state = XO.compose("app", service(), rpc_server(address))

    @state.public.image.generate
    def generate(prompt: str) -> str:
        return f"generated:{prompt}"

    state.start()
    capability = state.capability("rpc")
    assert isinstance(capability, RPCServer)
    try:
        with Client(address, namespace="app") as client:
            assert client.image.generate("sunrise") == "generated:sunrise"
    finally:
        state.close()
    assert not socket_path_exists(address)


def socket_path_exists(address: str) -> bool:
    from pathlib import Path

    return Path(address.removeprefix("unix://")).exists()


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


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), True, "1"])
def test_nonfinite_or_nonnumeric_timeouts_are_rejected(value: object) -> None:
    with pytest.raises(ValueError, match="positive and finite"):
        Server(ServiceRegistry(), ("127.0.0.1", 0), io_timeout=value)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="positive and finite"):
        Client(("127.0.0.1", 1), timeout=value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [0, -1, 1.5, True, "1"])
def test_rpc_bounds_must_be_positive_integers(value: object) -> None:
    with pytest.raises(ValueError, match="max_inflight"):
        Server(ServiceRegistry(), ("127.0.0.1", 0), max_inflight=value)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="max_stream_queue"):
        Client(("127.0.0.1", 1), max_stream_queue=value)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="max_frame_bytes"):
        Server(ServiceRegistry(), ("127.0.0.1", 0), max_frame_bytes=value)  # type: ignore[arg-type]


def test_stream_deadline_retires_pending_request(tmp_path) -> None:
    registry = ServiceRegistry()

    @registry.expose("slow")
    def slow():
        time.sleep(0.1)
        yield "late"

    address = f"unix:///tmp/xo-rpc-{tmp_path.name}-stream-deadline.sock"
    with (
        Server(registry, address, namespace="deadline-stream"),
        Client(address, namespace="deadline-stream", timeout=0.02) as client,
    ):
        stream = client.slow()
        request_id = stream._request_id
        with pytest.raises(Exception, match="deadline"):
            next(stream)
        assert request_id not in client._pending
        stream.close()


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
