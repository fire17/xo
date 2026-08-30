from __future__ import annotations

import base64
import os
import socket
import struct

import pytest

from xo import XO, DerivedEvent
from xo.web import WebSocketBridge, WebSocketLimits, websocket
from xo.wire import Envelope, decode_envelope, encode_envelope

TOKEN = "t" * 32




def _start(*, writable: bool = False, limits: WebSocketLimits | None = None):
    state = XO.compose(
        "app",
        websocket(
            token=TOKEN,
            writable=(["ui"],) if writable else (),
            limits=limits,
        ),
    )
    state.start()
    bridge = state.capability("websocket")
    assert isinstance(bridge, WebSocketBridge)
    return state, bridge


def _connect(bridge: WebSocketBridge) -> socket.socket:
    sock = socket.create_connection(bridge.address, timeout=2)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request = (
        f"GET /xo HTTP/1.1\r\n"
        f"Host: {bridge.address[0]}:{bridge.address[1]}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n"
    ).encode("ascii")
    sock.sendall(request)
    response = b""
    while not response.endswith(b"\r\n\r\n"):
        response += sock.recv(1024)
    assert response.startswith(b"HTTP/1.1 101 ")
    return sock


def _send(sock: socket.socket, envelope: Envelope, *, masked: bool = True) -> None:
    payload = encode_envelope(envelope)
    mask = os.urandom(4)
    if len(payload) < 126:
        header = bytes((0x81, (0x80 if masked else 0) | len(payload)))
    else:
        header = bytes((0x81, (0x80 if masked else 0) | 126)) + struct.pack("!H", len(payload))
    if masked:
        payload = bytes(byte ^ mask[index & 3] for index, byte in enumerate(payload))
        sock.sendall(header + mask + payload)
    else:
        sock.sendall(header + payload)


def _recv(sock: socket.socket) -> Envelope:
    first, second = _exact(sock, 2)
    assert first & 0x0F == 1
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", _exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", _exact(sock, 8))[0]
    return decode_envelope(_exact(sock, length))


def _exact(sock: socket.socket, length: int) -> bytes:
    result = bytearray()
    while len(result) < length:
        chunk = sock.recv(length - len(result))
        if not chunk:
            raise EOFError
        result.extend(chunk)
    return bytes(result)


def _hello(sock: socket.socket, *, role: str = "observer", namespace: str = "app") -> Envelope:
    _send(
        sock,
        Envelope(
            "hello",
            1,
            namespace,
            {
                "protocol": 1,
                "schema": 1,
                "origin_id": "abc",
                "role": role,
                "token": TOKEN,
            },
        ),
    )
    return _recv(sock)


def test_snapshot_event_tree_and_delete_surface() -> None:
    state, bridge = _start()
    state.ui.title = "ready"
    state.ui.count = 1
    sock = _connect(bridge)
    try:
        assert _hello(sock).kind == "welcome"
        _send(
            sock,
            Envelope(
                "sub",
                2,
                "app",
                {"prefixes": [["ui"]], "since_revision": 0, "materialize": []},
            ),
        )
        snapshot = _recv(sock)
        assert snapshot.kind == "snapshot"
        assert snapshot.payload["revision"] == 2
        assert snapshot.payload["root"]["$children"][0][0] == "ui"
        assert _recv(sock).payload["mode"] == "snapshot"

        event = state.ui.count.set(2)
        bridged = _recv(sock)
        assert bridged.kind == "event"
        assert bridged.payload["revision"] == event.revision
        assert bridged.payload["path"] == ["ui", "count"]
        state.ui.title.delete()
        assert _recv(sock).payload["operation"] == "delete_subtree"
    finally:
        sock.close()
        state.close()


def test_browser_writes_use_canonical_root_pipeline_and_expected_revision() -> None:
    state, bridge = _start(writable=True)
    sock = _connect(bridge)
    try:
        assert _hello(sock, role="writer").kind == "welcome"
        _send(sock, Envelope("sub", 2, "app", {"prefixes": [["ui"]], "since_revision": 0}))
        assert _recv(sock).kind == "snapshot"
        assert _recv(sock).kind == "ack"
        _send(
            sock,
            Envelope(
                "set",
                3,
                "app",
                {"path": ["ui", "name"], "value": "Tami", "expected_revision": 0},
            ),
        )
        messages = {_recv(sock).kind, _recv(sock).kind}
        assert messages == {"ack", "event"}
        assert state.ui.name.value == "Tami"
    finally:
        sock.close()
        state.close()


def test_derived_projection_never_mutates_source_or_revision() -> None:
    state, bridge = _start()
    sock = _connect(bridge)
    try:
        assert _hello(sock).kind == "welcome"
        _send(sock, Envelope("sub", 2, "app", {"prefixes": [["total"]], "since_revision": 0}))
        _recv(sock)
        _recv(sock)
        revision = state.revision
        bridge.publish_derived(
            DerivedEvent(
                namespace="app",
                origin_id=state.origin_id,
                cause_revision=revision,
                path=("total",),
                formula_generation=1,
                status="value",
                payload=42,
            )
        )
        message = _recv(sock)
        assert message.kind == "derived"
        assert "revision" not in message.payload
        assert "event_id" not in message.payload
        assert state.peek("total") is None
        assert state.revision == revision
    finally:
        sock.close()
        state.close()


def test_catchup_is_contiguous_and_duplicate_observe_is_deduped() -> None:
    state, bridge = _start()
    first = state.ui.a.set(1)
    bridge.observe(first)
    state.ui.b.set(2)
    sock = _connect(bridge)
    try:
        _hello(sock)
        _send(sock, Envelope("sub", 2, "app", {"prefixes": [["ui"]], "since_revision": 1}))
        event = _recv(sock)
        assert event.kind == "event"
        assert event.payload["revision"] == 2
        ack = _recv(sock)
        assert ack.payload == {"count": 1, "mode": "catchup", "revision": 2}
    finally:
        sock.close()
        state.close()


def test_malformed_unmasked_oversize_and_namespace_mismatch_fail_closed() -> None:
    state, bridge = _start(limits=WebSocketLimits(max_frame_bytes=512))
    try:
        unmasked = _connect(bridge)
        _send(unmasked, Envelope("hello", 1, "app", {}), masked=False)
        assert _recv(unmasked).payload["code"] == "xo.protocol.malformed"
        unmasked.close()

        wrong = _connect(bridge)
        _send(
            wrong,
            Envelope(
                "hello",
                1,
                "other",
                {
                    "protocol": 1,
                    "schema": 1,
                    "origin_id": "a",
                    "role": "observer",
                    "token": TOKEN,
                },
            ),
        )
        assert _recv(wrong).payload["code"] == "xo.protocol.namespace_mismatch"
        wrong.close()

        oversize = _connect(bridge)
        mask = os.urandom(4)
        oversize.sendall(bytes((0x81, 0x80 | 126)) + struct.pack("!H", 513) + mask)
        assert _recv(oversize).payload["code"] == "xo.protocol.frame_too_large"
        oversize.close()
    finally:
        state.close()


def test_bridge_refuses_non_loopback() -> None:
    with pytest.raises(ValueError, match="loopback"):
        XO.compose("app", websocket(host="0.0.0.0", token=TOKEN))
