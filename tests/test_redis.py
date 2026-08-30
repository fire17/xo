from __future__ import annotations

import hashlib
import os
import secrets
from collections import deque

import pytest

from xo.backends import redis as redis_module
from xo.backends.redis import (
    RedisBackend,
    RedisEndpoint,
    RedisLimits,
    RedisProtocolError,
    RedisUnavailable,
)
from xo.events import Event, EventGroup, Operation
from xo.exceptions import CommitOutcomeUnknown, ConflictError, RecoveryRequired
from xo.wire import decode_envelope, item_from_envelope


class FakeCommand:
    def __init__(self, replies: object = ()) -> None:
        self.replies = deque(replies if isinstance(replies, list) else [replies])
        self.commands: list[tuple[object, ...]] = []
        self.closed = False

    def connect(self) -> None:
        pass

    def execute(self, *parts: object) -> object:
        self.commands.append(parts)
        if not self.replies:
            raise AssertionError(f"no fake reply for {parts!r}")
        reply = self.replies.popleft()
        if isinstance(reply, BaseException):
            raise reply
        return reply

    def close_socket(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


def event(
    *,
    event_id: int = 0xAA,
    origin_id: int = 0x22,
    base_revision: int = 0,
    revision: int = 1,
    path: tuple[str, ...] = ("status",),
    payload: object = "ready",
) -> Event:
    return Event(
        event_id=event_id,
        namespace="app",
        origin_id=origin_id,
        base_revision=base_revision,
        revision=revision,
        operation=Operation.SET_VALUE,
        path=path,
        payload=payload,
    )


def ready_backend(*replies: object, remote_revision: int = 0) -> tuple[RedisBackend, FakeCommand]:
    backend = RedisBackend(namespace="app", epoch="epoch-1")
    command = FakeCommand(list(replies))
    backend._command = command
    backend._epoch = "epoch-1"
    backend._keys = redis_module._NamespaceKeys.for_namespace("app")
    backend._remote_revision = remote_revision
    return backend, command


def stream_entry(item: Event | EventGroup) -> list[object]:
    identity = item.transaction_id if isinstance(item, EventGroup) else item.event_id
    first = item.events[0] if isinstance(item, EventGroup) else item
    body = redis_module.encode_envelope(
        redis_module.commit_envelope(item, message_id=identity)
    )
    return [
        f"{first.revision}-0".encode(),
        [
            b"b",
            body,
            b"r",
            str(first.revision).encode(),
            b"i",
            format(identity, "x").encode(),
            b"e",
            b"epoch-1",
            b"h",
            hashlib.sha256(body).hexdigest().encode(),
        ],
    ]


def test_constructor_is_inert_and_close_is_idempotent() -> None:
    socket_calls = 0

    def socket_factory() -> object:
        nonlocal socket_calls
        socket_calls += 1
        raise AssertionError("constructor must not open Redis")

    backend = RedisBackend(namespace="app", socket_factory=socket_factory)

    assert socket_calls == 0
    assert not backend.listener_running
    backend.close()
    backend.close()
    assert socket_calls == 0


def test_endpoints_reject_non_loopback_and_unsafe_namespace() -> None:
    assert RedisEndpoint.parse("redis://127.0.0.1:6380/4").database == 4
    assert RedisEndpoint.parse("unix:///tmp/redis.sock?db=2").path == "/tmp/redis.sock"

    with pytest.raises(ValueError, match="loopback"):
        RedisEndpoint.parse("redis://redis.example.com:6379/0")
    with pytest.raises(ValueError, match="unsafe"):
        RedisBackend(namespace="bad{tag}")
    with pytest.raises(ValueError, match="strict"):
        RedisBackend(namespace="app", strict=False)


def test_limits_refuse_unbounded_or_zero_values() -> None:
    with pytest.raises(ValueError, match="max_frame_bytes"):
        RedisLimits(max_frame_bytes=0)
    with pytest.raises(ValueError, match="reconnect_attempts"):
        RedisBackend(reconnect_attempts=-1)
    with pytest.raises(ValueError, match="operation_timeout"):
        RedisBackend(operation_timeout=0)


def test_commit_uses_one_lua_cas_with_canonical_tagged_event() -> None:
    backend, command = ready_backend([b"ok", b"1-0"])
    change = event(payload=(b"safe", 3))

    assert backend.commit(change) == "1-0"

    assert len(command.commands) == 1
    parts = command.commands[0]
    assert parts[:3] == ("EVAL", redis_module._COMMIT_SCRIPT, 6)
    body = parts[13]
    assert isinstance(body, bytes)
    decoded = item_from_envelope(decode_envelope(body), namespace="app")
    assert decoded == change
    assert b"pickle" not in body.lower()
    assert parts[6] == "xo:{app}:tx"
    assert backend.remote_revision == 1


def test_event_group_is_one_atomic_stream_unit_and_uses_first_identity() -> None:
    backend, command = ready_backend([b"ok", b"1-0"])
    group = EventGroup(
        (
            event(event_id=0x101, path=("left",), payload=1),
            event(event_id=0x102, path=("right",), payload=2),
        )
    )

    backend.commit(group)

    assert len(command.commands) == 1
    parts = command.commands[0]
    decoded = item_from_envelope(decode_envelope(parts[13]), namespace="app")
    assert decoded == group
    assert parts[14] == "101"


def test_conflict_is_definite_and_transport_loss_after_send_is_unknown() -> None:
    conflict, _ = ready_backend([b"conflict", b"7"])
    with pytest.raises(ConflictError, match="head 7"):
        conflict.commit(event())

    unknown, _ = ready_backend(
        redis_module._TransportFailure("reply lost", sent=True)
    )
    with pytest.raises(CommitOutcomeUnknown, match="same event identity"):
        unknown.commit(event())

    definite, _ = ready_backend(
        redis_module._TransportFailure("connect refused", sent=False)
    )
    with pytest.raises(RedisUnavailable, match="not sent"):
        definite.commit(event())


def test_reconcile_checks_revision_identity_and_canonical_hash() -> None:
    change = event()
    backend, _ = ready_backend(b"1", [stream_entry(change)])
    assert backend.reconcile(change) is True

    absent, _ = ready_backend(b"0")
    assert absent.reconcile(change) is False

    other = event(event_id=0xBB)
    conflict, _ = ready_backend(b"1", [stream_entry(other)])
    with pytest.raises(ConflictError, match="belongs to commit"):
        conflict.reconcile(change)


def test_catch_up_delivers_contiguous_remote_units_once_and_suppresses_echo() -> None:
    change = event(origin_id=0x44)
    backend, _ = ready_backend(b"1", [stream_entry(change)], remote_revision=0)
    received: list[Event | EventGroup] = []
    backend.set_remote_sink(received.append, revision=0, origin_id=0x22)

    assert backend.catch_up() == 1
    assert received == [change]

    own = event(origin_id=0x22)
    echo, _ = ready_backend(b"1", [stream_entry(own)], remote_revision=0)
    echoed: list[Event | EventGroup] = []
    echo.set_remote_sink(echoed.append, revision=0, origin_id=0x22)
    assert echo.catch_up() == 1
    assert echoed == []


def test_gap_requires_explicit_snapshot_sink_instead_of_local_fallback() -> None:
    revision_two = event(
        event_id=0xCC,
        origin_id=0x44,
        base_revision=1,
        revision=2,
    )
    backend, _ = ready_backend(b"2", [stream_entry(revision_two)], remote_revision=0)
    backend.set_remote_sink(lambda _: None, revision=0)

    with pytest.raises(RecoveryRequired, match="snapshot sink"):
        backend.catch_up()


def test_resp_reader_enforces_frame_array_and_nesting_limits() -> None:
    class FakeSocket:
        def __init__(self, response: bytes) -> None:
            self.response = response

        def recv(self, size: int) -> bytes:
            chunk = self.response[:size]
            self.response = self.response[size:]
            return chunk

    reader = redis_module._RESPReader(
        FakeSocket(b"$5\r\nhello\r\n"),
        max_frame_bytes=32,
        max_array_items=4,
        max_nesting=2,
    )
    assert reader.read_response() == b"hello"

    oversized = redis_module._RESPReader(
        FakeSocket(b"$99\r\n"),
        max_frame_bytes=16,
        max_array_items=4,
        max_nesting=2,
    )
    with pytest.raises(RedisProtocolError, match="bulk length"):
        oversized.read_response()

    too_many = redis_module._RESPReader(
        FakeSocket(b"*5\r\n"),
        max_frame_bytes=16,
        max_array_items=4,
        max_nesting=2,
    )
    with pytest.raises(RedisProtocolError, match="array length"):
        too_many.read_response()


def test_optional_real_redis_round_trip() -> None:
    url = os.environ.get("XO_TEST_REDIS_URL")
    if not url:
        pytest.skip("set XO_TEST_REDIS_URL to a disposable loopback Redis server")
    namespace = f"xo-test-{secrets.token_hex(8)}"
    writer = RedisBackend(url, namespace=namespace, epoch=namespace)
    try:
        writer.prepare()
        change = event()
        object.__setattr__(change, "namespace", namespace)
        assert writer.commit(change) == "1-0"
        assert writer.reconcile(change) is True
    finally:
        writer.close()
