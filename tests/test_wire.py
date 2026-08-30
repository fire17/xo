from __future__ import annotations

from dataclasses import fields

import pytest

from xo.events import DerivedEvent, Event, EventGroup, Operation
from xo.wire import (
    Envelope,
    NamespaceMismatch,
    WireError,
    commit_envelope,
    decode_envelope,
    derived_envelope,
    encode_envelope,
    item_from_envelope,
)


def event(
    event_id: int,
    *,
    base: int = 0,
    revision: int = 1,
    operation: Operation = Operation.SET_VALUE,
    payload: object = 3,
) -> Event:
    return Event(event_id, "app", 7, base, revision, operation, ("count",), payload)


def test_single_event_round_trip_has_no_transaction_wrapper() -> None:
    source = event(1)
    envelope = commit_envelope(source, message_id=9)

    assert envelope.kind == "event"
    assert "events" not in envelope.payload
    assert item_from_envelope(decode_envelope(encode_envelope(envelope))) == source


def test_complete_transaction_round_trips_as_one_envelope() -> None:
    source = EventGroup(
        (
            event(1),
            event(
                2,
                base=0,
                revision=1,
                operation=Operation.CLEAR_VALUE,
                payload=None,
            ),
        )
    )
    envelope = commit_envelope(source, message_id=10)

    assert envelope.kind == "tx"
    assert "transaction_id" not in envelope.payload
    assert item_from_envelope(decode_envelope(encode_envelope(envelope))) == source


def test_derived_projection_is_structurally_not_an_authored_event() -> None:
    source = DerivedEvent("app", 7, 4, ("total",), 2, "value", 9)
    envelope = derived_envelope(source, message_id=11)
    payload = envelope.payload

    assert envelope.kind == "derived"
    assert {field.name for field in fields(DerivedEvent)}.isdisjoint(
        {"event_id", "base_revision", "revision", "operation"}
    )
    assert not ({"event_id", "base_revision", "operation"} & payload.keys())
    assert item_from_envelope(decode_envelope(encode_envelope(envelope))) == source


def test_namespace_mismatch_and_malformed_operation_payload_fail_closed() -> None:
    with pytest.raises(NamespaceMismatch):
        item_from_envelope(commit_envelope(event(1), message_id=1), namespace="other")

    malformed = Envelope(
        "event",
        1,
        "app",
        {
            "event_id": "1",
            "namespace": "app",
            "origin_id": "7",
            "base_revision": 0,
            "revision": 1,
            "operation": "delete_subtree",
            "path": ["count"],
            "payload": {"new": 3},
        },
    )
    with pytest.raises(WireError):
        item_from_envelope(malformed)
