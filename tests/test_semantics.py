from __future__ import annotations

from dataclasses import fields

import pytest

from xo import (
    XO,
    AmbiguousRedo,
    CommitOutcomeUnknown,
    DerivedEvent,
    Event,
    EventGroup,
    MissingPath,
    Operation,
    RecoveryRequired,
    StaleNode,
)
from xo.backends import backend
from xo.history import History, history
from xo.service import ServiceRegistry, service


def test_derived_event_is_not_authored_state() -> None:
    names = tuple(field.name for field in fields(DerivedEvent))
    assert names == (
        "namespace",
        "origin_id",
        "cause_revision",
        "path",
        "formula_generation",
        "status",
        "payload",
        "diagnostics",
    )
    assert not issubclass(DerivedEvent, Event)


def test_multi_operation_commit_is_atomic_and_has_single_revision() -> None:
    state = XO("app")
    seen: list[tuple[str, int, object, object]] = []
    state.a.subscribe(
        lambda event: seen.append(("a", event.revision, state.a.value, state.b.value))
    )
    state.b.subscribe(
        lambda event: seen.append(("b", event.revision, state.a.value, state.b.value))
    )

    item = state.commit_many(
        (
            (Operation.SET_VALUE, ("a",), 1),
            (Operation.SET_VALUE, ("b",), 2),
        )
    )

    assert isinstance(item, EventGroup)
    assert len(item.events) == 2
    assert {event.revision for event in item.events} == {1}
    assert {event.base_revision for event in item.events} == {0}
    assert item.transaction_id == item.events[0].event_id
    assert seen == [("a", 1, 1, 2), ("b", 1, 1, 2)]


def test_group_validation_failure_has_no_partial_apply() -> None:
    state = XO("app")
    state.a = "old"

    with pytest.raises(MissingPath):
        state.commit_many(
            (
                (Operation.SET_VALUE, ("a",), "new"),
                (Operation.DELETE_SUBTREE, ("missing",), None),
            )
        )

    assert state.a.value == "old"
    assert state.revision == 1
    assert state.peek("missing") is None


def test_remote_group_dedupes_without_echo_and_stays_atomic() -> None:
    source = XO("app", origin_id=7)
    item = source.commit_many(
        (
            (Operation.SET_VALUE, ("a",), 1),
            (Operation.SET_VALUE, ("b",), 2),
        )
    )
    replica = XO("app")
    observed: list[object] = []
    replica.a.subscribe(observed.append)

    assert replica.apply_remote(item)
    assert (replica.a.value, replica.b.value, replica.revision) == (1, 2, 1)
    assert len(observed) == 1
    assert not replica.apply_remote(item)
    assert len(observed) == 1


def test_unknown_commit_freezes_until_same_item_reconciles() -> None:
    class UnknownBackend:
        strict = True

        def __init__(self) -> None:
            self.item: Event | EventGroup | None = None

        def commit(self, item: Event | EventGroup) -> None:
            self.item = item
            raise CommitOutcomeUnknown("reply lost")

        def reconcile(self, item: Event | EventGroup) -> bool:
            return item == self.item

        def close(self) -> None:
            pass

    coordinator = UnknownBackend()
    state = XO.compose("app", backend(coordinator))
    with pytest.raises(CommitOutcomeUnknown):
        state.answer = 42
    with pytest.raises(RecoveryRequired):
        _ = state.answer.value

    assert state.reconcile()
    assert state.answer.value == 42
    assert state.revision == 1


def test_clear_preserves_handle_delete_stales_it() -> None:
    state = XO("app")
    state.item = 1
    handle = state.item
    handle.clear_value()
    handle.set(2)
    handle.delete()
    with pytest.raises(StaleNode):
        handle.set(3)


def test_history_preserves_abandoned_future_as_branch() -> None:
    state = XO.compose("app", history())
    state.item = "one"
    first = state.revision
    state.item = "two"
    second = state.revision
    built = state.capability("history")
    assert isinstance(built, History)

    built.undo()
    assert state.item.value == "one"
    assert state.revision == 3
    state.item = "other"
    third = state.revision

    assert set(built.branches(first)) == {second, third}
    with pytest.raises(AmbiguousRedo):
        built.redo(first)


def test_formula_materialization_is_derived_and_code_is_not_snapshotted() -> None:
    state = XO("app")
    state.source = 2
    state.total.derive(lambda: state.source.value * 3)
    seen: list[object] = []
    state.total.subscribe(seen.append)
    state.source = 4

    assert any(isinstance(event, DerivedEvent) and event.payload == 12 for event in seen)
    blob = state.snapshot_bytes()
    assert b"lambda" not in blob
    assert b"function" not in blob
    assert b"formula" not in blob


def test_root_service_and_public_delegate_to_registry() -> None:
    state = XO.compose("app", service())
    assert isinstance(state.service, ServiceRegistry)

    @state.public.hello
    def hello() -> str:
        return "world"

    assert state.service.call("hello") == "world"
    with pytest.raises(KeyError):
        _ = XO("bare").service
