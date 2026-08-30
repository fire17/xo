from __future__ import annotations

import threading

import pytest

from xo import MISSING, XO, FormulaCycleError, FormulaMutationError, MissingPath, StaleNode


def test_value_and_children_coexist() -> None:
    state = XO("app")
    state.user = "Tami"
    state.user.preferences.theme = "dark"
    state.user = "Tami 2"

    assert state.user.value == "Tami 2"
    assert state.user.preferences.theme.value == "dark"


def test_fluent_reads_are_virtual_and_non_mutating() -> None:
    state = XO("app")
    revision = state.revision
    probe = state.user.profile.name

    assert not probe.exists
    assert state.peek("user.profile.name") is None
    assert state.revision == revision
    assert len(state) == 0
    with pytest.raises(MissingPath):
        _ = probe.value


def test_none_missing_clear_and_delete_are_distinct() -> None:
    state = XO("app")
    state.user.name = None
    state.user.name.meta = "kept"
    assert state.user.name.has_value
    assert state.user.name.value is None

    state.user.name.clear_value()
    assert state.user.name.value is MISSING
    assert state.user.name.meta.value == "kept"

    stale = state.user.name
    stale.delete()
    assert state.peek("user.name") is None
    with pytest.raises(StaleNode):
        stale.set("wrong")


def test_subscribers_observe_committed_state_and_isolate_errors() -> None:
    errors: list[BaseException] = []
    seen: list[tuple[int, object]] = []
    state = XO("app", error_hook=lambda error, event: errors.append(error))

    state.user.subscribe(lambda event: (_ for _ in ()).throw(RuntimeError("boom")))
    state.user.subscribe(lambda event: seen.append((event.revision, state.user.value)))
    event = state.user.set("Tami")

    assert seen == [(event.revision, "Tami")]
    assert len(errors) == 1


def test_lazy_formula_dependencies_and_single_flight() -> None:
    state = XO("app")
    state.price = 12
    state.quantity = 3
    calls = 0
    entered = threading.Event()
    release = threading.Event()

    def total() -> int:
        nonlocal calls
        calls += 1
        entered.set()
        release.wait(timeout=2)
        return state.price.value * state.quantity.value

    state.total.derive(total)
    results: list[int] = []
    threads = [threading.Thread(target=lambda: results.append(state.total.value)) for _ in range(8)]
    for thread in threads:
        thread.start()
    assert entered.wait(timeout=2)
    release.set()
    for thread in threads:
        thread.join(timeout=2)

    assert calls == 1
    assert results == [36] * 8
    assert set(state.total.formula_dependencies) == {("price",), ("quantity",)}

    state.price = 20
    assert calls == 1
    assert state.total.value == 60
    assert calls == 2


def test_dynamic_formula_edges_are_replaced() -> None:
    state = XO("app")
    state.use_left = True
    state.left = 1
    state.right = 2
    state.selected.derive(
        lambda: state.left.value if state.use_left.value else state.right.value
    )

    assert state.selected.value == 1
    assert ("left",) in state.selected.formula_dependencies
    state.use_left = False
    assert state.selected.value == 2
    assert ("right",) in state.selected.formula_dependencies
    assert ("left",) not in state.selected.formula_dependencies


def test_formula_cycles_and_tree_mutations_fail() -> None:
    state = XO("app")
    state.a.derive(lambda: state.b.value + 1)
    state.b.derive(lambda: state.a.value + 1)
    with pytest.raises(FormulaCycleError):
        _ = state.a.value

    state.c.derive(lambda: state.source.set(3))
    with pytest.raises(FormulaMutationError):
        _ = state.c.value


def test_snapshot_is_deterministic_and_keeps_order() -> None:
    state = XO("app")
    state.z = 1
    state.a = None
    first = state.snapshot_bytes()
    second = state.snapshot_bytes()

    assert first == second
    assert b'"namespace":"app"' in first
    assert list(state) == ["z", "a"]


def test_install_snapshot_keeps_canonical_root_handle_live() -> None:
    state = XO("app")
    state.before = 1
    state.install_snapshot(
        {
            "schema": "xo.snapshot",
            "version": 1,
            "namespace": "app",
            "revision": 7,
            "root": {
                "$value": "root",
                "$children": [["after", {"$value": 2, "$children": []}]],
            },
        }
    )

    assert state.value == "root"
    assert state.after.value == 2
    state.live = 3
    assert state.live.value == 3
    assert state.revision == 8
