from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from itertools import permutations

from .capabilities import CapabilitySpec, compile_profile
from .core import XO
from .events import Event, EventGroup


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    passed: bool
    detail: str = ""


def check_capability_permutations(specs: Iterable[CapabilitySpec]) -> tuple[Check, ...]:
    materialized = tuple(specs)
    expected = compile_profile(materialized).order
    failures: list[str] = []
    for candidate in permutations(materialized):
        observed = compile_profile(candidate).order
        if observed != expected:
            failures.append(f"{tuple(spec.key for spec in candidate)!r} -> {observed!r}")
    return (
        Check(
            "deterministic-profile-order",
            not failures,
            "; ".join(failures),
        ),
    )


def check_backend_atomicity(
    backend_spec: CapabilitySpec,
    fail: Callable[[], None],
) -> tuple[Check, ...]:
    state = XO.compose("conformance", backend_spec)
    state.value_node = "before"
    revision = state.revision
    fail()
    failed = False
    try:
        state.value_node = "after"
    except BaseException:
        failed = True
    return (
        Check("backend-failure-raised", failed),
        Check("backend-failure-kept-value", state.value_node.value == "before"),
        Check("backend-failure-kept-revision", state.revision == revision),
    )


def check_observer_exactly_once(
    spec: CapabilitySpec,
    observed: list[Event | EventGroup],
) -> tuple[Check, ...]:
    state = XO.compose("conformance", spec)
    accepted = state.value_node.set("value")
    matches = [
        item
        for item in observed
        if isinstance(item, Event) and item.event_id == accepted.event_id
    ]
    return (Check("observer-exactly-once", len(matches) == 1, f"observed={len(matches)}"),)


def assert_conformant(checks: Iterable[Check]) -> None:
    failed = tuple(check for check in checks if not check.passed)
    if failed:
        detail = "; ".join(
            f"{check.name}: {check.detail or 'failed'}" for check in failed
        )
        raise AssertionError(detail)
