from __future__ import annotations

from dataclasses import dataclass
from itertools import permutations

import pytest

from xo import (
    XO,
    CapabilityConflictError,
    CapabilityOrderError,
    CapabilitySpec,
    NullCapability,
    compile_profile,
)


@dataclass
class Recorder(NullCapability):
    name: str
    log: list[str]
    fail: str | None = None

    def prepare(self) -> None:
        self.log.append(f"prepare:{self.name}")
        if self.fail == "prepare":
            raise RuntimeError(self.name)

    def start(self) -> None:
        self.log.append(f"start:{self.name}")
        if self.fail == "start":
            raise RuntimeError(self.name)

    def close(self) -> None:
        self.log.append(f"close:{self.name}")


def spec(
    name: str,
    log: list[str],
    *,
    provides: tuple[str, ...] = (),
    requires: tuple[str, ...] = (),
    before: tuple[str, ...] = (),
    after: tuple[str, ...] = (),
    conflicts: tuple[str, ...] = (),
    singleton_roles: tuple[str, ...] = (),
    fail: str | None = None,
) -> CapabilitySpec:
    return CapabilitySpec(
        key=name,
        factory=lambda context, n=name, f=fail: Recorder(n, log, f),
        provides=frozenset(provides),
        requires=frozenset(requires),
        before=frozenset(before),
        after=frozenset(after),
        conflicts=frozenset(conflicts),
        singleton_roles=frozenset(singleton_roles),
    )


def test_profile_order_is_independent_of_input_order() -> None:
    log: list[str] = []
    specs = (
        spec("history", log, provides=("history",)),
        spec("redis", log, provides=("durability",), singleton_roles=("durability",)),
        spec("web", log, requires=("durability",), after=("history",)),
    )
    orders = {compile_profile(order).order for order in permutations(specs)}
    assert orders == {("history", "redis", "web")}


def test_invalid_profiles_fail_before_factory_side_effects() -> None:
    log: list[str] = []
    with pytest.raises(CapabilityOrderError):
        XO.compose("app", spec("web", log, requires=("durability",)))
    assert log == []

    with pytest.raises(CapabilityConflictError):
        XO.compose(
            "app",
            spec("redis-a", log, singleton_roles=("durability",)),
            spec("redis-b", log, singleton_roles=("durability",)),
        )
    assert log == []


def test_prepare_failure_rolls_back_in_reverse() -> None:
    log: list[str] = []
    with pytest.raises(Exception, match="capability build failed"):
        XO.compose(
            "app",
            spec("a", log),
            spec("b", log, after=("a",), fail="prepare"),
        )
    assert log == ["prepare:a", "prepare:b", "close:b", "close:a"]


def test_start_and_close_are_explicit_ordered_and_idempotent() -> None:
    log: list[str] = []
    state = XO.compose("app", spec("a", log), spec("b", log, after=("a",)))
    assert log == ["prepare:a", "prepare:b"]

    state.start().start()
    state.close()
    state.close()
    assert log == [
        "prepare:a",
        "prepare:b",
        "start:a",
        "start:b",
        "close:b",
        "close:a",
    ]


def test_children_share_one_root_runtime() -> None:
    log: list[str] = []
    state = XO.compose("app", spec("history", log, provides=("history",)))
    state.data_child.value = 3

    assert state.capabilities == state.data_child.capabilities
    assert state.capability("history") is state.data_child.capability("history")
