from __future__ import annotations

from dataclasses import dataclass

import pytest

from xo import CapabilitySpec, NullCapability
from xo.conformance import Check, assert_conformant, check_capability_permutations


@dataclass
class Empty(NullCapability):
    name: str


def spec(name: str, *, after: tuple[str, ...] = ()) -> CapabilitySpec:
    return CapabilitySpec(
        key=name,
        factory=lambda context: Empty(name),
        provides=frozenset({name}),
        after=frozenset(after),
    )


def test_permutation_check_uses_compiled_profile_contract() -> None:
    checks = check_capability_permutations((spec("a"), spec("b", after=("a",))))
    assert checks == (Check("deterministic-profile-order", True, ""),)
    assert_conformant(checks)


def test_assert_conformant_names_every_failed_check() -> None:
    with pytest.raises(AssertionError, match="broken: evidence"):
        assert_conformant((Check("broken", False, "evidence"),))
