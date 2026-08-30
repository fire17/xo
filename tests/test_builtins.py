from __future__ import annotations

import pytest

from xo import XO
from xo.history import History, history
from xo.validation import validation


def test_history_and_validation_fuse_over_one_commit() -> None:
    state = XO.compose(
        "app",
        validation({"age": lambda value: value >= 0 or (_ for _ in ()).throw(ValueError())}),
        history(),
    )
    state.age = 3

    built_history = state.capability("history")
    assert isinstance(built_history, History)
    assert built_history.current() == state.revision == 1

    with pytest.raises(ValueError):
        state.age = -1
    assert state.age.value == 3
    assert state.revision == 1
    assert built_history.current() == 1


def test_future_pydantic_style_validator_needs_no_core_dependency() -> None:
    observed: list[object] = []
    state = XO.compose(
        "app",
        validation({("user",): lambda value: observed.append(value)}),
    )
    state.user = {"name": "Tami"}
    assert observed == [{"name": "Tami"}]
