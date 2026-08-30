from __future__ import annotations

from xo import XO, History, Service, projection, validation


def test_recommended_profile_is_ordinary_capability_composition() -> None:
    projected = []
    state = XO.recommended(
        "app",
        projections=(projection(projected.append, key="web", kind="web"),),
        validation=validation({"count": lambda value: isinstance(value, int) or 1 / 0}),
    )
    state.count = 1

    keys = {capability["key"] for capability in state.capabilities}
    assert keys == {"validation", "history", "service", "web"}
    assert isinstance(state.capability("history"), History)
    assert isinstance(state.capability("service"), Service)
    assert [event.path for event in projected] == [("count",)]
