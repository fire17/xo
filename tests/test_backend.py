from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from xo import XO, Event, EventGroup
from xo.backends import backend


@dataclass
class MemoryBackend:
    strict: bool = True
    events: list[Event | EventGroup] = field(default_factory=list)
    fail: bool = False
    closed: bool = False

    def commit(self, event: Event | EventGroup) -> None:
        if self.fail:
            raise OSError("unavailable")
        self.events.append(event)

    def reconcile(self, event: Event | EventGroup) -> bool:
        return event in self.events

    def close(self) -> None:
        self.closed = True


def test_strict_backend_runs_before_local_visibility() -> None:
    memory = MemoryBackend()
    state = XO.compose("app", backend(memory))
    state.status = "ready"

    assert len(memory.events) == 1
    assert memory.events[0].base_revision == 0
    assert state.status.value == "ready"

    memory.fail = True
    with pytest.raises(OSError):
        state.status = "broken"
    assert state.status.value == "ready"
    assert state.revision == 1

    state.close()
    assert memory.closed
