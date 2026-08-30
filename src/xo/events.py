from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .path import Path


class Operation(StrEnum):
    SET_VALUE = "set_value"
    CLEAR_VALUE = "clear_value"
    DELETE_SUBTREE = "delete_subtree"
    RESTORE_SUBTREE = "restore_subtree"


@dataclass(frozen=True, slots=True)
class Diagnostics:
    timestamp_ns: int | None = None
    trace_id: str | None = None
    metadata: tuple[tuple[str, object], ...] = ()


@dataclass(frozen=True, slots=True)
class Event:
    event_id: int
    namespace: str
    origin_id: int
    base_revision: int
    revision: int
    operation: Operation
    path: Path
    payload: object
    diagnostics: Diagnostics | None = None

    def __post_init__(self) -> None:
        if self.event_id < 0 or self.origin_id < 0:
            raise ValueError("event identities cannot be negative")
        if not self.namespace:
            raise ValueError("event namespace cannot be empty")
        if self.base_revision < 0 or self.revision != self.base_revision + 1:
            raise ValueError("an event must advance its base revision exactly once")
        if not isinstance(self.operation, Operation):
            raise TypeError("event operation must be an Operation")
        if not isinstance(self.path, tuple):
            raise TypeError("event path must be a canonical tuple")
        for segment in self.path:
            if not isinstance(segment, str) or not segment:
                raise TypeError("event path must be a canonical tuple")


@dataclass(frozen=True, slots=True)
class EventGroup:
    events: tuple[Event, ...]

    def __post_init__(self) -> None:
        if len(self.events) < 2:
            raise ValueError("an event group must contain at least two events")
        first = self.events[0]
        seen_ids: set[int] = set()
        for event in self.events:
            if (
                event.namespace != first.namespace
                or event.origin_id != first.origin_id
                or event.base_revision != first.base_revision
                or event.revision != first.revision
            ):
                raise ValueError("grouped events must share commit identity")
            if event.event_id in seen_ids:
                raise ValueError("grouped events must have distinct event IDs")
            seen_ids.add(event.event_id)

    @property
    def transaction_id(self) -> int:
        return self.events[0].event_id


Transaction = EventGroup


@dataclass(frozen=True, slots=True)
class DerivedEvent:
    namespace: str
    origin_id: int
    cause_revision: int
    path: Path
    formula_generation: int
    status: str
    payload: object
    diagnostics: Diagnostics | None = None

    def __post_init__(self) -> None:
        if not self.namespace:
            raise ValueError("derived-event namespace cannot be empty")
        if self.origin_id < 0 or self.cause_revision < 0:
            raise ValueError("derived-event identity and revision cannot be negative")
        if self.formula_generation < 1:
            raise ValueError("formula generation must be positive")
        if not self.status:
            raise ValueError("derived-event status cannot be empty")
        if not isinstance(self.path, tuple) or not all(
            isinstance(segment, str) and segment for segment in self.path
        ):
            raise TypeError("derived-event path must be a canonical tuple")
