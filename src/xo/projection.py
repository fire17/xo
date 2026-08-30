from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from .capabilities import BuildContext, CapabilitySpec, NullCapability, Observer
from .events import Event, EventGroup
from .path import Path, PathLike, is_prefix, parse_path


@dataclass(slots=True)
class Projection(NullCapability, Observer):
    context: BuildContext
    sink: Callable[[Event | EventGroup], object]
    prefixes: tuple[Path, ...] = ()
    _seen: set[int] = field(default_factory=set)
    _seen_order: list[int] = field(default_factory=list)
    max_seen: int = 10_000

    def observe(self, item: Event | EventGroup) -> None:
        events = item.events if isinstance(item, EventGroup) else (item,)
        event_id = events[0].event_id
        if event_id in self._seen:
            return
        if self.prefixes and not any(
            is_prefix(prefix, event.path)
            for prefix in self.prefixes
            for event in events
        ):
            return
        self._seen.add(event_id)
        self._seen_order.append(event_id)
        overflow = len(self._seen_order) - self.max_seen
        if overflow > 0:
            for expired in self._seen_order[:overflow]:
                self._seen.discard(expired)
            del self._seen_order[:overflow]
        self.sink(item)


def projection(
    sink: Callable[[Event | EventGroup], object],
    *,
    prefixes: tuple[PathLike, ...] = (),
    key: str = "projection",
    kind: str = "projection",
) -> CapabilitySpec:
    canonical = tuple(parse_path(prefix) for prefix in prefixes)
    return CapabilitySpec(
        key=key,
        factory=lambda context: Projection(context, sink, canonical),
        provides=frozenset({"projection", kind}),
        after=frozenset({"durability", "history"}),
        configuration={"kind": kind, "prefixes": canonical},
    )
