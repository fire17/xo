from __future__ import annotations

from typing import Protocol

from ..events import Event, EventGroup


class Backend(Protocol):
    """Strict durability coordinator for one complete semantic commit."""

    strict: bool

    def commit(self, event: Event | EventGroup) -> object:
        """Durably accept or raise before local visibility."""

    def reconcile(self, event: Event | EventGroup) -> bool:
        """Resolve an unknown commit outcome by revision and event identity."""

    def close(self) -> None:
        """Release owned resources idempotently."""
