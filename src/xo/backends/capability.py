from __future__ import annotations

from dataclasses import dataclass

from ..capabilities import BuildContext, CapabilitySpec, CommitCoordinator
from ..events import Event, EventGroup
from .base import Backend


@dataclass(slots=True)
class BackendCapability(CommitCoordinator):
    context: BuildContext
    backend: Backend
    strict: bool = True

    def prepare(self) -> None:
        bind = getattr(self.backend, "bind", None)
        if bind is not None:
            bind(self.context.namespace)
        set_remote_sink = getattr(self.backend, "set_remote_sink", None)
        if set_remote_sink is not None:
            set_remote_sink(
                self.context.root.apply_remote,
                revision=self.context.root.revision,
                origin_id=self.context.root.origin_id,
                snapshot_sink=self.context.root.install_snapshot,
            )
        set_snapshot_provider = getattr(self.backend, "set_snapshot_provider", None)
        if set_snapshot_provider is not None:
            set_snapshot_provider(self.context.root.snapshot)
        prepare = getattr(self.backend, "prepare", None)
        if prepare is not None:
            prepare()

    def start(self) -> None:
        start = getattr(self.backend, "start", None)
        if start is not None:
            start()

    def commit(self, event: Event | EventGroup) -> object:
        return self.backend.commit(event)

    def reconcile(self, event: Event | EventGroup) -> bool:
        return self.backend.reconcile(event)

    def close(self) -> None:
        self.backend.close()


def backend(
    instance: Backend,
    *,
    key: str = "backend",
    provides: frozenset[str] = frozenset(),
) -> CapabilitySpec:
    return CapabilitySpec(
        key=key,
        factory=lambda context: BackendCapability(context, instance, instance.strict),
        provides=frozenset({"durability", *provides}),
        singleton_roles=frozenset({"durability"}),
        after=frozenset({"validation"}),
        before=frozenset({"history", "projection"}),
        configuration={"strict": instance.strict, "type": type(instance).__qualname__},
    )
