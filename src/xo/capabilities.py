from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol

from .exceptions import (
    CapabilityConflictError,
    CapabilityLifecycleError,
    CapabilityOrderError,
)

if TYPE_CHECKING:
    from .core import XO, CommitPlan
    from .events import Event, EventGroup


CapabilityError = CapabilityLifecycleError
CapabilityDependencyError = CapabilityOrderError


class Hook(StrEnum):
    VALIDATE = "validate"
    NORMALIZE = "normalize"
    COMMIT = "commit"
    OBSERVE = "observe"
    REMOTE_SOURCE = "remote_source"
    SERVICE = "service"


@dataclass(frozen=True, slots=True)
class CapabilitySpec:
    """Declarative root capability; inert until compiled and built."""

    key: str
    factory: Callable[[BuildContext], Capability]
    provides: frozenset[str] = frozenset()
    requires: frozenset[str] = frozenset()
    conflicts: frozenset[str] = frozenset()
    before: frozenset[str] = frozenset()
    after: frozenset[str] = frozenset()
    singleton_roles: frozenset[str] = frozenset()
    configuration: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.key or self.key.startswith("_"):
            raise CapabilityError(f"invalid capability key: {self.key!r}")
        object.__setattr__(self, "configuration", MappingProxyType(dict(self.configuration)))


@dataclass(frozen=True, slots=True)
class BuildContext:
    namespace: str
    root: XO
    specs: tuple[CapabilitySpec, ...]
    services: Mapping[str, object]


class Capability(Protocol):
    """Runtime lifecycle. prepare must not publish externally visible state."""

    def prepare(self) -> None: ...

    def start(self) -> None: ...

    def close(self) -> None: ...


class Validator(Protocol):
    def validate(self, plan: CommitPlan) -> None: ...


class Normalizer(Protocol):
    def normalize(self, plan: CommitPlan) -> CommitPlan: ...


class CommitCoordinator(Protocol):
    strict: bool

    def commit(self, event: Event | EventGroup) -> object: ...

    def reconcile(self, event: Event | EventGroup) -> bool: ...


class Observer(Protocol):
    def observe(self, event: Event | EventGroup) -> None: ...


class NullCapability:
    __slots__ = ()

    def prepare(self) -> None:
        pass

    def start(self) -> None:
        pass

    def close(self) -> None:
        pass


@dataclass(frozen=True, slots=True)
class CompiledProfile:
    specs: tuple[CapabilitySpec, ...]
    order: tuple[str, ...]
    provided: frozenset[str]

    @property
    def configuration(self) -> Mapping[str, Mapping[str, object]]:
        return MappingProxyType({spec.key: spec.configuration for spec in self.specs})


class CapabilityRuntime:
    """A transactionally installed, root-scoped collection of capabilities."""

    __slots__ = (
        "_closed",
        "_instances",
        "_started",
        "coordinator",
        "normalizers",
        "observers",
        "profile",
        "remote_sources",
        "services",
        "validators",
    )

    def __init__(
        self,
        profile: CompiledProfile,
        instances: Sequence[tuple[str, Capability]],
    ) -> None:
        self.profile = profile
        self._instances = tuple(instances)
        self.validators = tuple(
            capability for _, capability in instances if hasattr(capability, "validate")
        )
        self.normalizers = tuple(
            capability for _, capability in instances if hasattr(capability, "normalize")
        )
        coordinators = [
            capability
            for _, capability in instances
            if hasattr(capability, "commit") and hasattr(capability, "reconcile")
        ]
        if len(coordinators) > 1:
            raise CapabilityConflictError("multiple commit coordinators were built")
        self.coordinator = coordinators[0] if coordinators else None
        self.observers = tuple(
            capability for _, capability in instances if hasattr(capability, "observe")
        )
        self.remote_sources = tuple(
            capability
            for _, capability in instances
            if hasattr(capability, "set_remote_sink")
        )
        self.services = MappingProxyType(
            {
                key: capability
                for key, capability in instances
                if hasattr(capability, "service") or "service" in _spec_for(profile, key).provides
            }
        )
        self._started = False
        self._closed = False

    @classmethod
    def build(cls, root: XO, specs: Iterable[CapabilitySpec]) -> CapabilityRuntime:
        profile = compile_profile(specs)
        services: dict[str, object] = {}
        context = BuildContext(
            namespace=root.namespace,
            root=root,
            specs=profile.specs,
            services=MappingProxyType(services),
        )
        built: list[tuple[str, Capability]] = []
        prepared: list[tuple[str, Capability]] = []
        try:
            by_key = {spec.key: spec for spec in profile.specs}
            for key in profile.order:
                capability = by_key[key].factory(context)
                if not all(
                    callable(getattr(capability, method, None))
                    for method in ("prepare", "start", "close")
                ):
                    raise CapabilityLifecycleError(
                        f"capability {key!r} does not implement prepare/start/close"
                    )
                built.append((key, capability))
                services[key] = capability
            for item in built:
                prepared.append(item)
                item[1].prepare()
            runtime = cls(profile, built)
            return runtime
        except BaseException as error:
            failures = _close_reverse(prepared or built)
            if failures:
                raise CapabilityLifecycleError(
                    f"capability build failed ({error}); rollback failures: {failures!r}"
                ) from error
            if isinstance(error, CapabilityError):
                raise
            raise CapabilityLifecycleError(f"capability build failed: {error}") from error

    def start(self) -> None:
        if self._closed:
            raise CapabilityLifecycleError("cannot start a closed capability runtime")
        if self._started:
            return
        started: list[tuple[str, Capability]] = []
        try:
            for item in self._instances:
                item[1].start()
                started.append(item)
            self._started = True
        except BaseException as error:
            failures = _close_reverse(self._instances)
            self._closed = True
            message = f"capability start failed: {error}"
            if failures:
                message += f"; rollback failures: {failures!r}"
            raise CapabilityLifecycleError(message) from error

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        failures = _close_reverse(self._instances)
        if failures:
            raise CapabilityLifecycleError(f"capability close failures: {failures!r}")

    def get(self, key: str) -> Capability:
        for current, capability in self._instances:
            if current == key:
                return capability
        raise KeyError(key)

    def describe(self) -> tuple[dict[str, object], ...]:
        return tuple(
            {
                "key": spec.key,
                "provides": sorted(spec.provides),
                "requires": sorted(spec.requires),
                "configuration": dict(spec.configuration),
            }
            for spec in self.profile.specs
        )


EMPTY_PROFILE = CompiledProfile(specs=(), order=(), provided=frozenset())


def compile_profile(specs: Iterable[CapabilitySpec]) -> CompiledProfile:
    provided_specs = tuple(specs)
    if not provided_specs:
        return EMPTY_PROFILE
    by_key: dict[str, CapabilitySpec] = {}
    providers: dict[str, list[str]] = defaultdict(list)
    roles: dict[str, list[str]] = defaultdict(list)
    for spec in provided_specs:
        if spec.key in by_key:
            raise CapabilityConflictError(f"duplicate capability key: {spec.key!r}")
        by_key[spec.key] = spec
        for provision in spec.provides:
            providers[provision].append(spec.key)
        for role in spec.singleton_roles:
            roles[role].append(spec.key)
    for role, keys in roles.items():
        if len(keys) > 1:
            raise CapabilityConflictError(
                f"singleton role {role!r} provided by {', '.join(sorted(keys))}"
            )
    all_provisions = frozenset(providers)
    for spec in provided_specs:
        missing = spec.requires - all_provisions
        if missing:
            raise CapabilityDependencyError(
                f"capability {spec.key!r} requires missing provisions: "
                f"{', '.join(sorted(missing))}"
            )
        conflicts = set(spec.conflicts & (all_provisions | by_key.keys()))
        conflicts.discard(spec.key)
        conflicts -= spec.provides
        if conflicts:
            raise CapabilityConflictError(
                f"capability {spec.key!r} conflicts with: {', '.join(sorted(conflicts))}"
            )

    edges: dict[str, set[str]] = {key: set() for key in by_key}
    indegree: dict[str, int] = {key: 0 for key in by_key}

    def add_edge(before: str, after: str) -> None:
        if before == after or after in edges[before]:
            return
        edges[before].add(after)
        indegree[after] += 1

    for spec in provided_specs:
        for requirement in spec.requires:
            for provider in providers[requirement]:
                add_edge(provider, spec.key)
        for target in spec.before:
            for target_key in _resolve_targets(target, by_key, providers):
                add_edge(spec.key, target_key)
        for target in spec.after:
            for target_key in _resolve_targets(target, by_key, providers):
                add_edge(target_key, spec.key)

    ready = sorted(key for key, degree in indegree.items() if degree == 0)
    order: list[str] = []
    while ready:
        key = ready.pop(0)
        order.append(key)
        for target in sorted(edges[key]):
            indegree[target] -= 1
            if indegree[target] == 0:
                ready.append(target)
                ready.sort()
    if len(order) != len(by_key):
        cycle = sorted(key for key, degree in indegree.items() if degree > 0)
        raise CapabilityDependencyError(
            f"capability ordering cycle among: {', '.join(cycle)}"
        )
    ordered_specs = tuple(by_key[key] for key in order)
    return CompiledProfile(
        specs=ordered_specs,
        order=tuple(order),
        provided=all_provisions,
    )


def _resolve_targets(
    target: str,
    by_key: Mapping[str, CapabilitySpec],
    providers: Mapping[str, Sequence[str]],
) -> tuple[str, ...]:
    if target in by_key:
        return (target,)
    if target in providers:
        return tuple(providers[target])
    # Ordering constraints may name optional provisions absent from this profile.
    return ()

def _close_reverse(
    instances: Iterable[tuple[str, Capability]],
) -> tuple[tuple[str, BaseException], ...]:
    failures: list[tuple[str, BaseException]] = []
    for key, capability in reversed(tuple(instances)):
        try:
            capability.close()
        except BaseException as error:
            failures.append((key, error))
    return tuple(failures)


def _spec_for(profile: CompiledProfile, key: str) -> CapabilitySpec:
    for spec in profile.specs:
        if spec.key == key:
            return spec
    raise KeyError(key)
