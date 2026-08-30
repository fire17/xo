from __future__ import annotations

import fnmatch
import threading
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from .codec import DEFAULT_CODEC, Codec
from .events import DerivedEvent, Event, EventGroup, Operation
from .exceptions import (
    ClosedError,
    CommitOutcomeUnknown,
    ConflictError,
    DerivedWriteError,
    InvariantViolation,
    MissingPath,
    RecoveryRequired,
    ReentrantMutationError,
    StaleNode,
    SubscriberError,
)
from .path import Path, PathLike, is_prefix, parse_path, render_path

if TYPE_CHECKING:
    from .capabilities import CapabilityRuntime, CapabilitySpec


class _Missing:
    __slots__ = ()

    def __repr__(self) -> str:
        return "MISSING"

    def __copy__(self) -> _Missing:
        return self

    def __deepcopy__(self, memo: dict[int, object]) -> _Missing:
        return self


MISSING: Final = _Missing()


@dataclass(slots=True)
class _Record:
    value: object = MISSING
    children: dict[str, _Record] = field(default_factory=dict)
    token: object = field(default_factory=object)
    value_revision: int = 0
    attached: bool = True


@dataclass(frozen=True, slots=True)
class CommitPlan:
    operation: Operation
    path: Path
    payload: object
    base_revision: int
    expected_revision: int | None = None


@dataclass(slots=True)
class _Subscriber:
    callback: Callable[[Event | DerivedEvent], object]
    path: Path
    descendants: bool
    pattern: str | None
    include_derived: bool


class Subscription:
    __slots__ = ("_active", "_cancel")

    def __init__(self, cancel: Callable[[], None]) -> None:
        self._cancel = cancel
        self._active = True

    @property
    def active(self) -> bool:
        return self._active

    def cancel(self) -> None:
        if self._active:
            self._active = False
            self._cancel()

    close = cancel

    def __enter__(self) -> Subscription:
        return self

    def __exit__(self, *_: object) -> None:
        self.cancel()


class _EmptyRuntime:
    __slots__ = ()
    validators = ()
    normalizers = ()
    coordinator = None
    observers = ()
    remote_sources = ()
    services: Mapping[str, object] = {}

    def start(self) -> None:
        pass

    def close(self) -> None:
        pass

    def describe(self) -> tuple[()]:
        return ()


EMPTY_RUNTIME = _EmptyRuntime()


class _RootState:
    __slots__ = (
        "closed",
        "codec",
        "dispatch_queue",
        "dispatching",
        "error_hook",
        "event_counter",
        "formula_graph",
        "lock",
        "max_seen",
        "namespace",
        "next_subscriber_id",
        "origin_id",
        "record",
        "recovery_item",
        "recovery_reason",
        "reentrant_budget",
        "revision",
        "root",
        "runtime",
        "seen_events",
        "seen_order",
        "subscribers",
    )

    def __init__(
        self,
        namespace: str,
        *,
        origin_id: int | None,
        codec: Codec,
        error_hook: Callable[[SubscriberError, Event], object] | None,
    ) -> None:
        import secrets

        self.namespace = namespace
        self.origin_id = origin_id if origin_id is not None else secrets.randbits(128)
        self.event_counter = 0
        self.lock = threading.RLock()
        self.record = _Record()
        self.revision = 0
        self.subscribers: dict[int, _Subscriber] = {}
        self.next_subscriber_id = 1
        self.codec = codec
        self.closed = False
        self.error_hook = error_hook
        self.root: XO
        self.runtime: CapabilityRuntime | _EmptyRuntime = EMPTY_RUNTIME
        self.formula_graph: object | None = None
        self.seen_events: dict[int, Event] = {}
        self.seen_order: deque[int] = deque()
        self.max_seen = 10_000
        self.dispatching = False
        self.dispatch_queue: list[Event | EventGroup | DerivedEvent] = []
        self.reentrant_budget = 64
        self.recovery_item: Event | EventGroup | None = None
        self.recovery_reason: BaseException | None = None

    def ensure_open(self) -> None:
        if self.closed:
            raise ClosedError(f"XO namespace {self.namespace!r} is closed")
        if self.recovery_item is not None:
            raise RecoveryRequired(
                f"XO namespace {self.namespace!r} requires authoritative reconciliation"
            ) from self.recovery_reason

    def allocate_event_id(self) -> int:
        self.event_counter += 1
        return (self.origin_id << 64) | self.event_counter

    def remember(self, event: Event) -> None:
        if event.event_id in self.seen_events:
            return
        if len(self.seen_order) == self.max_seen:
            self.seen_events.pop(self.seen_order.popleft(), None)
        self.seen_events[event.event_id] = event
        self.seen_order.append(event.event_id)

    def resolve(self, path: Path, *, create: bool = False) -> _Record | None:
        record = self.record
        for segment in path:
            child = record.children.get(segment)
            if child is None:
                if not create:
                    return None
                child = _Record()
                record.children[segment] = child
            record = child
        return record

    def detach(self, record: _Record) -> None:
        record.attached = False
        for child in record.children.values():
            self.detach(child)


class XO(MutableMapping[str, "XO"]):
    """A lightweight path reference into one atomic value-plus-children tree."""

    __slots__ = ("__weakref__", "_bound_token", "_path", "_root")

    def __init__(
        self,
        namespace: str = "default",
        *,
        value: object = MISSING,
        codec: Codec | None = None,
        origin_id: int | None = None,
        error_hook: Callable[[SubscriberError, Event], object] | None = None,
        capabilities: Iterable[CapabilitySpec] = (),
        _root: _RootState | None = None,
        _path: Path = (),
    ) -> None:
        if _root is None:
            root = _RootState(
                namespace,
                origin_id=origin_id,
                codec=codec or DEFAULT_CODEC,
                error_hook=error_hook,
            )
            object.__setattr__(self, "_root", root)
            object.__setattr__(self, "_path", ())
            object.__setattr__(self, "_bound_token", root.record.token)
            root.root = self
            if value is not MISSING:
                root.codec.dumps(value)
                root.record.value = value
            specs = tuple(capabilities)
            if specs:
                from .capabilities import CapabilityRuntime

                root.runtime = CapabilityRuntime.build(self, specs)
        else:
            object.__setattr__(self, "_root", _root)
            object.__setattr__(self, "_path", _path)
            record = _root.resolve(_path)
            object.__setattr__(self, "_bound_token", None if record is None else record.token)

    @classmethod
    def compose(
        cls,
        namespace: str,
        *capabilities: CapabilitySpec,
        value: object = MISSING,
        codec: Codec | None = None,
    ) -> XO:
        return cls(namespace, value=value, codec=codec, capabilities=capabilities)

    @classmethod
    def recommended(
        cls,
        namespace: str = "default",
        *,
        durability: CapabilitySpec | None = None,
        services: tuple[CapabilitySpec, ...] = (),
        projections: tuple[CapabilitySpec, ...] = (),
        validation: CapabilitySpec | None = None,
    ) -> XO:
        from .profiles import Profile

        return Profile.hybrid(
            durability=durability,
            services=services,
            projections=projections,
            validation=validation,
        ).apply(namespace)

    @classmethod
    def builder(cls, namespace: str = "default") -> XOBuilder:
        return XOBuilder(namespace)

    @property
    def namespace(self) -> str:
        return self._root.namespace

    @property
    def origin_id(self) -> int:
        return self._root.origin_id

    @property
    def revision(self) -> int:
        return self._root.revision
    @property
    def service(self) -> object:
        runtime = self._root.runtime
        if runtime is EMPTY_RUNTIME:
            raise KeyError("service capability is not attached")
        capability = runtime.get("service")
        registry = getattr(capability, "registry", None)
        if registry is None:
            registry = getattr(capability, "service", None)
        if registry is None:
            raise KeyError("service capability has no registry")
        return registry

    @property
    def public(self) -> object:
        return self.service.public


    @property
    def path(self) -> Path:
        return self._path

    @property
    def exists(self) -> bool:
        with self._root.lock:
            return self._root.resolve(self._path) is not None

    @property
    def has_value(self) -> bool:
        graph = self._root.formula_graph
        if graph is not None and graph.contains(self._path):
            return True
        with self._root.lock:
            record = self._record(required=False)
            return record is not None and record.value is not MISSING

    @property
    def value(self) -> object:
        root = self._root
        root.ensure_open()
        graph = root.formula_graph
        if graph is not None and graph.contains(self._path):
            return graph.read(self)
        with root.lock:
            record = self._record(required=True)
            value = record.value
            revision = record.value_revision
        if graph is not None:
            graph.record_read(root, self._path, revision)
        return value

    @value.setter
    def value(self, value: object) -> None:
        self.set(value)

    def get(self, default: object = None) -> object:  # type: ignore[override]
        try:
            value = self.value
        except MissingPath:
            return default
        return default if value is MISSING else value

    def set(self, value: object, *, expected_revision: int | None = None) -> Event:
        root = self._root
        if (
            root.runtime is EMPTY_RUNTIME
            and root.formula_graph is None
            and not root.subscribers
            and not root.dispatching
        ):
            return self._commit_bare_set(value, expected_revision=expected_revision)
        return self._commit(Operation.SET_VALUE, value, expected_revision=expected_revision)

    def clear_value(self, *, expected_revision: int | None = None) -> Event:
        return self._commit(Operation.CLEAR_VALUE, MISSING, expected_revision=expected_revision)

    def delete(self, *, expected_revision: int | None = None) -> Event:
        if not self._path:
            raise ValueError("the root node cannot be deleted")
        return self._commit(Operation.DELETE_SUBTREE, MISSING, expected_revision=expected_revision)

    def at(self, path: PathLike | None = None) -> XO:
        return XO(_root=self._root, _path=self._path + parse_path(path))

    node = at

    def child(self, segment: str) -> XO:
        return XO(_root=self._root, _path=self._path + parse_path((segment,)))

    def ensure(self) -> XO:
        with self._root.lock:
            self._root.ensure_open()
            record = self._root.resolve(self._path, create=True)
            object.__setattr__(self, "_bound_token", record.token)
        return self

    def peek(self, path: PathLike | None = None) -> XO | None:
        target = self.at(path)
        with self._root.lock:
            if self._root.resolve(target.path) is None:
                return None
        return target

    def contains_path(self, path: PathLike | None = None) -> bool:
        return self.peek(path) is not None

    def derive(self, function: Callable[[], object]) -> XO:
        graph = self._root.formula_graph
        if graph is None:
            from .formula import FormulaGraph

            graph = FormulaGraph(self._root)
            self._root.formula_graph = graph
        graph.register(self._path, function)
        return self

    formula = derive

    def undefine(self) -> bool:
        graph = self._root.formula_graph
        return False if graph is None else graph.remove(self._path)

    @property
    def formula_dependencies(self) -> tuple[Path, ...]:
        graph = self._root.formula_graph
        return () if graph is None else graph.dependencies(self._path)

    def subscribe(
        self,
        callback: Callable[[Event | DerivedEvent], object],
        *,
        descendants: bool = False,
        recursive: bool | None = None,
        pattern: str | None = None,
        include_derived: bool = True,
    ) -> Subscription:
        if not callable(callback):
            raise TypeError("subscription callback must be callable")
        if recursive is not None:
            descendants = recursive
        root = self._root
        with root.lock:
            root.ensure_open()
            identifier = root.next_subscriber_id
            root.next_subscriber_id += 1
            root.subscribers[identifier] = _Subscriber(
                callback, self._path, descendants, pattern, include_derived
            )
        graph = root.formula_graph
        stop_formula = None
        if graph is not None and graph.contains(self._path):
            stop_formula = graph.observe(self._path)
            _ = self.value

        def cancel() -> None:
            with root.lock:
                root.subscribers.pop(identifier, None)
            if stop_formula is not None:
                stop_formula()

        return Subscription(cancel)

    def snapshot(self) -> dict[str, object]:
        with self._root.lock:
            self._root.ensure_open()
            record = self._record(required=True)
            snapshot: dict[str, object] = {
                "schema": "xo.snapshot",
                "version": 1,
                "namespace": self.namespace,
                "revision": self.revision,
                "root": self._image(record),
            }
            try:
                history = self.capability("history")
                current = history.current()
            except (KeyError, AttributeError):
                pass
            else:
                snapshot["head_revision"] = current
            return snapshot
    def restore(
        self,
        snapshot: Mapping[str, object] | bytes | bytearray | memoryview | str,
        *,
        expected_revision: int | None = None,
    ) -> Event:
        decoded = self._decode_snapshot(snapshot)
        return self._commit(
            Operation.RESTORE_SUBTREE,
            decoded["root"],
            expected_revision=expected_revision,
        )

    def install_snapshot(
        self,
        snapshot: Mapping[str, object] | bytes | bytearray | memoryview | str,
    ) -> None:
        """Install an authoritative snapshot, including while recovery is required."""
        decoded = self._decode_snapshot(snapshot)
        revision = decoded["revision"]
        assert isinstance(revision, int)
        replacement = self._record_from_image(decoded["root"], revision)
        root = self._root
        with root.lock:
            if root.closed:
                raise ClosedError(f"XO namespace {root.namespace!r} is closed")
            root.detach(root.record)
            root.record = replacement
            root.revision = revision
            root.seen_events.clear()
            root.seen_order.clear()
            root.recovery_item = None
            root.recovery_reason = None
            graph = root.formula_graph
            affected = () if graph is None else graph.invalidate_all()
        derived = self._derive_observed(affected)
        for event in derived:
            self._enqueue_dispatch(event)

    def commit_many(
        self,
        operations: Iterable[CommitPlan | tuple[Operation, PathLike, object]],
        *,
        expected_revision: int | None = None,
    ) -> Event | EventGroup:
        plans: list[CommitPlan] = []
        for item in operations:
            if isinstance(item, CommitPlan):
                plans.append(
                    CommitPlan(
                        item.operation,
                        item.path,
                        item.payload,
                        item.base_revision,
                        item.expected_revision,
                    )
                )
            else:
                operation, path, payload = item
                plans.append(
                    CommitPlan(
                        operation, parse_path(path), payload, self.revision, expected_revision
                    )
                )
        return self._commit_plans(tuple(plans), expected_revision=expected_revision)

    transaction = commit_many

    def reconcile(self) -> bool:
        root = self._root
        with root.lock:
            if root.closed:
                raise ClosedError(f"XO namespace {root.namespace!r} is closed")
            item = root.recovery_item
            if item is None:
                return False
            coordinator = root.runtime.coordinator
            if coordinator is None:
                raise RecoveryRequired("authoritative snapshot installation is required")
            durable = coordinator.reconcile(item)
            if durable:
                try:
                    affected = self._apply_item_locked(item)
                except BaseException as cause:
                    root.recovery_reason = cause
                    raise InvariantViolation(
                        "durable commit could not be applied locally"
                    ) from cause
            else:
                affected = ()
            root.recovery_item = None
            root.recovery_reason = None
        if durable:
            self._observe(item, remote=False)
            derived = self._derive_observed(affected)
            self._enqueue_dispatch(item)
            for event in derived:
                self._enqueue_dispatch(event)
        return durable

    def snapshot_bytes(self) -> bytes:
        return self._root.codec.dumps(self.snapshot())

    @property
    def capabilities(self) -> tuple[dict[str, object], ...]:
        return self._root.runtime.describe()

    def capability(self, key: str) -> object:
        runtime = self._root.runtime
        if runtime is EMPTY_RUNTIME:
            raise KeyError(key)
        return runtime.get(key)

    def start(self) -> XO:
        self._root.runtime.start()
        return self

    def close(self) -> None:
        root = self._root
        with root.lock:
            if root.closed:
                return
            root.closed = True
            root.subscribers.clear()
            runtime = root.runtime
        runtime.close()

    def apply_remote(self, item: Event | EventGroup) -> bool:
        root = self._root
        events = item.events if isinstance(item, EventGroup) else (item,)
        if any(event.namespace != root.namespace for event in events):
            raise ConflictError("remote namespace does not match")
        with root.lock:
            root.ensure_open()
            remembered = tuple(root.seen_events.get(event.event_id) for event in events)
            if all(previous is not None for previous in remembered):
                duplicate = zip(remembered, events, strict=True)
                if all(previous == event for previous, event in duplicate):
                    return False
                conflict = ConflictError(
                    "remote event ID was previously bound to different content"
                )
                root.recovery_item = item
                root.recovery_reason = conflict
                raise RecoveryRequired("remote authored history diverged") from conflict
            if any(previous is not None for previous in remembered):
                raise ConflictError("remote group overlaps only part of an applied commit")
            first = events[0]
            if first.base_revision != root.revision or first.revision != root.revision + 1:
                conflict = ConflictError(
                    f"remote revision {first.base_revision}->{first.revision} is not contiguous "
                    f"with local {root.revision}"
                )
                if first.revision <= root.revision:
                    root.recovery_item = item
                    root.recovery_reason = conflict
                    raise RecoveryRequired("remote authored history diverged") from conflict
                raise conflict
            self._simulate(
                tuple(
                    CommitPlan(event.operation, event.path, event.payload, event.base_revision)
                    for event in events
                ),
                first.revision,
            )
            affected = self._apply_item_locked(item)
        self._observe(item, remote=True)
        derived = self._derive_observed(affected)
        self._enqueue_dispatch(item)
        for event in derived:
            self._enqueue_dispatch(event)
        return True

    def __getattr__(self, key: str) -> XO:
        if key.startswith("_") or key in {"service", "public"}:
            raise AttributeError(key)
        return self.child(key)

    def __setattr__(self, key: str, value: object) -> None:
        if key.startswith("_"):
            object.__setattr__(self, key, value)
            return
        descriptor = getattr(type(self), key, None)
        if descriptor is not None and hasattr(descriptor, "__set__"):
            descriptor.__set__(self, value)
            return
        self.child(key).set(value)

    def __delattr__(self, key: str) -> None:
        if key.startswith("_"):
            object.__delattr__(self, key)
        else:
            self.child(key).delete()

    def __getitem__(self, key: str) -> XO:
        return self.at(key)

    def __setitem__(self, key: str, value: object) -> None:
        self.at(key).set(value)

    def __delitem__(self, key: str) -> None:
        self.at(key).delete()

    def __iter__(self) -> Iterator[str]:
        with self._root.lock:
            record = self._record(required=True)
            return iter(tuple(record.children))

    def __len__(self) -> int:
        with self._root.lock:
            record = self._record(required=True)
            return len(record.children)

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and self.peek(key) is not None

    def __repr__(self) -> str:
        with self._root.lock:
            record = self._record(required=False)
            value = MISSING if record is None else record.value
            children = 0 if record is None else len(record.children)
        return (
            f"XO(path={render_path(self._path) or '<root>'!r}, "
            f"value={value!r}, children={children})"
        )

    def _record(self, *, required: bool) -> _Record | None:
        record = self._root.resolve(self._path)
        bound = self._bound_token
        if bound is not None and (
            record is None or record.token is not bound or not record.attached
        ):
            raise StaleNode(render_path(self._path) or "<root>")
        if record is None:
            if required:
                raise MissingPath(render_path(self._path) or "<root>")
            return None
        if bound is None:
            object.__setattr__(self, "_bound_token", record.token)
        return record

    def _commit_bare_set(
        self,
        payload: object,
        *,
        expected_revision: int | None,
    ) -> Event:
        root = self._root
        with root.lock:
            root.ensure_open()
            if expected_revision is not None and expected_revision != root.revision:
                raise ConflictError(
                    f"expected revision {expected_revision}, observed {root.revision}"
                )
            if isinstance(payload, int) and not isinstance(payload, bool):
                bits = payload.bit_length()
                encoded_bound = 1 if bits == 0 else (bits * 30_103) // 100_000 + 1
                if payload < 0:
                    encoded_bound += 1
                if encoded_bound > root.codec.limits.max_bytes:
                    root.codec.validate(payload)
            elif payload is not None and not isinstance(payload, bool):
                root.codec.validate(payload)
            record = self._record(required=False)
            if record is None:
                record = root.resolve(self._path, create=True)
                assert record is not None
                object.__setattr__(self, "_bound_token", record.token)
            event_id = root.allocate_event_id()
            base_revision = root.revision
            revision = base_revision + 1
            record.value = payload
            record.value_revision = revision
            root.revision = revision
        return Event(
            event_id=event_id,
            namespace=root.namespace,
            origin_id=root.origin_id,
            base_revision=base_revision,
            revision=revision,
            operation=Operation.SET_VALUE,
            path=self._path,
            payload=payload,
        )

    def _commit(
        self,
        operation: Operation,
        payload: object,
        *,
        expected_revision: int | None,
    ) -> Event:
        with self._root.lock:
            self._root.ensure_open()
            self._record(required=operation is Operation.DELETE_SUBTREE)
        item = self._commit_plans(
            (CommitPlan(operation, self._path, payload, self.revision, expected_revision),),
            expected_revision=expected_revision,
        )
        assert isinstance(item, Event)
        return item

    def _commit_plans(
        self,
        plans: tuple[CommitPlan, ...],
        *,
        expected_revision: int | None,
    ) -> Event | EventGroup:
        from .formula import assert_formula_may_not_write

        if not plans:
            raise ValueError("a commit must contain at least one operation")
        assert_formula_may_not_write(self._root)
        root = self._root
        runtime = root.runtime
        with root.lock:
            root.ensure_open()
            if expected_revision is not None and expected_revision != root.revision:
                raise ConflictError(
                    f"expected revision {expected_revision}, observed {root.revision}"
                )
            normalized: list[CommitPlan] = []
            for proposed in plans:
                path = parse_path(proposed.path)
                if proposed.operation is Operation.SET_VALUE:
                    if root.formula_graph is not None and root.formula_graph.contains(path):
                        raise DerivedWriteError(render_path(path))
                    root.codec.validate(proposed.payload)
                elif proposed.operation is Operation.RESTORE_SUBTREE:
                    root.codec.validate(proposed.payload)
                plan = CommitPlan(
                    proposed.operation,
                    path,
                    proposed.payload,
                    root.revision,
                    expected_revision,
                )
                for normalizer in runtime.normalizers:
                    plan = normalizer.normalize(plan)
                for validator in runtime.validators:
                    validator.validate(plan)
                normalized.append(plan)
            if len(normalized) == 1:
                self._validate_single(normalized[0], root.revision + 1)
            else:
                self._simulate(tuple(normalized), root.revision + 1)
            events = tuple(
                Event(
                    event_id=root.allocate_event_id(),
                    namespace=root.namespace,
                    origin_id=root.origin_id,
                    base_revision=root.revision,
                    revision=root.revision + 1,
                    operation=plan.operation,
                    path=plan.path,
                    payload=plan.payload,
                )
                for plan in normalized
            )
            item: Event | EventGroup = events[0] if len(events) == 1 else EventGroup(events)
            if runtime.coordinator is not None:
                try:
                    runtime.coordinator.commit(item)
                except CommitOutcomeUnknown as error:
                    root.recovery_item = item
                    root.recovery_reason = error
                    raise
            try:
                affected = self._apply_item_locked(item)
            except BaseException as cause:
                if runtime.coordinator is not None:
                    root.recovery_item = item
                    root.recovery_reason = cause
                    raise InvariantViolation(
                        "durable commit could not be applied locally"
                    ) from cause
                raise
        self._observe(item, remote=False)
        derived = self._derive_observed(affected)
        self._enqueue_dispatch(item)
        for event in derived:
            self._enqueue_dispatch(event)
        return item

    def _apply_item_locked(self, item: Event | EventGroup) -> tuple[Path, ...]:
        root = self._root
        events = item.events if isinstance(item, EventGroup) else (item,)
        affected: list[Path] = []
        for event in events:
            self._apply(event.operation, event.path, event.payload, event.revision)
            root.remember(event)
            graph = root.formula_graph
            if graph is not None:
                affected.extend(graph.invalidate(event.path))
        root.revision = events[0].revision
        return tuple(dict.fromkeys(affected))

    def _validate_single(self, plan: CommitPlan, revision: int) -> None:
        if plan.operation is Operation.DELETE_SUBTREE:
            if not plan.path:
                raise ValueError("the root node cannot be deleted")
            parent = self._root.resolve(plan.path[:-1])
            if parent is None or plan.path[-1] not in parent.children:
                raise MissingPath(render_path(plan.path))
            return
        if plan.operation is Operation.RESTORE_SUBTREE:
            self._record_from_image(plan.payload, revision)
            return
        if plan.operation not in (Operation.SET_VALUE, Operation.CLEAR_VALUE):
            raise ValueError(f"unsupported operation: {plan.operation!r}")

    def _simulate(self, plans: tuple[CommitPlan, ...], revision: int) -> None:
        shadow = self._record_from_image(self._image(self._root.record), revision)
        for plan in plans:
            shadow = self._apply_record(
                shadow, plan.operation, plan.path, plan.payload, revision, detach=None
            )

    def _observe(self, item: Event | EventGroup, *, remote: bool) -> None:
        for observer in self._root.runtime.observers:
            callback = getattr(observer, "observe_remote", None) if remote else observer.observe
            if callback is None:
                continue
            try:
                callback(item)
            except BaseException:
                continue

    def _apply(
        self,
        operation: Operation,
        path: Path,
        payload: object,
        revision: int,
    ) -> None:
        self._root.record = self._apply_record(
            self._root.record,
            operation,
            path,
            payload,
            revision,
            detach=self._root.detach,
        )

    @classmethod
    def _apply_record(
        cls,
        root_record: _Record,
        operation: Operation,
        path: Path,
        payload: object,
        revision: int,
        *,
        detach: Callable[[_Record], None] | None,
    ) -> _Record:
        def resolve(target: Path, *, create: bool = False) -> _Record | None:
            record = root_record
            for segment in target:
                child = record.children.get(segment)
                if child is None:
                    if not create:
                        return None
                    child = _Record()
                    record.children[segment] = child
                record = child
            return record

        if operation is Operation.DELETE_SUBTREE:
            if not path:
                raise ValueError("the root node cannot be deleted")
            parent = resolve(path[:-1])
            if parent is None or path[-1] not in parent.children:
                raise MissingPath(render_path(path))
            removed = parent.children.pop(path[-1])
            if detach is not None:
                detach(removed)
            return root_record
        if operation is Operation.RESTORE_SUBTREE:
            replacement = cls._record_from_image(payload, revision)
            if not path:
                if detach is not None:
                    detach(root_record)
                return replacement
            parent = resolve(path[:-1], create=True)
            assert parent is not None
            old = parent.children.get(path[-1])
            if old is not None and detach is not None:
                detach(old)
            parent.children[path[-1]] = replacement
            return root_record
        record = resolve(path, create=True)
        assert record is not None
        if operation is Operation.SET_VALUE:
            record.value = payload
        elif operation is Operation.CLEAR_VALUE:
            record.value = MISSING
        else:
            raise ValueError(f"unsupported operation: {operation!r}")
        record.value_revision = revision
        return root_record

    def _enqueue_dispatch(self, item: Event | EventGroup | DerivedEvent) -> None:
        root = self._root
        with root.lock:
            root.dispatch_queue.append(item)
            if root.dispatching:
                if len(root.dispatch_queue) > root.reentrant_budget:
                    root.dispatch_queue.pop()
                    raise ReentrantMutationError("subscriber mutation budget exceeded")
                return
            root.dispatching = True
        try:
            while True:
                with root.lock:
                    if not root.dispatch_queue:
                        return
                    current = root.dispatch_queue.pop(0)
                    subscribers = tuple(root.subscribers.values())
                events = current.events if isinstance(current, EventGroup) else (current,)
                for event in events:
                    rendered = render_path(event.path)
                    for subscriber in subscribers:
                        if isinstance(event, DerivedEvent) and not subscriber.include_derived:
                            continue
                        matches = (
                            is_prefix(subscriber.path, event.path)
                            if subscriber.descendants
                            else subscriber.path == event.path
                        )
                        if not matches or (
                            subscriber.pattern is not None
                            and not fnmatch.fnmatchcase(rendered, subscriber.pattern)
                        ):
                            continue
                        try:
                            subscriber.callback(event)
                        except BaseException as cause:
                            if root.error_hook is not None:
                                root.error_hook(SubscriberError(subscriber.callback, cause), event)
        finally:
            with root.lock:
                root.dispatching = False

    def _derive_observed(self, affected: tuple[Path, ...]) -> tuple[DerivedEvent, ...]:
        graph = self._root.formula_graph
        if graph is None:
            return ()
        return tuple(graph.materialize(path) for path in graph.eager_targets(affected))

    @classmethod
    def _image(cls, record: _Record) -> dict[str, object]:
        image: dict[str, object] = {
            "$children": [
                [key, cls._image(child)] for key, child in record.children.items()
            ]
        }
        if record.value is not MISSING:
            image["$value"] = record.value
        return image

    @classmethod
    def _record_from_image(cls, image: object, revision: int) -> _Record:
        if not isinstance(image, dict) or set(image) - {"$value", "$children"}:
            raise ValueError("invalid node image")
        if "$children" not in image:
            raise ValueError("invalid node image")
        record = _Record(value=image.get("$value", MISSING), value_revision=revision)
        children = image["$children"]
        if not isinstance(children, list):
            raise ValueError("invalid node children image")
        for item in children:
            if not isinstance(item, list) or len(item) != 2 or not isinstance(item[0], str):
                raise ValueError("invalid node child image")
            key = parse_path((item[0],))[0]
            if key in record.children:
                raise ValueError(f"duplicate node child: {key!r}")
            record.children[key] = cls._record_from_image(item[1], revision)
        return record
    def _decode_snapshot(
        self,
        snapshot: Mapping[str, object] | bytes | bytearray | memoryview | str,
    ) -> Mapping[str, object]:
        value = self._root.codec.loads(snapshot) if not isinstance(snapshot, Mapping) else snapshot
        if not isinstance(value, Mapping):
            raise ValueError("snapshot must be a mapping")
        if value.get("schema") != "xo.snapshot" or value.get("version") != 1:
            raise ValueError("unsupported snapshot schema or version")
        if value.get("namespace") != self.namespace:
            raise ConflictError("snapshot namespace does not match")
        revision = value.get("revision")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
            raise ValueError("snapshot revision must be a non-negative integer")
        if "root" not in value:
            raise ValueError("snapshot root is missing")
        self._record_from_image(value["root"], revision)
        return value



class XOBuilder:
    __slots__ = ("_codec", "_specs", "_value", "namespace")

    def __init__(self, namespace: str) -> None:
        self.namespace = namespace
        self._specs: list[CapabilitySpec] = []
        self._value: object = MISSING
        self._codec: Codec | None = None

    def use(self, capability: CapabilitySpec) -> XOBuilder:
        self._specs.append(capability)
        return self

    def value(self, value: object) -> XOBuilder:
        self._value = value
        return self

    def codec(self, codec: Codec) -> XOBuilder:
        self._codec = codec
        return self

    def build(self) -> XO:
        return XO(
            self.namespace,
            value=self._value,
            codec=self._codec,
            capabilities=tuple(self._specs),
        )
