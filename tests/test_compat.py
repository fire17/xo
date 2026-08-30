from __future__ import annotations

import pytest

from xo import MISSING, XO
from xo.compat import (
    CompatibilityError,
    Fresh,
    FreshClient,
    FreshRedis,
    FreshZero,
    ServiceFacade,
    xoBenedict,
    xoBranch,
    xoRedis,
)
from xo.events import Event, EventGroup
from xo.history import History


class RecordingBackend:
    strict = True

    def __init__(self) -> None:
        self.events: list[Event | EventGroup] = []
        self.closed = 0

    def commit(self, event: Event | EventGroup) -> None:
        self.events.append(event)

    def reconcile(self, event: Event | EventGroup) -> bool:
        return event in self.events

    def close(self) -> None:
        self.closed += 1


class RecordingServer:
    def __init__(self) -> None:
        self.started = 0
        self.closed = 0

    def start(self) -> None:
        self.started += 1

    def close(self) -> None:
        self.closed += 1


class DynamicClient:
    def __init__(self, path: tuple[str, ...] = ()) -> None:
        self.path = path

    def __getattr__(self, segment: str) -> DynamicClient:
        return DynamicClient((*self.path, segment))

    def __call__(self, *args: object, **kwargs: object) -> tuple[object, ...]:
        return (self.path, args, kwargs)


def test_benedict_returns_canonical_xo_with_fluent_value_and_children() -> None:
    state = xoBenedict("app", value="root")

    assert type(state) is XO
    assert state.value == "root"
    state.user = "Tami"
    state.user.preferences.theme = "dark"
    state.user = "Tami 2"

    assert state.user.value == "Tami 2"
    assert state.user.preferences.theme.value == "dark"


def test_benedict_instances_never_share_a_singleton_root() -> None:
    first = xoBenedict("app")
    second = xoBenedict("app")
    first.answer = 42

    assert second.answer.get(MISSING) is MISSING
    assert first.origin_id != second.origin_id


def test_branch_is_canonical_xo_with_one_history_capability() -> None:
    state = xoBranch("conversation")
    state.prompt = "first"
    state.prompt = "edited"

    assert type(state) is XO
    built = state.capability("history")
    assert isinstance(built, History)
    assert built.current() == state.revision
    assert tuple(built.revisions) == (1, 2)
    assert [item["key"] for item in state.capabilities].count("history") == 1


def test_fresh_delegates_validation_to_canonical_precommit_capability() -> None:
    seen: list[object] = []

    def positive(value: object) -> None:
        seen.append(value)
        if not isinstance(value, int) or value < 0:
            raise ValueError("positive integer required")

    state = Fresh("validated", validators={"count": positive})
    state.count = 2

    with pytest.raises(ValueError, match="positive integer required"):
        state.count = -1

    assert state.count.value == 2
    assert state.revision == 1
    assert seen == [2, -1]


def test_fresh_rejects_ambiguous_validation_constructors() -> None:
    def rule(value: object) -> None:
        pass

    with pytest.raises(TypeError, match="only one"):
        Fresh("app", validator=rule, validators={"count": rule})


def test_redis_facades_compose_one_supplied_backend_without_subclassing() -> None:
    durable = RecordingBackend()
    state = FreshRedis("app", backend=durable)
    state.counter = 1

    assert type(state) is XO
    assert state.capability("backend").backend is durable
    assert durable.events[0].path == ("counter",)
    assert durable.events[0].payload == 1
    assert xoRedis is FreshRedis

    state.close()
    state.close()
    assert durable.closed == 1


def test_redis_backend_is_authoritative_before_local_visibility() -> None:
    class RejectingBackend(RecordingBackend):
        def commit(self, event: Event | EventGroup) -> None:
            raise RuntimeError("durability unavailable")

    state = FreshRedis("app", backend=RejectingBackend())

    with pytest.raises(RuntimeError, match="durability unavailable"):
        state.counter = 1

    assert state.counter.get(MISSING) is MISSING
    assert state.revision == 0


def test_freshzero_composes_service_and_delegates_optional_server_lifecycle() -> None:
    server = RecordingServer()
    facade = FreshZero("app", server=server)

    assert isinstance(facade, ServiceFacade)
    assert type(facade.state) is XO

    @facade.public.math.double
    def double(value: int) -> int:
        return value * 2

    facade.status = "ready"
    assert facade.registry.call("math.double", 3) == 6
    assert facade.status.value == "ready"

    facade.start()
    facade.close()
    facade.close()
    assert server.started == 1
    assert server.closed == 1


def test_freshzero_preserves_a_supplied_canonical_registry() -> None:
    from xo.service import ServiceRegistry

    registry = ServiceRegistry()
    facade = FreshZero("app", registry=registry)

    assert facade.registry is registry


def test_freshzero_factory_receives_the_canonical_service_registry() -> None:
    captured: list[tuple[object, object, str]] = []

    class Server(RecordingServer):
        def __init__(self, registry: object) -> None:
            super().__init__()
            self.registry = registry

    def factory(registry: object, address: object, *, namespace: str) -> Server:
        captured.append((registry, address, namespace))
        return Server(registry)

    facade = FreshZero(
        "app",
        server_factory=factory,
        address="unix:///tmp/app.xo.sock",
    )

    assert captured == [(facade.registry, "unix:///tmp/app.xo.sock", "app")]
    assert facade.server.registry is facade.registry


def test_freshclient_returns_supplied_canonical_dynamic_proxy_unchanged() -> None:
    client = DynamicClient()
    remote = FreshClient(client)

    assert remote is client
    assert remote.image.generate("42") == (
        ("image", "generate"),
        ("42",),
        {},
    )


def test_freshclient_requires_explicit_client_or_address() -> None:
    with pytest.raises(TypeError, match="requires client= or an explicit address="):
        FreshClient()


@pytest.mark.parametrize(
    ("factory", "option"),
    [
        (xoBenedict, {"singleton": True}),
        (xoBranch, {"global_root": True}),
        (Fresh, {"eval": True}),
        (FreshRedis, {"pickle": True}),
        (FreshZero, {"killport": True}),
        (FreshClient, {"retry_forever": True}),
    ],
)
def test_unsafe_legacy_mechanics_are_rejected(factory, option) -> None:
    with pytest.raises(CompatibilityError):
        factory(**option)


@pytest.mark.parametrize("option", ["_inc", "inc", "request_port", "publish_port"])
def test_ambiguous_port_offsets_are_rejected(option: str) -> None:
    with pytest.raises(CompatibilityError, match="explicit Unix or loopback address"):
        FreshClient(client=DynamicClient(), **{option: 111})
