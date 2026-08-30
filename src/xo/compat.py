from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import TypeVar, cast

from .backends.base import Backend
from .backends.capability import backend as backend_capability
from .capabilities import CapabilitySpec
from .codec import Codec
from .core import MISSING, XO
from .exceptions import XOError
from .history import history
from .path import PathLike
from .service import Service, ServiceRegistry, service
from .validation import ValueValidator
from .validation import validation as validation_capability


class CompatibilityError(XOError, ValueError):
    """A legacy behavior has no safe, unambiguous XO equivalent."""


_UNSAFE_OPTIONS = {
    "allow_eval": "remote eval is not supported; expose a callable through ServiceRegistry",
    "dill": "dill payloads are not supported; XO uses its tagged JSON Codec",
    "eval": "remote eval is not supported; expose a callable through ServiceRegistry",
    "force_port": "port takeover is not supported; choose an available explicit address",
    "global_root": "global singleton roots are not supported; pass one XO explicitly",
    "kill_port": "port killing is not supported; choose an available explicit address",
    "kill_ports": "port killing is not supported; choose an available explicit address",
    "killport": "port killing is not supported; choose an available explicit address",
    "pickle": "pickle payloads are not supported; XO uses its tagged JSON Codec",
    "retry": "implicit retries are not supported; use the canonical adapter's explicit policy",
    "retries": "implicit retries are not supported; use the canonical adapter's explicit policy",
    "retry_count": (
        "implicit retries are not supported; use the canonical adapter's explicit policy"
    ),
    "retry_forever": (
        "implicit retries are not supported; use the canonical adapter's explicit policy"
    ),
    "shared_root": "global singleton roots are not supported; pass one XO explicitly",
    "singleton": "global singleton roots are not supported; pass one XO explicitly",
    "takeover_port": "port takeover is not supported; choose an available explicit address",
    "_root": "legacy root injection is not supported; pass one XO explicitly",
}
_PORT_OFFSET_OPTIONS = {"_inc", "inc", "publish_port", "request_port"}


def _reject_legacy_options(options: Mapping[str, object]) -> None:
    offsets = sorted(_PORT_OFFSET_OPTIONS.intersection(options))
    if offsets:
        names = ", ".join(offsets)
        raise CompatibilityError(
            f"ambiguous legacy port offset option(s) {names}; pass one explicit Unix or "
            "loopback address"
        )
    rejected = sorted(_UNSAFE_OPTIONS.keys() & options.keys())
    if rejected:
        name = rejected[0]
        raise CompatibilityError(f"legacy option {name!r} rejected: {_UNSAFE_OPTIONS[name]}")


def _new_xo(
    namespace: str,
    *,
    value: object,
    codec: Codec | None,
    capabilities: Iterable[CapabilitySpec],
) -> XO:
    if not isinstance(namespace, str) or not namespace:
        raise TypeError("namespace must be a non-empty string")
    return XO(
        namespace,
        value=value,
        codec=codec,
        capabilities=tuple(capabilities),
    )


def xoBenedict(
    namespace: str = "default",
    *,
    value: object = MISSING,
    codec: Codec | None = None,
    capabilities: Iterable[CapabilitySpec] = (),
    **legacy_options: object,
) -> XO:
    """Construct the canonical bare XO under the observed legacy name."""

    _reject_legacy_options(legacy_options)
    if legacy_options:
        names = ", ".join(sorted(legacy_options))
        raise TypeError(f"unsupported xoBenedict option(s): {names}")
    return _new_xo(
        namespace,
        value=value,
        codec=codec,
        capabilities=capabilities,
    )


def xoBranch(
    namespace: str = "default",
    *,
    value: object = MISSING,
    codec: Codec | None = None,
    capabilities: Iterable[CapabilitySpec] = (),
    **legacy_options: object,
) -> XO:
    """Construct one canonical XO root with the revision-DAG observer attached."""

    _reject_legacy_options(legacy_options)
    if legacy_options:
        names = ", ".join(sorted(legacy_options))
        raise TypeError(f"unsupported xoBranch option(s): {names}")
    return _new_xo(
        namespace,
        value=value,
        codec=codec,
        capabilities=(*tuple(capabilities), history()),
    )


def Fresh(
    namespace: str = "default",
    *,
    value: object = MISSING,
    codec: Codec | None = None,
    validators: Mapping[PathLike, ValueValidator] | None = None,
    validator: ValueValidator | None = None,
    validation: CapabilitySpec | None = None,
    descendants: bool = False,
    capabilities: Iterable[CapabilitySpec] = (),
    **legacy_options: object,
) -> XO:
    """Construct XO with an optional explicit pre-commit validation capability."""

    _reject_legacy_options(legacy_options)
    if legacy_options:
        names = ", ".join(sorted(legacy_options))
        raise TypeError(f"unsupported Fresh option(s): {names}")
    selected = sum(item is not None for item in (validators, validator, validation))
    if selected > 1:
        raise TypeError("pass only one of validators, validator, or validation")
    validation_spec = validation
    if validators is not None:
        validation_spec = validation_capability(validators, descendants=descendants)
    elif validator is not None:
        validation_spec = validation_capability({(): validator}, descendants=True)
    specs = tuple(capabilities)
    if validation_spec is not None:
        specs = (*specs, validation_spec)
    return _new_xo(namespace, value=value, codec=codec, capabilities=specs)


def FreshRedis(
    namespace: str = "default",
    *,
    backend: Backend | None = None,
    url: str | None = None,
    strict: bool = True,
    start: bool = False,
    value: object = MISSING,
    codec: Codec | None = None,
    capabilities: Iterable[CapabilitySpec] = (),
    **backend_options: object,
) -> XO:
    """Construct XO with exactly one canonical durability backend."""

    _reject_legacy_options(backend_options)
    if backend is not None and (url is not None or backend_options):
        raise TypeError("backend cannot be combined with url or backend constructor options")
    selected = backend
    if selected is None:
        from .backends.redis import RedisBackend

        selected = RedisBackend(
            url=url or "redis://127.0.0.1:6379/0",
            namespace=namespace,
            strict=strict,
            **backend_options,
        )
    _require_backend(selected)
    state = _new_xo(
        namespace,
        value=value,
        codec=codec,
        capabilities=(*tuple(capabilities), backend_capability(selected)),
    )
    if start:
        state.start()
    return state


xoRedis = FreshRedis


def _require_backend(candidate: object) -> None:
    if not isinstance(getattr(candidate, "strict", None), bool):
        raise TypeError("backend.strict must be bool")
    for name in ("commit", "reconcile", "close"):
        if not callable(getattr(candidate, name, None)):
            raise TypeError(f"backend must provide callable {name}()")


def _service_capability(registry: ServiceRegistry | None) -> CapabilitySpec:
    if registry is None:
        return service()
    if not isinstance(registry, ServiceRegistry):
        raise TypeError("registry must be a ServiceRegistry")
    return CapabilitySpec(
        key="service",
        factory=lambda context: Service(context, registry),
        provides=frozenset({"service"}),
        configuration={"transport": "local"},
    )


class ServiceFacade:
    """Thin legacy surface over one XO, its Service registry, and an optional RPC server."""

    __slots__ = ("_closed", "_server", "_service", "_state")

    def __init__(self, state: XO, built_service: Service, server: object | None) -> None:
        object.__setattr__(self, "_state", state)
        object.__setattr__(self, "_service", built_service)
        object.__setattr__(self, "_server", server)
        object.__setattr__(self, "_closed", False)

    @property
    def state(self) -> XO:
        return self._state

    @property
    def service(self) -> Service:
        return self._service

    @property
    def registry(self) -> ServiceRegistry:
        return self._service.registry

    @property
    def public(self) -> object:
        return self._service.public

    @property
    def server(self) -> object | None:
        return self._server

    def start(self) -> ServiceFacade:
        self._state.start()
        if self._server is not None:
            start = getattr(self._server, "start", None)
            if not callable(start):
                raise TypeError("server must provide callable start()")
            start()
        return self

    def close(self) -> None:
        if self._closed:
            return
        object.__setattr__(self, "_closed", True)
        try:
            if self._server is not None:
                close = getattr(self._server, "close", None)
                if not callable(close):
                    raise TypeError("server must provide callable close()")
                close()
        finally:
            self._state.close()

    def __enter__(self) -> ServiceFacade:
        return self.start()

    def __exit__(self, *_error: object) -> None:
        self.close()

    def __getattr__(self, name: str) -> object:
        return getattr(self._state, name)

    def __setattr__(self, name: str, value: object) -> None:
        if name in self.__slots__:
            object.__setattr__(self, name, value)
        elif name in {"state", "service", "registry", "public", "server"}:
            raise AttributeError(f"{name} is read-only")
        else:
            setattr(self._state, name, value)

    def __getitem__(self, key: str) -> XO:
        return self._state[key]

    def __setitem__(self, key: str, value: object) -> None:
        self._state[key] = value

    def __delitem__(self, key: str) -> None:
        del self._state[key]

    def __iter__(self):
        return iter(self._state)

    def __len__(self) -> int:
        return len(self._state)

    def __repr__(self) -> str:
        return f"ServiceFacade(state={self._state!r}, server={self._server!r})"


_ServerT = TypeVar("_ServerT")


def FreshZero(
    namespace: str = "default",
    *,
    registry: ServiceRegistry | None = None,
    server: object | None = None,
    server_factory: Callable[..., _ServerT] | None = None,
    address: object | None = None,
    start: bool = False,
    value: object = MISSING,
    codec: Codec | None = None,
    capabilities: Iterable[CapabilitySpec] = (),
    **server_options: object,
) -> ServiceFacade:
    """Compose XO + Service and optionally delegate lifecycle to canonical RPC Server."""

    _reject_legacy_options(server_options)
    if server is not None and (server_factory is not None or address is not None or server_options):
        raise TypeError("server cannot be combined with server_factory, address, or server options")
    if server_factory is not None and address is None:
        raise TypeError("server_factory requires an explicit address")
    if server is None and address is None and server_options:
        raise TypeError("server options require an explicit address")

    state = _new_xo(
        namespace,
        value=value,
        codec=codec,
        capabilities=(*tuple(capabilities), _service_capability(registry)),
    )
    built_service = cast(Service, state.capability("service"))
    if registry is not None and built_service.registry is not registry:
        state.close()
        raise CompatibilityError("service capability did not retain the supplied registry")
    selected_server = server
    try:
        if selected_server is None and address is not None:
            factory: Callable[..., object]
            if server_factory is None:
                from .rpc import Server

                factory = Server
            else:
                factory = server_factory
            selected_server = factory(
                built_service.registry,
                address,
                namespace=namespace,
                **server_options,
            )
        if selected_server is not None:
            server_registry = getattr(selected_server, "registry", built_service.registry)
            if server_registry is not built_service.registry:
                raise CompatibilityError(
                    "server uses a different ServiceRegistry; construct it with facade.registry"
                )
        facade = ServiceFacade(state, built_service, selected_server)
        if start:
            facade.start()
        return facade
    except BaseException:
        state.close()
        raise


def FreshClient(
    client: object | None = None,
    *,
    address: object | None = None,
    namespace: str = "default",
    timeout: float = 5.0,
    **client_options: object,
) -> object:
    """Return a supplied canonical RPC client or construct one for an explicit address."""

    _reject_legacy_options(client_options)
    if client is not None and (address is not None or client_options):
        raise TypeError("client cannot be combined with address or client constructor options")
    if client is not None:
        return client
    if address is None:
        raise TypeError("FreshClient requires client= or an explicit address=")
    from .rpc import Client

    return Client(
        address,
        namespace=namespace,
        timeout=timeout,
        **client_options,
    )


__all__ = [
    "CompatibilityError",
    "Fresh",
    "FreshClient",
    "FreshRedis",
    "FreshZero",
    "ServiceFacade",
    "xoBenedict",
    "xoBranch",
    "xoRedis",
]
