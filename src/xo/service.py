from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from .capabilities import BuildContext, CapabilitySpec, NullCapability
from .exceptions import ProtocolError
from .path import Path, PathLike, parse_path, render_path


class ServiceNotFound(ProtocolError):
    code = "xo.not_found"


class ServiceRegistry:
    __slots__ = ("_functions", "public")

    def __init__(self) -> None:
        self._functions: dict[Path, Callable[..., object]] = {}
        self.public = _PublicPath(self, ())

    def expose(
        self,
        path: PathLike | None = None,
    ) -> Callable[[Callable[..., object]], Callable[..., object]]:
        canonical = parse_path(path)

        def decorator(function: Callable[..., object]) -> Callable[..., object]:
            target = canonical or (function.__name__,)
            if target in self._functions:
                raise ValueError(f"service path already exposed: {render_path(target)}")
            self._functions[target] = function
            return function

        return decorator

    def register(self, path: PathLike, function: Callable[..., object]) -> None:
        self.expose(path)(function)

    def call(self, path: PathLike, *args: object, **kwargs: object) -> object:
        canonical = parse_path(path)
        try:
            function = self._functions[canonical]
        except KeyError as error:
            raise ServiceNotFound(render_path(canonical)) from error
        return function(*args, **kwargs)

    def stream(self, path: PathLike, *args: object, **kwargs: object) -> Iterator[object]:
        result = self.call(path, *args, **kwargs)
        if isinstance(result, Iterator):
            yield from result
        else:
            yield result

    def describe(self) -> tuple[str, ...]:
        return tuple(render_path(path) for path in sorted(self._functions))

    @property
    def functions(self) -> Mapping[Path, Callable[..., object]]:
        return MappingProxyType(self._functions)


class _PublicPath:
    __slots__ = ("_path", "_registry")

    def __init__(self, registry: ServiceRegistry, path: Path) -> None:
        self._registry = registry
        self._path = path

    def __getattr__(self, segment: str) -> _PublicPath:
        if segment.startswith("_"):
            raise AttributeError(segment)
        return _PublicPath(self._registry, (*self._path, segment))

    def __call__(self, function: Callable[..., object]) -> Callable[..., object]:
        return self._registry.expose(self._path)(function)


@dataclass(slots=True)
class Service(NullCapability):
    context: BuildContext
    registry: ServiceRegistry = field(default_factory=ServiceRegistry)

    @property
    def service(self) -> ServiceRegistry:
        return self.registry

    @property
    def public(self) -> _PublicPath:
        return self.registry.public


def service(*, key: str = "service") -> CapabilitySpec:
    return CapabilitySpec(
        key=key,
        factory=Service,
        provides=frozenset({"service"}),
        configuration={"transport": "local"},
    )
