from __future__ import annotations

import pytest

from xo import XO
from xo.service import Service, ServiceNotFound, service


def test_service_registry_composes_with_state_and_decorator_paths() -> None:
    state = XO.compose("app", service())
    built = state.capability("service")
    assert isinstance(built, Service)

    @built.public.image.thumbnail
    def thumbnail(image_id: str) -> str:
        return f"thumb:{image_id}:{state.revision}"

    state.ready = True
    assert built.registry.call("image.thumbnail", "42") == "thumb:42:1"
    assert built.registry.describe() == ("image.thumbnail",)

    with pytest.raises(ServiceNotFound):
        built.registry.call("private.function")


def test_service_stream_keeps_local_generator_semantics() -> None:
    state = XO.compose("app", service())
    built = state.capability("service")

    @built.registry.expose("numbers")
    def numbers():
        yield from range(3)

    assert list(built.registry.stream("numbers")) == [0, 1, 2]
