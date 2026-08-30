from __future__ import annotations

from dataclasses import dataclass

from .capabilities import CapabilitySpec
from .history import history
from .service import service


@dataclass(frozen=True, slots=True)
class Profile:
    name: str
    capabilities: tuple[CapabilitySpec, ...]

    def apply(self, namespace: str):
        from .core import XO

        return XO.compose(namespace, *self.capabilities)

    @classmethod
    def bare(cls) -> Profile:
        return cls("bare", ())

    @classmethod
    def hybrid(
        cls,
        *,
        durability: CapabilitySpec | None = None,
        projections: tuple[CapabilitySpec, ...] = (),
        validation: CapabilitySpec | None = None,
        include_history: bool = True,
        include_service: bool = True,
    ) -> Profile:
        specs: list[CapabilitySpec] = []
        if validation is not None:
            specs.append(validation)
        if include_history:
            specs.append(history())
        if durability is not None:
            specs.append(durability)
        if include_service:
            specs.append(service())
        specs.extend(projections)
        return cls("hybrid", tuple(specs))
