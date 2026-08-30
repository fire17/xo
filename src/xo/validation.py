from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

from .capabilities import BuildContext, CapabilitySpec, NullCapability, Validator
from .core import CommitPlan
from .events import Operation
from .path import Path, PathLike, parse_path


class ValueValidator(Protocol):
    def __call__(self, value: object) -> object: ...


@dataclass(slots=True)
class Validation(NullCapability, Validator):
    context: BuildContext
    rules: Mapping[Path, ValueValidator]
    descendants: bool = False

    def validate(self, plan: CommitPlan) -> None:
        if plan.operation is not Operation.SET_VALUE:
            return
        validator = self.rules.get(plan.path)
        if validator is None and self.descendants:
            candidates = (
                (path, rule)
                for path, rule in self.rules.items()
                if len(path) <= len(plan.path) and plan.path[: len(path)] == path
            )
            match = max(candidates, key=lambda item: len(item[0]), default=None)
            validator = None if match is None else match[1]
        if validator is not None:
            validator(plan.payload)


def validation(
    rules: Mapping[PathLike, ValueValidator],
    *,
    descendants: bool = False,
    key: str = "validation",
) -> CapabilitySpec:
    canonical = {parse_path(path): rule for path, rule in rules.items()}
    return CapabilitySpec(
        key=key,
        factory=lambda context: Validation(context, canonical, descendants),
        provides=frozenset({"validation"}),
        before=frozenset({"durability", "history", "projection"}),
        configuration={
            "paths": tuple(".".join(path) for path in canonical),
            "descendants": descendants,
        },
    )


def pydantic_validation(
    models: Mapping[PathLike, type[object]],
    *,
    key: str = "pydantic",
) -> CapabilitySpec:
    """Optional adapter; importing XO never imports Pydantic."""

    def adapter(model: type[object]) -> Callable[[object], object]:
        validate = getattr(model, "model_validate", None)
        if validate is None:
            raise TypeError(f"{model!r} has no Pydantic model_validate method")
        return validate

    return validation(
        {path: adapter(model) for path, model in models.items()},
        key=key,
    )
