from __future__ import annotations

import contextvars
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .events import DerivedEvent
from .exceptions import (
    CrossTreeDependencyError,
    FormulaCycleError,
    FormulaError,
    FormulaEvaluationError,
    FormulaMutationError,
    FormulaStaleError,
)
from .path import Path

if TYPE_CHECKING:
    from .core import XO, _RootState


@dataclass(slots=True)
class _Capture:
    root: _RootState
    dependencies: dict[Path, int] = field(default_factory=dict)


@dataclass(slots=True)
class Formula:
    function: Callable[[], object]
    dependencies: dict[Path, int] = field(default_factory=dict)
    cached: object = None
    has_cached: bool = False
    dirty: bool = True
    error: FormulaEvaluationError | FormulaCycleError | None = None
    computing_thread: int | None = None
    observers: int = 0
    generation: int = 1


_CAPTURE: contextvars.ContextVar[_Capture | None] = contextvars.ContextVar(
    "xo_formula_capture", default=None
)
_STACK: contextvars.ContextVar[tuple[tuple[_RootState, Path], ...]] = contextvars.ContextVar(
    "xo_formula_stack", default=()
)


def assert_formula_may_not_write(root: _RootState) -> None:
    capture = _CAPTURE.get()
    if capture is not None and capture.root is root:
        path = _STACK.get()[-1][1]
        raise FormulaMutationError(
            f"formula {'.'.join(path) or '<root>'} attempted to mutate XO"
        )


class FormulaGraph:
    __slots__ = ("_condition", "_dependents", "_formulas", "_max_retries", "_root")

    def __init__(self, root: _RootState, *, max_retries: int = 3) -> None:
        self._root = root
        self._formulas: dict[Path, Formula] = {}
        self._dependents: dict[Path, set[Path]] = {}
        self._condition = threading.Condition(root.lock)
        self._max_retries = max_retries

    @staticmethod
    def record_read(root: _RootState, path: Path, revision: int) -> None:
        capture = _CAPTURE.get()
        if capture is None:
            return
        if capture.root is not root:
            raise CrossTreeDependencyError("formula dependencies must belong to one XO root")
        capture.dependencies[path] = revision

    def register(self, path: Path, function: Callable[[], object]) -> None:
        if not callable(function):
            raise TypeError("a formula must be callable")
        with self._root.lock:
            previous = self._formulas.get(path)
            generation = 1 if previous is None else previous.generation + 1
            if previous is not None:
                self._remove_edges(path, previous.dependencies)
            self._formulas[path] = Formula(function=function, generation=generation)

    def remove(self, path: Path) -> bool:
        with self._root.lock:
            formula = self._formulas.pop(path, None)
            if formula is None:
                return False
            self._remove_edges(path, formula.dependencies)
            return True

    def contains(self, path: Path) -> bool:
        with self._root.lock:
            return path in self._formulas

    def dependencies(self, path: Path) -> tuple[Path, ...]:
        with self._root.lock:
            formula = self._formulas.get(path)
            return () if formula is None else tuple(formula.dependencies)

    def observe(self, path: Path) -> Callable[[], None]:
        with self._root.lock:
            self._formulas[path].observers += 1
        active = True

        def stop() -> None:
            nonlocal active
            if not active:
                return
            active = False
            with self._root.lock:
                formula = self._formulas.get(path)
                if formula is not None and formula.observers:
                    formula.observers -= 1

        return stop

    def invalidate(self, changed: Path) -> tuple[Path, ...]:
        with self._root.lock:
            queue = list(self._dependents.get(changed, ()))
            affected: list[Path] = []
            seen: set[Path] = set()
            while queue:
                path = queue.pop()
                if path in seen:
                    continue
                seen.add(path)
                formula = self._formulas.get(path)
                if formula is None:
                    continue
                formula.dirty = True
                formula.error = None
                affected.append(path)
                queue.extend(self._dependents.get(path, ()))
            return tuple(affected)
    def invalidate_all(self) -> tuple[Path, ...]:
        with self._root.lock:
            affected = tuple(self._formulas)
            for formula in self._formulas.values():
                formula.dirty = True
                formula.error = None
            return affected


    def eager_targets(self, affected: tuple[Path, ...]) -> tuple[Path, ...]:
        with self._root.lock:
            return tuple(
                path
                for path in affected
                if (formula := self._formulas.get(path)) is not None
                and formula.observers > 0
            )

    def materialize(self, path: Path) -> DerivedEvent:
        try:
            payload = self.read(self._root.root.at(path))
            status = "value"
        except FormulaError as error:
            cause = error.cause if isinstance(error, FormulaEvaluationError) else error
            payload = {
                "code": "xo.formula.error",
                "message": f"{type(cause).__name__}: {cause}",
            }
            status = "error"
        with self._root.lock:
            formula = self._formulas[path]
            generation = formula.generation
            cause_revision = self._root.revision
        return DerivedEvent(
            namespace=self._root.namespace,
            origin_id=self._root.origin_id,
            cause_revision=cause_revision,
            path=path,
            formula_generation=generation,
            status=status,
            payload=payload,
        )


    def read(self, node: XO) -> object:
        path = node.path
        for _ in range(self._max_retries):
            with self._condition:
                formula = self._formulas[path]
                while formula.computing_thread is not None:
                    if formula.computing_thread == threading.get_ident():
                        self._raise_cycle(path)
                    self._condition.wait()
                    formula = self._formulas[path]
                if not formula.dirty and formula.has_cached:
                    record = self._root.resolve(path)
                    revision = record.value_revision if record is not None else 0
                    self.record_read(self._root, path, revision)
                    if formula.error is not None:
                        raise formula.error
                    return formula.cached
                if any(root is self._root and current == path for root, current in _STACK.get()):
                    self._raise_cycle(path)
                formula.computing_thread = threading.get_ident()
                function = formula.function

            capture = _Capture(self._root)
            capture_token = _CAPTURE.set(capture)
            stack_token = _STACK.set((*_STACK.get(), (self._root, path)))
            error: FormulaEvaluationError | FormulaCycleError | None = None
            result: object = None
            try:
                result = function()
            except (FormulaCycleError, FormulaMutationError, CrossTreeDependencyError):
                with self._condition:
                    formula = self._formulas[path]
                    formula.computing_thread = None
                    formula.dirty = True
                    self._condition.notify_all()
                raise
            except BaseException as cause:
                error = FormulaEvaluationError(path, cause)
            finally:
                _STACK.reset(stack_token)
                _CAPTURE.reset(capture_token)

            with self._condition:
                formula = self._formulas[path]
                stale = any(
                    (
                        (record := self._root.resolve(dependency)) is not None
                        and record.value_revision != revision
                    )
                    or (record is None and revision != 0)
                    for dependency, revision in capture.dependencies.items()
                )
                if stale:
                    formula.computing_thread = None
                    formula.dirty = True
                    self._condition.notify_all()
                    continue
                self._remove_edges(path, formula.dependencies)
                if error is not None:
                    # Keep prior edges as well so any previously known source can recover it.
                    capture.dependencies.update(formula.dependencies)
                formula.dependencies = capture.dependencies
                for dependency in capture.dependencies:
                    self._dependents.setdefault(dependency, set()).add(path)
                formula.cached = result
                formula.has_cached = error is None
                formula.error = error
                formula.dirty = False
                formula.computing_thread = None
                record = self._root.resolve(path, create=True)
                record.value_revision = self._root.revision
                self._condition.notify_all()
                self.record_read(self._root, path, record.value_revision)
            if error is not None:
                raise error
            return result
        raise FormulaStaleError(
            f"formula {'.'.join(path) or '<root>'} changed during "
            f"{self._max_retries} evaluations"
        )

    def _raise_cycle(self, path: Path) -> None:
        stack = tuple(current for root, current in _STACK.get() if root is self._root)
        start = stack.index(path) if path in stack else 0
        raise FormulaCycleError((*stack[start:], path))

    def _remove_edges(self, formula_path: Path, dependencies: dict[Path, int]) -> None:
        for dependency in dependencies:
            targets = self._dependents.get(dependency)
            if targets is None:
                continue
            targets.discard(formula_path)
            if not targets:
                self._dependents.pop(dependency, None)
