from __future__ import annotations

from dataclasses import dataclass, field

from .capabilities import BuildContext, CapabilitySpec, NullCapability, Observer
from .core import MISSING, CommitPlan
from .events import Event, EventGroup, Operation
from .exceptions import AmbiguousRedo, HistoryError, RevisionNotFound


@dataclass(frozen=True, slots=True)
class Revision:
    revision_id: int
    parent_revision: int | None
    event: Event | EventGroup
    image: dict[str, object]


@dataclass(slots=True)
class History(NullCapability, Observer):
    context: BuildContext
    revisions: dict[int, Revision] = field(default_factory=dict)
    children: dict[int | None, list[int]] = field(default_factory=dict)
    cursor: int | None = None
    _initial_image: dict[str, object] = field(init=False)
    _navigating: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        self._initial_image = self.context.root.snapshot()["root"]

    def observe(self, event: Event | EventGroup) -> None:
        if self._navigating:
            return
        revision_id = event.events[0].revision if isinstance(event, EventGroup) else event.revision
        image = self.context.root.snapshot()["root"]
        record = Revision(revision_id, self.cursor, event, image)
        self.revisions[revision_id] = record
        self.children.setdefault(self.cursor, []).append(revision_id)
        self.cursor = revision_id

    observe_remote = observe

    def current(self) -> int | None:
        return self.cursor

    def parents(self, revision: int | None = None) -> tuple[int, ...]:
        current = self.cursor if revision is None else revision
        result: list[int] = []
        while current is not None:
            result.append(current)
            current = self.checkout_record(current).parent_revision
        return tuple(result)

    def branches(self, revision: int | None = None) -> tuple[int, ...]:
        return tuple(self.children.get(self.cursor if revision is None else revision, ()))

    def undo_target(self) -> int | None:
        if self.cursor is None:
            raise HistoryError("history is empty")
        return self.revisions[self.cursor].parent_revision

    def redo_target(self, revision: int | None = None) -> int:
        candidates = self.branches(revision)
        if not candidates:
            raise HistoryError("no future revision")
        if len(candidates) > 1:
            raise AmbiguousRedo(candidates)
        return candidates[0]

    def checkout_record(self, revision: int) -> Revision:
        try:
            return self.revisions[revision]
        except KeyError as error:
            raise RevisionNotFound(revision) from error

    def undo(self) -> Event | EventGroup:
        return self.checkout(self.undo_target())

    def redo(self, revision: int | None = None) -> Event | EventGroup:
        return self.checkout(self.redo_target(revision))

    def checkout(self, revision: int | None) -> Event | EventGroup:
        if revision is not None:
            target = self.checkout_record(revision).image
        else:
            target = self._initial_image
        if revision == self.cursor:
            raise HistoryError("already at requested revision")
        root = self.context.root
        current = root.snapshot()["root"]
        plans = tuple(self._diff((), current, target))
        if not plans:
            raise HistoryError("requested revision has identical content")
        self._navigating = True
        try:
            item = root.commit_many(plans, expected_revision=root.revision)
        finally:
            self._navigating = False
        self.cursor = revision
        return item

    def _diff(
        self,
        path: tuple[str, ...],
        current: object,
        target: object,
    ) -> list[CommitPlan]:
        if not isinstance(current, dict) or not isinstance(target, dict):
            raise HistoryError("corrupt history image")
        current_children = current.get("$children")
        target_children = target.get("$children")
        if not isinstance(current_children, list) or not isinstance(target_children, list):
            raise HistoryError("corrupt history image")
        current_keys = [item[0] for item in current_children]
        target_keys = [item[0] for item in target_children]
        common_current = [key for key in current_keys if key in target_keys]
        common_target = [key for key in target_keys if key in current_keys]
        if common_current != common_target:
            return [CommitPlan(Operation.RESTORE_SUBTREE, path, target, self.context.root.revision)]
        plans: list[CommitPlan] = []
        current_has = "$value" in current
        target_has = "$value" in target
        if current_has != target_has:
            plans.append(
                CommitPlan(
                    Operation.SET_VALUE if target_has else Operation.CLEAR_VALUE,
                    path,
                    target.get("$value", MISSING),
                    self.context.root.revision,
                )
            )
        elif current_has and (
            self.context.root._root.codec.dumps(current["$value"])
            != self.context.root._root.codec.dumps(target["$value"])
        ):
            plans.append(
                CommitPlan(Operation.SET_VALUE, path, target["$value"], self.context.root.revision)
            )
        current_by_key = {item[0]: item[1] for item in current_children}
        target_by_key = {item[0]: item[1] for item in target_children}
        for key in reversed(current_keys):
            if key not in target_by_key:
                plans.append(
                    CommitPlan(
                        Operation.DELETE_SUBTREE,
                        (*path, key),
                        MISSING,
                        self.context.root.revision,
                    )
                )
        for key in target_keys:
            child_path = (*path, key)
            if key not in current_by_key:
                plans.append(
                    CommitPlan(
                        Operation.RESTORE_SUBTREE,
                        child_path,
                        target_by_key[key],
                        self.context.root.revision,
                    )
                )
            else:
                plans.extend(self._diff(child_path, current_by_key[key], target_by_key[key]))
        return plans


def history(*, key: str = "history") -> CapabilitySpec:
    return CapabilitySpec(
        key=key,
        factory=History,
        provides=frozenset({"history"}),
        before=frozenset({"projection"}),
        configuration={"kind": "revision-dag"},
    )
