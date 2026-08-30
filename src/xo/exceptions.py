from __future__ import annotations


class XOError(Exception):
    """Base class for XO failures."""


class InvalidPath(XOError, ValueError):
    pass


PathError = InvalidPath


class MissingPath(XOError, KeyError):
    pass


class StaleNode(XOError, RuntimeError):
    pass


class ClosedError(XOError, RuntimeError):
    pass


class ConflictError(XOError):
    pass


class CodecError(XOError, ValueError):
    pass


class PersistenceError(XOError):
    """A backend definitely did not commit; local state is unchanged."""


class CommitOutcomeUnknown(PersistenceError):
    """The backend may have committed; authoritative reconciliation is required."""


class RecoveryRequired(XOError):
    pass


class InvariantViolation(XOError):
    pass


class ReentrantMutationError(XOError):
    pass


class SubscriberError(XOError):
    def __init__(self, callback: object, cause: BaseException) -> None:
        self.callback = callback
        self.cause = cause
        super().__init__(f"subscriber {callback!r} failed: {cause}")


class FormulaError(XOError):
    pass


class DerivedWriteError(FormulaError):
    pass


class FormulaCycleError(FormulaError):
    def __init__(self, paths: tuple[tuple[str, ...], ...]) -> None:
        self.paths = paths
        rendered = " -> ".join(".".join(path) or "<root>" for path in paths)
        super().__init__(f"formula dependency cycle: {rendered}")


class FormulaEvaluationError(FormulaError):
    def __init__(self, path: tuple[str, ...], cause: BaseException) -> None:
        self.path = path
        self.cause = cause
        super().__init__(f"formula {'.'.join(path) or '<root>'} failed: {cause}")


class FormulaMutationError(FormulaError):
    pass


FormulaWriteError = FormulaMutationError


class FormulaStaleError(FormulaError):
    pass


class CrossTreeDependencyError(FormulaError):
    pass


class HistoryError(XOError):
    pass


class RevisionNotFound(HistoryError):
    pass


class AmbiguousRedo(HistoryError):
    def __init__(self, revisions: tuple[int, ...]) -> None:
        self.revisions = revisions
        super().__init__(f"redo is ambiguous; choose one of {revisions!r}")


AmbiguousRedoError = AmbiguousRedo


class CapabilityError(XOError):
    pass


class CapabilityConflictError(CapabilityError):
    pass


class CapabilityOrderError(CapabilityError):
    pass


class CapabilityLifecycleError(CapabilityError):
    pass


class CapabilityHandoffError(CapabilityError):
    pass


class ProtocolError(XOError):
    code = "xo.protocol"


class AuthenticationError(ProtocolError):
    code = "xo.auth"


class BackpressureError(ProtocolError):
    code = "xo.backpressure"


class CancelledError(ProtocolError):
    code = "xo.cancelled"


class DeadlineExceeded(ProtocolError):
    code = "xo.deadline"
