from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .codec import DEFAULT_CODEC, Codec
from .events import DerivedEvent, Diagnostics, Event, EventGroup, Operation
from .exceptions import ProtocolError

PROTOCOL_VERSION = 1
SCHEMA_VERSION = 1


class WireError(ProtocolError):
    code = "xo.protocol.malformed"


class NamespaceMismatch(WireError):
    code = "xo.protocol.namespace_mismatch"


@dataclass(frozen=True, slots=True)
class Envelope:
    kind: str
    message_id: int
    namespace: str
    payload: object
    reply_to: int | None = None
    deadline: float | None = None
    trace_id: str | None = None

    def as_mapping(self) -> dict[str, object]:
        value: dict[str, object] = {
            "k": self.kind,
            "mid": self.message_id,
            "ns": self.namespace,
            "p": self.payload,
        }
        if self.reply_to is not None:
            value["rid"] = self.reply_to
        if self.deadline is not None:
            value["dl"] = self.deadline
        if self.trace_id is not None:
            value["tr"] = self.trace_id
        return value


def event_payload(event: Event) -> dict[str, object]:
    payload: object
    if event.operation is Operation.SET_VALUE:
        payload = {"new": event.payload}
    elif event.operation is Operation.RESTORE_SUBTREE:
        payload = {"node": event.payload}
    else:
        payload = {}
    result: dict[str, object] = {
        "event_id": _encode_id(event.event_id),
        "namespace": event.namespace,
        "origin_id": _encode_id(event.origin_id),
        "base_revision": event.base_revision,
        "revision": event.revision,
        "operation": event.operation.value,
        "path": list(event.path),
        "payload": payload,
    }
    if event.diagnostics is not None:
        result["diagnostics"] = _diagnostics_payload(event.diagnostics)
    return result


def event_from_payload(value: object) -> Event:
    payload = _mapping(value, "event")
    required = {
        "event_id",
        "namespace",
        "origin_id",
        "base_revision",
        "revision",
        "operation",
        "path",
        "payload",
    }
    missing = required - payload.keys()
    if missing:
        raise WireError(f"event missing fields: {', '.join(sorted(missing))}")
    try:
        operation = Operation(_string(payload["operation"], "operation"))
        operation_payload = _operation_payload(operation, payload["payload"])
        return Event(
            event_id=_decode_id(payload["event_id"], "event_id"),
            namespace=_string(payload["namespace"], "namespace"),
            origin_id=_decode_id(payload["origin_id"], "origin_id"),
            base_revision=_integer(payload["base_revision"], "base_revision"),
            revision=_integer(payload["revision"], "revision"),
            operation=operation,
            path=_path(payload["path"]),
            payload=operation_payload,
            diagnostics=_diagnostics_from_payload(payload.get("diagnostics")),
        )
    except (ValueError, TypeError) as error:
        if isinstance(error, WireError):
            raise
        raise WireError(f"invalid event: {error}") from error


def derived_payload(event: DerivedEvent) -> dict[str, object]:
    result: dict[str, object] = {
        "path": list(event.path),
        "generation": event.formula_generation,
        "cause_revision": event.cause_revision,
        "origin_id": _encode_id(event.origin_id),
    }
    if event.status == "value":
        result["value"] = event.payload
    else:
        result["status"] = event.status
        result["error"] = event.payload
    if event.diagnostics is not None:
        result["diagnostics"] = _diagnostics_payload(event.diagnostics)
    return result


def derived_from_payload(namespace: str, value: object) -> DerivedEvent:
    payload = _mapping(value, "derived event")
    required = {"path", "generation", "cause_revision", "origin_id"}
    missing = required - payload.keys()
    if missing:
        raise WireError(f"derived event missing fields: {', '.join(sorted(missing))}")
    has_value = "value" in payload
    has_error = "error" in payload
    if has_value == has_error:
        raise WireError("derived event must carry exactly one of value or error")
    status = "value" if has_value else _string(payload.get("status", "error"), "status")
    return DerivedEvent(
        namespace=namespace,
        origin_id=_decode_id(payload["origin_id"], "origin_id"),
        cause_revision=_integer(payload["cause_revision"], "cause_revision"),
        path=_path(payload["path"]),
        formula_generation=_integer(payload["generation"], "generation"),
        status=status,
        payload=payload["value"] if has_value else payload["error"],
        diagnostics=_diagnostics_from_payload(payload.get("diagnostics")),
    )


def commit_envelope(
    item: Event | EventGroup,
    *,
    message_id: int,
) -> Envelope:
    if isinstance(item, EventGroup):
        first = item.events[0]
        payload: object = {"events": [event_payload(event) for event in item.events]}
        kind = "tx"
    else:
        first = item
        payload = event_payload(item)
        kind = "event"
    return Envelope(kind, message_id, first.namespace, payload)


def derived_envelope(event: DerivedEvent, *, message_id: int) -> Envelope:
    return Envelope("derived", message_id, event.namespace, derived_payload(event))


def item_from_envelope(
    envelope: Envelope,
    *,
    namespace: str | None = None,
) -> Event | EventGroup | DerivedEvent:
    if namespace is not None and envelope.namespace != namespace:
        raise NamespaceMismatch(
            f"envelope namespace {envelope.namespace!r} does not match {namespace!r}"
        )
    if envelope.kind == "event":
        event = event_from_payload(envelope.payload)
        _require_namespace(envelope.namespace, event.namespace)
        return event
    if envelope.kind == "tx":
        payload = _mapping(envelope.payload, "transaction")
        raw_events = payload.get("events")
        if not isinstance(raw_events, list) or not raw_events:
            raise WireError("transaction events must be a non-empty list")
        events = tuple(event_from_payload(value) for value in raw_events)
        for event in events:
            _require_namespace(envelope.namespace, event.namespace)
        return EventGroup(events)
    if envelope.kind == "derived":
        return derived_from_payload(envelope.namespace, envelope.payload)
    raise WireError(f"unsupported state envelope kind: {envelope.kind!r}")


def encode_envelope(envelope: Envelope, *, codec: Codec = DEFAULT_CODEC) -> bytes:
    return codec.dumps(envelope.as_mapping())


def decode_envelope(
    data: bytes | bytearray | memoryview | str,
    *,
    codec: Codec = DEFAULT_CODEC,
) -> Envelope:
    value = _mapping(codec.loads(data), "envelope")
    required = {"k", "mid", "ns", "p"}
    missing = required - value.keys()
    if missing:
        raise WireError(f"envelope missing fields: {', '.join(sorted(missing))}")
    return Envelope(
        kind=_string(value["k"], "k"),
        message_id=_integer(value["mid"], "mid"),
        namespace=_string(value["ns"], "ns"),
        payload=value["p"],
        reply_to=None if "rid" not in value else _integer(value["rid"], "rid"),
        deadline=None if "dl" not in value else _number(value["dl"], "dl"),
        trace_id=None if "tr" not in value else _string(value["tr"], "tr"),
    )


def _operation_payload(operation: Operation, value: object) -> object:
    payload = _mapping(value, "operation payload")
    if operation is Operation.SET_VALUE:
        if set(payload) != {"new"}:
            raise WireError("set_value payload must contain only new")
        return payload["new"]
    if operation is Operation.RESTORE_SUBTREE:
        if set(payload) != {"node"}:
            raise WireError("restore_subtree payload must contain only node")
        return payload["node"]
    if payload:
        raise WireError(f"{operation.value} payload must be empty")
    return None


def _diagnostics_payload(value: Diagnostics) -> dict[str, object]:
    result: dict[str, object] = {"metadata": dict(value.metadata)}
    if value.timestamp_ns is not None:
        result["timestamp_ns"] = value.timestamp_ns
    if value.trace_id is not None:
        result["trace_id"] = value.trace_id
    return result


def _diagnostics_from_payload(value: object) -> Diagnostics | None:
    if value is None:
        return None
    payload = _mapping(value, "diagnostics")
    raw_metadata = payload.get("metadata", {})
    metadata = _mapping(raw_metadata, "diagnostics metadata")
    return Diagnostics(
        timestamp_ns=(
            None
            if "timestamp_ns" not in payload
            else _integer(payload["timestamp_ns"], "timestamp_ns")
        ),
        trace_id=(
            None if "trace_id" not in payload else _string(payload["trace_id"], "trace_id")
        ),
        metadata=tuple(sorted(metadata.items())),
    )


def _require_namespace(expected: str, actual: str) -> None:
    if actual != expected:
        raise NamespaceMismatch(
            f"payload namespace {actual!r} does not match envelope {expected!r}"
        )


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise WireError(f"{name} must be an object with string keys")
    return value


def _path(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(segment, str) for segment in value):
        raise WireError("path must be a list of strings")
    return tuple(value)


def _encode_id(value: int) -> str:
    if value < 0:
        raise WireError("identity cannot be negative")
    return format(value, "x")


def _decode_id(value: object, name: str) -> int:
    if not isinstance(value, str) or not value or any(
        character not in "0123456789abcdefABCDEF" for character in value
    ):
        raise WireError(f"{name} must be a hexadecimal string")
    return int(value, 16)


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise WireError(f"{name} must be a non-negative integer")
    return value


def _number(value: object, name: str) -> float:
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise WireError(f"{name} must be a number")
    return float(value)


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise WireError(f"{name} must be a string")
    return value
