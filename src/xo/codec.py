from __future__ import annotations

import base64
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from .exceptions import CodecError


@dataclass(frozen=True, slots=True)
class CodecLimits:
    max_depth: int = 64
    max_items: int = 100_000
    max_bytes: int = 16 * 1024 * 1024


class Extension(Protocol):
    tag: str

    def matches(self, value: object) -> bool: ...

    def encode(self, value: object) -> object: ...

    def decode(self, value: object) -> object: ...


class Codec:
    """Deterministic JSON codec. It never imports or executes encoded types."""

    __slots__ = ("_by_tag", "_extensions", "limits")

    def __init__(self, *, limits: CodecLimits | None = None) -> None:
        self.limits = limits or CodecLimits()
        self._extensions: list[Extension] = []
        self._by_tag: dict[str, Extension] = {}

    def register(self, extension: Extension) -> None:
        if not extension.tag or extension.tag in {"bytes", "tuple"}:
            raise CodecError(f"invalid or reserved codec tag: {extension.tag!r}")
        if extension.tag in self._by_tag:
            raise CodecError(f"codec tag already registered: {extension.tag!r}")
        self._extensions.append(extension)
        self._by_tag[extension.tag] = extension

    def validate(self, value: object) -> None:
        """Reject values that cannot cross XO's portable JSON boundary."""
        if value is None or isinstance(value, bool):
            return
        if isinstance(value, int):
            bits = value.bit_length()
            encoded_bound = 1 if bits == 0 else (bits * 30_103) // 100_000 + 1
            if value < 0:
                encoded_bound += 1
            if encoded_bound > self.limits.max_bytes:
                raise CodecError(f"encoded value exceeds {self.limits.max_bytes} bytes")
            return
        self.dumps(value)

    def dumps(self, value: object) -> bytes:
        encoded = self._encode(value, depth=0, count=[0])
        data = json.dumps(
            encoded,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(data) > self.limits.max_bytes:
            raise CodecError(f"encoded value exceeds {self.limits.max_bytes} bytes")
        return data

    def loads(self, data: bytes | bytearray | memoryview | str) -> object:
        raw = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        if len(raw) > self.limits.max_bytes:
            raise CodecError(f"encoded value exceeds {self.limits.max_bytes} bytes")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise CodecError(f"invalid XO JSON: {error}") from error
        return self._decode(value, depth=0, count=[0])

    def _visit(self, count: list[int], depth: int) -> None:
        if depth > self.limits.max_depth:
            raise CodecError(f"value nesting exceeds depth {self.limits.max_depth}")
        count[0] += 1
        if count[0] > self.limits.max_items:
            raise CodecError(f"value contains more than {self.limits.max_items} items")

    def _encode(self, value: object, *, depth: int, count: list[int]) -> object:
        self._visit(count, depth)
        if value is None or isinstance(value, bool | int | str):
            return value
        if isinstance(value, float):
            if value != value or value in (float("inf"), float("-inf")):
                raise CodecError("NaN and infinity are not valid XO values")
            return value
        if isinstance(value, bytes | bytearray | memoryview):
            return {"$xo": "bytes", "value": base64.b64encode(bytes(value)).decode("ascii")}
        if isinstance(value, tuple):
            return {
                "$xo": "tuple",
                "value": [self._encode(item, depth=depth + 1, count=count) for item in value],
            }
        if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
            return [self._encode(item, depth=depth + 1, count=count) for item in value]
        if isinstance(value, Mapping):
            result: dict[str, object] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise CodecError("XO mapping keys must be strings")
                result[key] = self._encode(item, depth=depth + 1, count=count)
            return result
        for extension in self._extensions:
            if extension.matches(value):
                return {
                    "$xo": extension.tag,
                    "value": self._encode(extension.encode(value), depth=depth + 1, count=count),
                }
        raise CodecError(f"unsupported XO value type: {type(value).__qualname__}")

    def _decode(self, value: object, *, depth: int, count: list[int]) -> object:
        self._visit(count, depth)
        if isinstance(value, list):
            return [self._decode(item, depth=depth + 1, count=count) for item in value]
        if not isinstance(value, dict):
            return value
        if set(value) == {"$xo", "value"}:
            tag = value["$xo"]
            payload = self._decode(value["value"], depth=depth + 1, count=count)
            if tag == "bytes":
                try:
                    return base64.b64decode(payload, validate=True)
                except (TypeError, ValueError) as error:
                    raise CodecError("invalid base64 bytes payload") from error
            if tag == "tuple":
                if not isinstance(payload, list):
                    raise CodecError("tuple payload must be a list")
                return tuple(payload)
            extension = self._by_tag.get(tag)
            if extension is None:
                raise CodecError(f"unknown XO codec tag: {tag!r}")
            return extension.decode(payload)
        return {
            str(key): self._decode(item, depth=depth + 1, count=count)
            for key, item in value.items()
        }


DEFAULT_CODEC = Codec()
