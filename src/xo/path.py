from __future__ import annotations

from collections.abc import Iterable
from typing import TypeAlias

from .exceptions import PathError

Path: TypeAlias = tuple[str, ...]
PathLike: TypeAlias = str | Iterable[str]

_MAX_SEGMENTS = 256
_MAX_SEGMENT_BYTES = 1_024
_MAX_PATH_BYTES = 16_384


def parse_path(path: PathLike | None, *, separator: str = ".") -> Path:
    if path is None or path == "":
        return ()
    if isinstance(path, str):
        if not separator:
            raise PathError("path separator cannot be empty")
        parts = tuple(path.split(separator))
    else:
        parts = tuple(path)
    return validate_path(parts)


def validate_path(path: Path) -> Path:
    if len(path) > _MAX_SEGMENTS:
        raise PathError(f"path has more than {_MAX_SEGMENTS} segments")
    total = 0
    for part in path:
        if not isinstance(part, str):
            raise PathError("path segments must be strings")
        if not part:
            raise PathError("path segments cannot be empty")
        if "\x00" in part:
            raise PathError("path segments cannot contain NUL")
        size = len(part.encode("utf-8"))
        if size > _MAX_SEGMENT_BYTES:
            raise PathError(f"path segment exceeds {_MAX_SEGMENT_BYTES} UTF-8 bytes")
        total += size
    if total > _MAX_PATH_BYTES:
        raise PathError(f"path exceeds {_MAX_PATH_BYTES} UTF-8 bytes")
    return path


def render_path(path: Path, *, separator: str = ".") -> str:
    return separator.join(path)


def is_prefix(prefix: Path, path: Path) -> bool:
    return len(prefix) <= len(path) and path[: len(prefix)] == prefix
