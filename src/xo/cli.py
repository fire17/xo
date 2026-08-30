from __future__ import annotations

import argparse
import importlib
import json
import platform
import statistics
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Protocol

from . import __version__
from .codec import DEFAULT_CODEC
from .core import XO


class ClosableServer(Protocol):
    def start(self) -> object: ...

    def close(self) -> object: ...

    def wait(self) -> object: ...


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="xo", description="XO state tools")
    parser.add_argument("--version", action="version", version=f"xo {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)

    inspect = commands.add_parser("inspect", help="validate and inspect a snapshot")
    inspect.add_argument("snapshot", type=Path)
    inspect.add_argument("--json", action="store_true", dest="as_json")

    doctor = commands.add_parser("doctor", help="report runtime and optional capabilities")
    doctor.add_argument("--json", action="store_true", dest="as_json")

    benchmark = commands.add_parser("benchmark", help="measure local core operations")
    benchmark.add_argument("--loops", type=_positive_int, default=20_000)
    benchmark.add_argument("--rounds", type=_positive_int, default=9)
    benchmark.add_argument("--json", action="store_true", dest="as_json")

    serve = commands.add_parser("serve", help="run an explicitly constructed local XO server")
    serve.add_argument("target", help="module:attribute resolving to Server or zero-arg factory")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "inspect":
        return _inspect(args.snapshot, as_json=args.as_json)
    if args.command == "doctor":
        return _doctor(as_json=args.as_json)
    if args.command == "benchmark":
        return _benchmark(args.loops, args.rounds, as_json=args.as_json)
    if args.command == "serve":
        return _serve(args.target)
    raise AssertionError(args.command)


def _inspect(path: Path, *, as_json: bool) -> int:
    try:
        value = DEFAULT_CODEC.loads(path.read_bytes())
        snapshot = _validate_snapshot(value)
    except (OSError, ValueError) as error:
        print(f"xo inspect: {error}", file=sys.stderr)
        return 2

    if as_json:
        print(json.dumps(snapshot, ensure_ascii=False, sort_keys=True))
        return 0

    root = snapshot["root"]
    nodes, values, depth = _snapshot_stats(root)
    print(f"namespace: {snapshot['namespace']}")
    print(f"revision: {snapshot['revision']}")
    print(f"nodes: {nodes}")
    print(f"values: {values}")
    print(f"max_depth: {depth}")
    return 0


def _doctor(*, as_json: bool) -> int:
    optional: dict[str, bool] = {}
    for name, module in (
        ("redis", "xo.backends.redis"),
        ("rpc", "xo.rpc"),
        ("websocket", "xo.web"),
        ("compat", "xo.compat"),
    ):
        try:
            importlib.import_module(module)
        except ImportError:
            optional[name] = False
        else:
            optional[name] = True
    report = {
        "version": __version__,
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "core_dependency_free": True,
        "optional": optional,
    }
    if as_json:
        print(json.dumps(report, sort_keys=True))
    else:
        print(f"XO {report['version']} on Python {report['python']} ({report['platform']})")
        for name, available in optional.items():
            print(f"{name}: {'available' if available else 'unavailable'}")
    return 0 if all(optional.values()) else 1


def _benchmark(loops: int, rounds: int, *, as_json: bool) -> int:
    state = XO("benchmark")
    node = state.item
    counter = 0

    def scalar_set() -> None:
        nonlocal counter
        counter += 1
        node.set(counter)

    scalar_set()

    def existing_read() -> object:
        return node.value

    formula_state = XO("formula-benchmark")
    formula_state.source = 1
    formula_state.computed.derive(lambda: formula_state.source.value + 1)
    _ = formula_state.computed.value

    def formula_read() -> object:
        return formula_state.computed.value

    report = {
        "loops": loops,
        "rounds": rounds,
        "scalar_set_us": _measure(scalar_set, loops, rounds),
        "existing_read_us": _measure(existing_read, loops, rounds),
        "clean_formula_read_us": _measure(formula_read, loops, rounds),
    }
    if as_json:
        print(json.dumps(report, sort_keys=True))
    else:
        for name, value in report.items():
            print(f"{name}: {value}")
    return 0


def _serve(target: str) -> int:
    module_name, separator, attribute_path = target.partition(":")
    if not separator or not module_name or not attribute_path:
        print("xo serve: target must be module:attribute", file=sys.stderr)
        return 2
    try:
        value: object = importlib.import_module(module_name)
        for segment in attribute_path.split("."):
            value = getattr(value, segment)
        if callable(value) and not _is_server(value):
            value = value()
        if not _is_server(value):
            raise TypeError("target must provide start(), wait(), and close()")
        server: ClosableServer = value
        server.start()
        try:
            server.wait()
        finally:
            server.close()
    except KeyboardInterrupt:
        return 130
    except (ImportError, AttributeError, TypeError, OSError, RuntimeError) as error:
        print(f"xo serve: {error}", file=sys.stderr)
        return 1
    return 0


def _is_server(value: object) -> bool:
    return all(callable(getattr(value, name, None)) for name in ("start", "wait", "close"))


def _measure(function: Callable[[], object], loops: int, rounds: int) -> float:
    samples: list[float] = []
    for _ in range(rounds):
        started = time.perf_counter_ns()
        for _ in range(loops):
            function()
        samples.append((time.perf_counter_ns() - started) / loops / 1_000)
    return statistics.median(samples)


def _validate_snapshot(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("snapshot must be an object")
    required = {"schema", "version", "namespace", "revision", "root"}
    missing = required - value.keys()
    if missing:
        raise ValueError(f"snapshot missing fields: {', '.join(sorted(missing))}")
    if value["schema"] != "xo.snapshot" or value["version"] != 1:
        raise ValueError("unsupported snapshot schema or version")
    if not isinstance(value["namespace"], str):
        raise ValueError("snapshot namespace must be a string")
    revision = value["revision"]
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
        raise ValueError("snapshot revision must be a non-negative integer")
    _snapshot_stats(value["root"])
    return value


def _snapshot_stats(value: object, depth: int = 0) -> tuple[int, int, int]:
    if not isinstance(value, dict):
        raise ValueError("snapshot node must be an object")
    if set(value) - {"$value", "$children"} or "$children" not in value:
        raise ValueError("invalid snapshot node fields")
    children = value["$children"]
    if not isinstance(children, list):
        raise ValueError("snapshot children must be a list")
    nodes = 1
    values = int("$value" in value)
    max_depth = depth
    seen: set[str] = set()
    for child in children:
        if not isinstance(child, list) or len(child) != 2 or not isinstance(child[0], str):
            raise ValueError("snapshot child must be [name, node]")
        if child[0] in seen:
            raise ValueError(f"duplicate snapshot child: {child[0]!r}")
        seen.add(child[0])
        child_nodes, child_values, child_depth = _snapshot_stats(child[1], depth + 1)
        nodes += child_nodes
        values += child_values
        max_depth = max(max_depth, child_depth)
    return nodes, values, max_depth


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
