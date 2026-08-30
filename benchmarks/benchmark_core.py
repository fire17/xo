from __future__ import annotations

import argparse
import json
import platform
import statistics
import time
from collections.abc import Callable

from xo import XO


def measure(function: Callable[[], object], *, loops: int, rounds: int) -> dict[str, float]:
    samples: list[float] = []
    for _ in range(rounds):
        started = time.perf_counter_ns()
        for _ in range(loops):
            function()
        samples.append((time.perf_counter_ns() - started) / loops / 1_000)
    ordered = sorted(samples)
    return {
        "median_us": statistics.median(ordered),
        "p95_us": ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))],
        "min_us": ordered[0],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--loops", type=int, default=100_000)
    parser.add_argument("--rounds", type=int, default=15)
    args = parser.parse_args()

    state = XO("bench")
    node = state.value_node
    counter = 0

    def scalar_set() -> None:
        nonlocal counter
        counter += 1
        node.set(counter)

    node.set(1)

    def existing_read() -> object:
        return node.value

    formula_state = XO("formula-bench")
    formula_state.source = 1
    formula_state.computed.derive(lambda: formula_state.source.value + 1)
    _warm_formula_value = formula_state.computed.value

    def computed_read() -> object:
        return formula_state.computed.value

    results = {
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
        "loops": args.loops,
        "rounds": args.rounds,
        "scalar_set": measure(scalar_set, loops=args.loops, rounds=args.rounds),
        "existing_read": measure(existing_read, loops=args.loops, rounds=args.rounds),
        "clean_formula_read": measure(computed_read, loops=args.loops, rounds=args.rounds),
    }
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
