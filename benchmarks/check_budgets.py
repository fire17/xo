from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import time
from collections.abc import Callable

from xo import XO

BUDGETS_US = {
    "create_root": 10.0,
    "existing_read": 1.0,
    "scalar_set": 5.0,
    "five_segment_set": 15.0,
    "clean_formula_read": 2.0,
}
IMPORT_BUDGET_MS = 25.0


def median_us(function: Callable[[], object], *, loops: int, rounds: int) -> float:
    samples: list[float] = []
    for _ in range(rounds):
        started = time.perf_counter_ns()
        for _ in range(loops):
            function()
        samples.append((time.perf_counter_ns() - started) / loops / 1_000)
    return statistics.median(samples)


def import_median_ms(rounds: int) -> float:
    program = (
        "import time; "
        "started=time.perf_counter_ns(); "
        "import xo; "
        "print((time.perf_counter_ns()-started)/1_000_000)"
    )
    samples = [
        float(
            subprocess.run(
                [sys.executable, "-c", program],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        )
        for _ in range(rounds)
    ]
    return statistics.median(samples)


def measure(*, loops: int, rounds: int, import_rounds: int) -> dict[str, float]:
    state = XO("budget")
    node = state.item
    node.set(0)
    counter = 0

    def scalar_set() -> None:
        nonlocal counter
        counter += 1
        node.set(counter)

    def existing_read() -> object:
        return node.value

    path_state = XO("path-budget")
    path_node = path_state.a.b.c.d.e
    path_counter = 0

    def five_segment_set() -> None:
        nonlocal path_counter
        path_counter += 1
        path_node.set(path_counter)

    formula = XO("formula-budget")
    formula.source = 1
    formula.total.derive(lambda: formula.source.value + 1)
    _ = formula.total.value

    def clean_formula_read() -> object:
        return formula.total.value

    root_counter = 0

    def create_root() -> XO:
        nonlocal root_counter
        root_counter += 1
        return XO(f"root-{root_counter}")

    return {
        "import_xo_ms": import_median_ms(import_rounds),
        "create_root": median_us(create_root, loops=loops, rounds=rounds),
        "existing_read": median_us(existing_read, loops=loops, rounds=rounds),
        "scalar_set": median_us(scalar_set, loops=loops, rounds=rounds),
        "five_segment_set": median_us(five_segment_set, loops=loops, rounds=rounds),
        "clean_formula_read": median_us(clean_formula_read, loops=loops, rounds=rounds),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail when XO exceeds architecture budgets")
    parser.add_argument("--loops", type=int, default=20_000)
    parser.add_argument("--rounds", type=int, default=15)
    parser.add_argument("--import-rounds", type=int, default=15)
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()
    if min(args.loops, args.rounds, args.import_rounds) <= 0:
        parser.error("all counts must be positive")

    observed = measure(
        loops=args.loops,
        rounds=args.rounds,
        import_rounds=args.import_rounds,
    )
    failures = {
        name: {"observed": observed[name], "budget": budget}
        for name, budget in BUDGETS_US.items()
        if observed[name] > budget
    }
    if observed["import_xo_ms"] > IMPORT_BUDGET_MS:
        failures["import_xo_ms"] = {
            "observed": observed["import_xo_ms"],
            "budget": IMPORT_BUDGET_MS,
        }
    report = {
        "unit": {"import_xo_ms": "ms", **{name: "us" for name in BUDGETS_US}},
        "observed": observed,
        "budgets": {"import_xo_ms": IMPORT_BUDGET_MS, **BUDGETS_US},
        "failures": failures,
    }
    if args.as_json:
        print(json.dumps(report, sort_keys=True))
    else:
        for name, value in observed.items():
            unit = report["unit"][name]
            budget = report["budgets"][name]
            print(f"{name}: {value:.3f} {unit} (budget {budget:.3f} {unit})")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
