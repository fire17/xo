from __future__ import annotations

import json
import platform
import threading

from xo import XO, history, service, validation


def main() -> None:
    state = XO.compose(
        "verify",
        validation({"count": lambda value: None if isinstance(value, int) else 1 / 0}),
        history(),
        service(),
    )
    state.count = 1
    state.double.derive(lambda: state.count.value * 2)

    values: list[int] = []
    threads = [threading.Thread(target=lambda: values.append(state.double.value)) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)
        if thread.is_alive():
            raise RuntimeError("formula reader did not terminate")

    if values != [2, 2, 2, 2]:
        raise AssertionError(values)
    if state.snapshot()["revision"] != 1:
        raise AssertionError(state.snapshot())
    if {item["key"] for item in state.capabilities} != {"history", "service", "validation"}:
        raise AssertionError(state.capabilities)

    print(
        json.dumps(
            {
                "python": platform.python_version(),
                "revision": state.revision,
                "formula_values": values,
                "capabilities": sorted(item["key"] for item in state.capabilities),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
