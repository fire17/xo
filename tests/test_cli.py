from __future__ import annotations

import json

from xo import XO
from xo.cli import main


def test_inspect_reports_snapshot_shape(tmp_path, capsys) -> None:
    state = XO("demo")
    state.user.name = "Tami"
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_bytes(state.snapshot_bytes())

    assert main(["inspect", str(snapshot)]) == 0
    output = capsys.readouterr().out
    assert "namespace: demo" in output
    assert "nodes: 3" in output
    assert "values: 1" in output


def test_inspect_rejects_invalid_snapshot_without_traceback(tmp_path, capsys) -> None:
    snapshot = tmp_path / "broken.json"
    snapshot.write_text('{"schema":"xo.snapshot"}', encoding="utf-8")

    assert main(["inspect", str(snapshot)]) == 2
    assert "snapshot missing fields" in capsys.readouterr().err


def test_doctor_json_is_machine_readable(capsys) -> None:
    result = main(["doctor", "--json"])
    report = json.loads(capsys.readouterr().out)

    assert result in {0, 1}
    assert report["core_dependency_free"] is True
    assert set(report["optional"]) == {"redis", "rpc", "websocket", "compat"}


def test_benchmark_json_reports_positive_medians(capsys) -> None:
    assert main(["benchmark", "--loops", "5", "--rounds", "3", "--json"]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["scalar_set_us"] > 0
    assert report["existing_read_us"] > 0
    assert report["clean_formula_read_us"] > 0
