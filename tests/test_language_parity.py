from __future__ import annotations

import json
import subprocess
from pathlib import Path

from xo import XO
from xo.web import WebSocketBridge, websocket

TOKEN = "p" * 32
ROOT = Path(__file__).parent.parent
FIXTURE = Path(__file__).parent / "fixtures" / "language_parity.json"
PEER = Path(__file__).parent / "helpers" / "javascript_parity_peer.js"


def test_python_and_javascript_share_one_live_xo_environment_bidirectionally() -> None:
    fixture = json.loads(FIXTURE.read_text())
    state = XO.compose(
        fixture["namespace"],
        websocket(token=TOKEN, writable=((),)),
    )
    state.install_snapshot(
        {
            "schema": "xo.snapshot",
            "version": 1,
            "namespace": fixture["namespace"],
            "revision": 0,
            "root": fixture["initial"],
        }
    )
    state.shared.pythonBytes = b"\x00\x01\x02\xff"
    state.shared.pythonTuple = ("fixed", 7)
    state.start()
    bridge = state.capability("websocket")
    assert isinstance(bridge, WebSocketBridge)

    process = subprocess.Popen(
        [
            "bun",
            str(PEER),
            json.dumps(
                {
                    "url": bridge.url,
                    "namespace": fixture["namespace"],
                    "token": TOKEN,
                }
            ),
        ],
        cwd=ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        line = process.stdout.readline()
        if not line:
            return_code = process.wait(timeout=5)
            stderr = "" if process.stderr is None else process.stderr.read()
            raise AssertionError(f"JavaScript peer exited {return_code}: {stderr}")
        report = json.loads(line)
        assert report["before"] == {
            "revision": 2,
            "parent": "parent",
            "counter": 1,
            "clearable": "remove-value",
            "keys": ["counter", "clearable", "deletable", "pythonBytes", "pythonTuple"],
            "bytes": {"bytes": [0, 1, 2, 255]},
            "tuple": ["fixed", 7],
        }

        assert state.shared.counter.value == 2
        assert state.shared.fromJs.value == {
            "language": "javascript",
            "bytes": b"\x04\x05\x06",
        }
        assert state.shared.clearable.exists
        assert not state.shared.clearable.has_value
        assert state.shared.clearable.kept.value is True
        assert not state.shared.deletable.exists
        assert state.shared.restored["child"].value == 9
        assert state.revision == report["afterRevision"] == 4

        state.shared.pythonAfterJs = {"status": "seen", "revision": state.revision}
        state.shared.counter = 3
        assert state.revision == 6
        assert process.stdin is not None
        process.stdin.write("verify\n")
        process.stdin.flush()
        final = json.loads(process.stdout.readline())
        assert final == {
            "finalRevision": 6,
            "pythonAfterJs": {"status": "seen", "revision": 4},
            "counter": 3,
        }
    finally:
        if process.stdin is not None:
            process.stdin.close()
        return_code = process.wait(timeout=5)
        stderr = "" if process.stderr is None else process.stderr.read()
        state.close()
    assert return_code == 0, stderr


def test_shared_language_fixture_round_trips_through_canonical_python_codec() -> None:
    from xo.codec import DEFAULT_CODEC

    fixture = json.loads(FIXTURE.read_text())
    decoded = DEFAULT_CODEC.loads(json.dumps(fixture["values"]))
    assert decoded == {
        "null": None,
        "bool": True,
        "safe_integer": 9_007_199_254_740_991,
        "negative_integer": -42,
        "float": 3.25,
        "unicode": "mushroom 🍄 שלום",
        "bytes": b"\x00\x01\x02\xff",
        "tuple": ("fixed", 7),
        "list": [None, False, 2.5, "leaf"],
        "object": {"nested": {"enabled": True}},
    }


def test_javascript_peer_cannot_silently_accept_python_integer_beyond_safe_range() -> None:
    state = XO.compose("parity", websocket(token=TOKEN))
    state.tooLarge = 9_007_199_254_740_992
    state.start()
    bridge = state.capability("websocket")
    assert isinstance(bridge, WebSocketBridge)
    process = subprocess.run(
        [
            "bun",
            "-e",
            (
                "import {createXO} from './js/xo.js';"
                f"createXO({{url:{json.dumps(bridge.url)},namespace:'parity',token:{json.dumps(TOKEN)},reconnect:false," 
                "onState:s=>{if(s.state==='disconnected'){console.log(s.error.code);process.exit(0)}}});"
                "setTimeout(()=>process.exit(2),2000);"
            ),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    state.close()
    assert process.returncode == 0, process.stderr
    assert process.stdout.strip() == "xo.codec.integer_out_of_range"
