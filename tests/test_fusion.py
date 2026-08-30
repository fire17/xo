from __future__ import annotations

from dataclasses import dataclass, field

from xo import (
    XO,
    Event,
    EventGroup,
    History,
    RPCServer,
    WebSocketBridge,
    backend,
    projection,
    rpc_server,
    validation,
    websocket,
)
from xo.rpc import Client


@dataclass
class RecordingBackend:
    strict: bool = True
    events: list[Event | EventGroup] = field(default_factory=list)
    closed: bool = False

    def commit(self, item: Event | EventGroup) -> None:
        self.events.append(item)

    def reconcile(self, item: Event | EventGroup) -> bool:
        return item in self.events

    def close(self) -> None:
        self.closed = True


def test_full_fusion_uses_one_root_revision_and_owned_lifecycle(tmp_path) -> None:
    durable = RecordingBackend()
    projected: list[Event | EventGroup] = []
    address = f"unix:///tmp/xo-fusion-{tmp_path.name}.sock"
    state = XO.recommended(
        "app",
        durability=backend(durable),
        services=(rpc_server(address),),
        projections=(
            projection(projected.append, key="audit", kind="audit"),
            websocket(token="t" * 32, writable=(("ui",),)),
        ),
        validation=validation(
            {("ui", "count"): lambda value: isinstance(value, int) or 1 / 0}
        ),
    )

    @state.public.current_count
    def current_count() -> int:
        return state.ui.count.value

    state.start()
    state.ui.count = 1

    history = state.capability("history")
    rpc = state.capability("rpc")
    bridge = state.capability("websocket")
    assert isinstance(history, History)
    assert isinstance(rpc, RPCServer)
    assert isinstance(bridge, WebSocketBridge)
    assert state.revision == history.current() == 1
    assert len(durable.events) == len(projected) == 1
    assert durable.events[0] == projected[0]

    with Client(address, namespace="app") as client:
        assert client.current_count() == 1

    state.close()
    assert durable.closed
