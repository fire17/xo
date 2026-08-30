from __future__ import annotations

from xo import XO, History, RPCServer, Service, projection, rpc_server, validation


def test_recommended_profile_is_ordinary_capability_composition() -> None:
    projected = []
    state = XO.recommended(
        "app",
        projections=(projection(projected.append, key="web", kind="web"),),
        validation=validation({"count": lambda value: isinstance(value, int) or 1 / 0}),
    )
    state.count = 1

    keys = {capability["key"] for capability in state.capabilities}
    assert keys == {"validation", "history", "service", "web"}
    assert isinstance(state.capability("history"), History)
    assert isinstance(state.capability("service"), Service)
    assert [event.path for event in projected] == [("count",)]


def test_recommended_profile_can_include_owned_rpc_transport(tmp_path) -> None:
    address = f"unix:///tmp/xo-profile-{tmp_path.name}.sock"
    state = XO.recommended("app", services=(rpc_server(address),))

    keys = {capability["key"] for capability in state.capabilities}
    assert keys == {"history", "service", "rpc"}
    assert isinstance(state.capability("rpc"), RPCServer)
    assert state.capability("rpc").server.registry is state.service
    state.close()
