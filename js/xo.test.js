import { afterEach, describe, expect, test } from "bun:test";

import { closeXO, createXO, XOProtocolError } from "./xo.js";

class FakeSocket {
  constructor() {
    this.readyState = 0;
    this.sent = [];
    this.listeners = new Map();
  }

  addEventListener(kind, callback) {
    const listeners = this.listeners.get(kind) ?? [];
    listeners.push(callback);
    this.listeners.set(kind, listeners);
  }

  emit(kind, event = {}) {
    for (const callback of this.listeners.get(kind) ?? []) callback(event);
  }

  open() {
    this.readyState = 1;
    this.emit("open");
  }

  deliver(envelope) {
    this.emit("message", { data: JSON.stringify(envelope) });
  }

  send(payload) {
    this.sent.push(JSON.parse(payload));
  }

  close() {
    this.readyState = 3;
  }
}

const TOKEN = "t".repeat(32);
let current = null;

afterEach(() => {
  if (current !== null) closeXO(current);
  current = null;
});

function connected({ writable = false } = {}) {
  const socket = new FakeSocket();
  const states = [];
  current = createXO({
    url: "ws://127.0.0.1:7802/xo",
    namespace: "app",
    token: TOKEN,
    prefixes: [["ui"]],
    writable,
    socketFactory: () => socket,
    reconnect: false,
    onState: (change) => states.push(change.state),
  });
  socket.open();
  expect(socket.sent.at(-1).k).toBe("hello");
  socket.deliver({
    k: "welcome",
    mid: 1,
    ns: "app",
    p: { protocol: 1, schema: 1 },
  });
  expect(socket.sent.at(-1).k).toBe("sub");
  socket.deliver({
    k: "snapshot",
    mid: 2,
    ns: "app",
    p: {
      schema: "xo.snapshot",
      version: 1,
      namespace: "app",
      revision: 0,
      root: { $children: [["ui", { $children: [["count", { $value: 1, $children: [] }]] }]] },
    },
  });
  socket.deliver({
    k: "ack",
    mid: 3,
    ns: "app",
    rid: 2,
    p: { mode: "snapshot", revision: 0, count: 0 },
  });
  return { xo: current, socket, states };
}

function eventEnvelope(mid, revision, path, value) {
  return {
    k: "event",
    mid,
    ns: "app",
    p: {
      event_id: revision.toString(16),
      namespace: "app",
      origin_id: "2",
      base_revision: revision - 1,
      revision,
      operation: "set_value",
      path,
      payload: { new: value },
    },
  };
}

describe("XO JavaScript peer", () => {
  test("hydrates snapshots and applies contiguous authored events", () => {
    const { xo, socket, states } = connected();
    const changes = [];
    const subscription = xo.ui.subscribe((change) => changes.push(change));

    expect(xo.ui.count.value).toBe(1);
    socket.deliver(eventEnvelope(4, 1, ["ui", "count"], 2));

    expect(xo.ui.count.value).toBe(2);
    expect(changes.at(-1)).toMatchObject({ kind: "event", revision: 1 });
    expect(states).toContain("ready");
    expect(socket.sent.at(-1)).toMatchObject({ k: "ack", p: { revision: 1 } });
    subscription.close();
  });

  test("sends revision-guarded writes and resolves their acknowledgement", async () => {
    const { xo, socket } = connected({ writable: true });
    socket.deliver(eventEnvelope(4, 1, ["ui", "count"], 2));

    const pending = xo.ui.name.set("Tami");
    const request = socket.sent.at(-1);
    expect(request).toMatchObject({
      k: "set",
      p: { path: ["ui", "name"], value: "Tami", expected_revision: 1 },
    });

    socket.deliver(eventEnvelope(5, 2, ["ui", "name"], "Tami"));
    socket.deliver({
      k: "ack",
      mid: 6,
      ns: "app",
      rid: request.mid,
      p: { mode: "write", revision: 2, event_id: "2" },
    });

    await expect(pending).resolves.toMatchObject({ revision: 2 });
    expect(xo.ui.name.value).toBe("Tami");
  });

  test("keeps derived projections outside authored state", () => {
    const { xo, socket } = connected();
    socket.deliver({
      k: "derived",
      mid: 4,
      ns: "app",
      p: {
        origin_id: "2",
        cause_revision: 0,
        path: ["cart", "total"],
        generation: 1,
        status: "value",
        value: 12,
      },
    });

    expect(xo.derived.cart.total.value).toBe(12);
    expect(xo.cart.total.exists).toBe(false);
  });

  test("refuses offline and unauthorized writes", async () => {
    const { xo } = connected();
    await expect(xo.ui.name.set("blocked")).rejects.toBeInstanceOf(XOProtocolError);
  });
  test("resets wire message IDs for each reconnect session", async () => {
    const sockets = [];
    const states = [];
    current = createXO({
      url: "ws://127.0.0.1:7802/xo",
      namespace: "app",
      token: TOKEN,
      prefixes: [["ui"]],
      minBackoff: 1,
      maxBackoff: 1,
      socketFactory: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket;
      },
      onState: (change) => states.push(change.state),
    });

    const first = sockets[0];
    first.open();
    first.deliver({ k: "welcome", mid: 1, ns: "app", p: { protocol: 1, schema: 1 } });
    first.readyState = 3;
    first.emit("close");
    await Bun.sleep(5);

    const second = sockets[1];
    second.open();
    expect(second.sent[0]).toMatchObject({ k: "hello", mid: 1 });
    second.deliver({ k: "welcome", mid: 1, ns: "app", p: { protocol: 1, schema: 1 } });
    expect(second.readyState).toBe(1);
    expect(states.at(-1)).toBe("catching_up");
  });

});
