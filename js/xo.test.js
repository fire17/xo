import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";

import { closeXO, createXO, XOProtocolError } from "./xo.js";

const PARITY_FIXTURE = JSON.parse(
  readFileSync(new URL("../tests/fixtures/language_parity.json", import.meta.url), "utf8"),
);

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

function fixtureSnapshot() {
  const socket = new FakeSocket();
  current = createXO({
    url: "ws://127.0.0.1:7802/xo",
    namespace: PARITY_FIXTURE.namespace,
    token: TOKEN,
    prefixes: [[]],
    socketFactory: () => socket,
    reconnect: false,
  });
  socket.open();
  socket.deliver({ k: "welcome", mid: 1, ns: PARITY_FIXTURE.namespace, p: { protocol: 1, schema: 1 } });
  socket.deliver({
    k: "snapshot",
    mid: 2,
    ns: PARITY_FIXTURE.namespace,
    p: { schema: "xo.snapshot", version: 1, namespace: PARITY_FIXTURE.namespace, revision: 0, root: PARITY_FIXTURE.initial },
  });
  socket.deliver({ k: "ack", mid: 3, ns: PARITY_FIXTURE.namespace, rid: 2, p: { mode: "snapshot", revision: 0, count: 0 } });
  return current;
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
  test("consumes the shared value-plus-children fixture", () => {
    const xo = fixtureSnapshot();
    expect(xo.shared.value).toBe("parent");
    expect(xo.shared.counter.value).toBe(1);
    expect(xo.shared.clearable.value).toBe("remove-value");
    expect(xo.shared.clearable.kept.value).toBe(true);
    expect(xo.shared.keys).toEqual(["counter", "clearable", "deletable"]);
  });
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
  test("returns rejected promises for invalid explicit writes", async () => {
    const { xo, socket } = connected({ writable: true });
    const before = socket.sent.length;

    await expect(xo.ui.payload.set(Number.MAX_SAFE_INTEGER + 1)).rejects.toThrow(
      "XO integers must be safe JavaScript integers",
    );
    await expect(xo.ui.payload.set(Symbol("invalid"))).rejects.toThrow(
      "unsupported XO value type: symbol",
    );
    expect(socket.sent).toHaveLength(before);
  });

  test("reports failed assignment writes to subscribers", async () => {
    const { xo } = connected();
    const changes = [];
    xo.subscribe((change) => changes.push(change));

    xo.ui.name = "blocked";
    await Bun.sleep(0);

    expect(changes.at(-1)).toMatchObject({
      kind: "error",
      error: { code: "xo.auth.invalid" },
    });
  });

  test("matches node reads, iteration, scoped subscriptions, clear, restore, and atomic writes", async () => {
    const { xo, socket } = connected({ writable: true });
    expect(xo.revision).toBe(0);
    expect(xo.ui.get("missing")).toBe("missing");
    expect(xo.ui.keys).toEqual(["count"]);
    expect([...xo.ui]).toEqual(["count"]);
    expect(xo.ui.has("count")).toBe(true);
    expect(xo.ui.entries).toEqual([["count", 1]]);

    const scoped = [];
    xo.ui.subscribe((change) => scoped.push(change));
    socket.deliver(eventEnvelope(4, 1, ["other", "ignored"], 1));
    expect(scoped).toHaveLength(0);
    socket.deliver(eventEnvelope(5, 2, ["ui", "count"], 2));
    expect(scoped).toHaveLength(1);

    const clear = xo.ui.count.clear();
    const clearRequest = socket.sent.at(-1);
    expect(clearRequest).toMatchObject({ k: "clear", p: { path: ["ui", "count"], expected_revision: 2 } });
    socket.deliver({
      k: "event", mid: 6, ns: "app", p: {
        event_id: "3", namespace: "app", origin_id: "2", base_revision: 2,
        revision: 3, operation: "clear_value", path: ["ui", "count"], payload: {},
      },
    });
    socket.deliver({ k: "ack", mid: 7, ns: "app", rid: clearRequest.mid, p: { mode: "write", revision: 3, event_id: "3", count: 1 } });
    await expect(clear).resolves.toMatchObject({ revision: 3 });
    expect(xo.ui.count.exists).toBe(true);
    expect(xo.ui.count.hasValue).toBe(false);

    const tx = xo.transaction([
      { kind: "set", path: ["ui", "first"], value: 1 },
      { kind: "set", path: "ui.second", value: new Uint8Array([1, 2]) },
    ]);
    const txRequest = socket.sent.at(-1);
    expect(txRequest).toMatchObject({
      k: "tx",
      p: {
        expected_revision: 3,
        operations: [
          { kind: "set", path: ["ui", "first"], value: 1 },
          { kind: "set", path: ["ui", "second"], value: { $xo: "bytes", value: "AQI=" } },
        ],
      },
    });
    socket.deliver({ k: "ack", mid: 8, ns: "app", rid: txRequest.mid, p: { mode: "write", revision: 4, event_id: "4", count: 2 } });
    await expect(tx).resolves.toMatchObject({ revision: 4, count: 2 });

    const restore = xo.ui.restore({ $value: "parent", $children: [["child", { $value: 7, $children: [] }]] });
    const restoreRequest = socket.sent.at(-1);
    expect(restoreRequest).toMatchObject({
      k: "restore",
      p: { path: ["ui"], node: { $value: "parent", $children: [["child", { $value: 7, $children: [] }]] } },
    });
    socket.deliver({ k: "ack", mid: 9, ns: "app", rid: restoreRequest.mid, p: { mode: "write", revision: 5, event_id: "5", count: 1 } });
    await expect(restore).resolves.toMatchObject({ revision: 5 });
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
