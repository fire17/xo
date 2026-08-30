const PROTOCOL = 1;
const SCHEMA = 1;
const MISSING = Symbol("xo.missing");
const INTERNAL = Symbol("xo.internal");

export class XOProtocolError extends Error {
  constructor(code, message, detail = {}) {
    super(message);
    this.name = "XOProtocolError";
    this.code = code;
    this.detail = detail;
  }
}

class Record {
  constructor() {
    this.value = MISSING;
    this.children = new Map();
  }
}

class Client {
  constructor(options) {
    if (!options || typeof options !== "object") throw new TypeError("options are required");
    if (typeof options.url !== "string" || !options.url.startsWith("ws://")) throw new TypeError("url must be a ws:// URL");
    if (typeof options.namespace !== "string" || !options.namespace) throw new TypeError("namespace must be a non-empty string");
    if (typeof options.token !== "string" || new TextEncoder().encode(options.token).length < 32) throw new TypeError("token must contain at least 32 UTF-8 bytes");
    this.url = options.url;
    this.namespace = options.namespace;
    this.token = options.token;
    this.role = options.writable === true ? "writer" : "observer";
    this.prefixes = normalizePrefixes(options.prefixes ?? [[]]);
    this.socketFactory = options.socketFactory ?? ((url) => new WebSocket(url));
    this.reconnect = options.reconnect !== false;
    this.minBackoff = positive(options.minBackoff ?? 100, "minBackoff");
    this.maxBackoff = positive(options.maxBackoff ?? 30000, "maxBackoff");
    this.maxPending = positive(options.maxPending ?? 256, "maxPending");
    this.onState = typeof options.onState === "function" ? options.onState : null;
    this.originId = randomHex(16);
    this.root = new Record();
    this.derivedRoot = new Record();
    this.derivedClock = new Map();
    this.seenEvents = new Set();
    this.seenOrder = [];
    this.maxSeen = positive(options.maxSeen ?? 10000, "maxSeen");
    this.revision = 0;
    this.incomingMid = 0;
    this.outgoingMid = 0;
    this.pending = new Map();
    this.listeners = new Set();
    this.socket = null;
    this.timer = null;
    this.attempt = 0;
    this.ready = false;
    this.closed = false;
    this.proxy = makeProxy(this, [], false);
    this.derivedProxy = makeProxy(this, [], true);
  }

  connect() {
    if (this.closed || this.socket) return;
    this.transition("connecting");
    let socket;
    try { socket = this.socketFactory(this.url); }
    catch (error) { this.scheduleReconnect(error); return; }
    if (!socket || typeof socket.addEventListener !== "function") {
      this.scheduleReconnect(new TypeError("socketFactory must return a WebSocket-compatible object"));
      return;
    }
    this.socket = socket;
    socket.addEventListener("open", () => this.open(socket));
    socket.addEventListener("message", (event) => void this.message(socket, event));
    socket.addEventListener("error", () => this.disconnect(socket, connectionLost("WebSocket error")));
    socket.addEventListener("close", () => this.disconnect(socket, connectionLost("WebSocket closed")));
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    this.ready = false;
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
    const socket = this.socket;
    this.socket = null;
    if (socket && socket.readyState < 2) socket.close(1000, "client closing");
    this.rejectPending(new XOProtocolError("xo.cancelled", "XO client closed"));
    this.transition("closed");
  }

  open(socket) {
    if (socket !== this.socket || this.closed) return;
    this.incomingMid = 0;
    this.outgoingMid = 0;
    this.transition("handshaking");
    this.send("hello", { protocol: PROTOCOL, min_protocol: PROTOCOL, schema: SCHEMA,
      origin_id: this.originId, client: "xo-js/1", role: this.role, token: this.token,
      restorable: false, diagnostics: false });
  }

  async message(socket, event) {
    if (socket !== this.socket || this.closed) return;
    try {
      let text;
      if (typeof event.data === "string") text = event.data;
      else if (event.data instanceof ArrayBuffer) text = new TextDecoder("utf-8", { fatal: true }).decode(event.data);
      else if (typeof Blob !== "undefined" && event.data instanceof Blob) text = new TextDecoder("utf-8", { fatal: true }).decode(await event.data.arrayBuffer());
      else throw malformed("unsupported WebSocket message data");
      const envelope = decodeEnvelope(text, this.namespace);
      if (envelope.mid <= this.incomingMid) throw malformed("server message ids must increase");
      this.incomingMid = envelope.mid;
      this.handle(envelope);
    } catch (error) {
      this.fail(error instanceof XOProtocolError ? error : malformed(String(error)));
    }
  }

  handle(envelope) {
    if (envelope.k === "welcome") {
      const payload = object(envelope.p, "welcome payload");
      if (uint(payload.protocol, "protocol") !== PROTOCOL || uint(payload.schema, "schema") !== SCHEMA) throw new XOProtocolError("xo.protocol.version", "unsupported protocol or schema");
      this.transition("catching_up");
      this.send("sub", { prefixes: this.prefixes, since_revision: this.revision, restorable: false, materialize: [] });
      return;
    }
    if (envelope.k === "snapshot") {
      const payload = object(envelope.p, "snapshot");
      if (payload.schema !== "xo.snapshot" || uint(payload.version, "snapshot version") !== 1) throw new XOProtocolError("xo.protocol.version", "unsupported snapshot schema");
      if (payload.namespace !== this.namespace) throw namespaceError("snapshot namespace mismatch");
      this.root = decodeImage(payload.root);
      this.revision = uint(payload.revision, "revision");
      this.notify({ kind: "snapshot", revision: this.revision });
      return;
    }
    if (envelope.k === "event") return this.applyEvents([envelope.p]);
    if (envelope.k === "tx") {
      const payload = object(envelope.p, "transaction");
      if (!Array.isArray(payload.events) || payload.events.length === 0) throw malformed("transaction events must be non-empty");
      return this.applyEvents(payload.events);
    }
    if (envelope.k === "derived") return this.applyDerived(envelope.p);
    if (envelope.k === "ack") {
      const payload = object(envelope.p, "ack");
      if (payload.mode === "watermark") this.advanceWatermark(uint(payload.revision, "revision"));
      if (payload.mode === "snapshot" || payload.mode === "catchup") {
        if (uint(payload.revision, "revision") !== this.revision) throw resync("catch-up acknowledgement differs from local revision");
        this.ready = true;
        this.attempt = 0;
        this.transition("ready");
      }
      if (envelope.rid !== undefined) this.resolve(envelope.rid, payload);
      return;
    }
    if (envelope.k === "ping") return void this.send("pong", envelope.p, envelope.mid);
    if (envelope.k === "pong") return;
    if (envelope.k === "error") {
      const payload = object(envelope.p, "error");
      const error = new XOProtocolError(String(payload.code), String(payload.message), payload.detail ?? {});
      if (envelope.rid !== undefined && this.pending.has(envelope.rid)) { this.reject(envelope.rid, error); return; }
      if (payload.code === "xo.resync_required") { this.revision = 0; return; }
      if (String(payload.code).startsWith("xo.protocol.") || payload.code === "xo.auth.invalid") throw error;
      this.notify({ kind: "error", error });
      return;
    }
    throw malformed(`unsupported server message kind: ${String(envelope.k)}`);
  }

  applyEvents(values) {
    const events = values.map((value) => decodeEvent(value, this.namespace));
    const novel = events.filter((event) => !this.seenEvents.has(event.eventId));
    if (novel.length === 0) {
      this.send("ack", { mode: "duplicate", revision: this.revision });
      return;
    }
    if (novel.length !== events.length) throw resync("transaction mixes duplicate and unseen events");
    const targetRevision = this.revision + 1;
    for (const event of events) {
      if (event.baseRevision !== this.revision || event.revision !== targetRevision) {
        throw resync(`event gap: expected revision ${targetRevision}, received ${event.revision}`);
      }
    }
    for (const event of events) {
      applyEvent(this.root, event);
      this.rememberEvent(event.eventId);
    }
    this.revision = targetRevision;
    this.send("ack", { mode: "event", revision: this.revision, event_id: events[0].eventId });
    this.notify({ kind: events.length === 1 ? "event" : "tx", events, revision: this.revision });
  }

  rememberEvent(eventId) {
    this.seenEvents.add(eventId);
    this.seenOrder.push(eventId);
    while (this.seenOrder.length > this.maxSeen) {
      this.seenEvents.delete(this.seenOrder.shift());
    }
  }

  applyDerived(value) {
    const payload = object(value, "derived projection");
    const path = wirePath(payload.path);
    const generation = uint(payload.generation, "generation");
    const causeRevision = uint(payload.cause_revision, "cause_revision");
    if (causeRevision > this.revision) return;
    const key = JSON.stringify(path);
    const previous = this.derivedClock.get(key);
    if (previous && (causeRevision < previous.causeRevision || (causeRevision === previous.causeRevision && generation <= previous.generation))) return;
    this.derivedClock.set(key, { generation, causeRevision });
    const record = resolve(this.derivedRoot, path, true);
    record.value = Object.hasOwn(payload, "value") ? decodeValue(payload.value) : Object.freeze({ status: String(payload.status ?? "error"), error: decodeValue(payload.error) });
    this.notify({ kind: "derived", path, generation, causeRevision, value: record.value });
  }

  advanceWatermark(revision) {
    if (revision < this.revision) return;
    if (revision !== this.revision + 1) throw resync("non-contiguous subscription watermark");
    this.revision = revision;
  }

  write(kind, path, value) {
    if (this.role !== "writer") return Promise.reject(new XOProtocolError("xo.auth.invalid", "writes are disabled"));
    if (!this.ready || !this.socket || this.socket.readyState !== 1) return Promise.reject(connectionLost("offline writes are refused"));
    if (this.pending.size >= this.maxPending) return Promise.reject(new XOProtocolError("xo.backpressure", "pending write queue is full"));
    let payload;
    try {
      payload = { path, expected_revision: this.revision };
      if (kind === "set") payload.value = encodeValue(value);
      const mid = this.send(kind, payload);
      return new Promise((resolvePromise, rejectPromise) => this.pending.set(mid, { resolve: resolvePromise, reject: rejectPromise }));
    } catch (error) {
      return Promise.reject(error);
    }
  }

  send(kind, payload, replyTo = undefined) {
    if (!this.socket || this.socket.readyState !== 1) throw connectionLost("WebSocket is not open");
    const envelope = { k: kind, mid: ++this.outgoingMid, ns: this.namespace, p: payload };
    if (replyTo !== undefined) envelope.rid = replyTo;
    this.socket.send(JSON.stringify(envelope));
    return envelope.mid;
  }

  resolve(id, value) { const pending = this.pending.get(id); if (pending) { this.pending.delete(id); pending.resolve(value); } }
  reject(id, error) { const pending = this.pending.get(id); if (pending) { this.pending.delete(id); pending.reject(error); } }
  disconnect(socket, error) { if (socket !== this.socket) return; this.socket = null; this.ready = false; this.rejectPending(error); if (!this.closed) this.scheduleReconnect(error); }
  scheduleReconnect(error) {
    if (this.closed || !this.reconnect) return this.transition("disconnected", error);
    const cap = Math.min(this.maxBackoff, this.minBackoff * (2 ** Math.min(this.attempt, 20)));
    const delay = Math.max(this.minBackoff, Math.floor(cap * (0.5 + Math.random() * 0.5)));
    this.attempt += 1; this.transition("backoff", error);
    this.timer = setTimeout(() => { this.timer = null; this.connect(); }, delay);
  }
  fail(error) { const socket = this.socket; if (socket && socket.readyState < 2) socket.close(1002, String(error.code).slice(0, 123)); this.disconnect(socket, error); }
  rejectPending(error) { for (const pending of this.pending.values()) pending.reject(error); this.pending.clear(); }
  subscribe(callback) {
    if (typeof callback !== "function") throw new TypeError("subscriber must be callable");
    this.listeners.add(callback); let active = true;
    const close = () => { if (active) { active = false; this.listeners.delete(callback); } };
    return Object.freeze({ close, cancel: close });
  }
  notify(change) { for (const callback of [...this.listeners]) { try { callback(change); } catch (_) {} } }
  transition(state, error = undefined) { if (this.onState) this.onState({ state, error, revision: this.revision }); }
}

function makeProxy(client, path, derived) {
  const api = {
    get path() { return Object.freeze([...path]); },
    get exists() { return resolve(derived ? client.derivedRoot : client.root, path, false) !== null; },
    get hasValue() { const record = resolve(derived ? client.derivedRoot : client.root, path, false); return record !== null && record.value !== MISSING; },
    get value() { const record = resolve(derived ? client.derivedRoot : client.root, path, false); return record === null || record.value === MISSING ? undefined : record.value; },
    set(value) { return derived ? Promise.reject(new XOProtocolError("xo.auth.invalid", "derived projections are read-only")) : client.write("set", path, value); },
    delete() { if (derived) return Promise.reject(new XOProtocolError("xo.auth.invalid", "derived projections are read-only")); if (path.length === 0) return Promise.reject(new XOProtocolError("xo.path.invalid", "root cannot be deleted")); return client.write("delete", path); },
    at(extra) { return makeProxy(client, [...path, ...normalizePath(extra)], derived); },
    subscribe(callback) { return client.subscribe(callback); },
    toJSON() { return toObject(resolve(derived ? client.derivedRoot : client.root, path, false)); },
  };
  return new Proxy(function xoNode() {}, {
    get(_target, property) {
      if (property === INTERNAL) return client;
      if (property === "then") return undefined;
      if (property === "derived" && path.length === 0 && !derived) return client.derivedProxy;
      if (property in api) { const value = api[property]; return typeof value === "function" ? value.bind(api) : value; }
      if (typeof property === "symbol") return undefined;
      return makeProxy(client, [...path, property], derived);
    },
    set(_target, property, value) { if (derived) throw new XOProtocolError("xo.auth.invalid", "derived projections are read-only"); if (typeof property !== "string") throw new TypeError("XO keys must be strings"); client.write("set", [...path, property], value).catch((error) => client.notify({ kind: "error", error })); return true; },
    deleteProperty(_target, property) { if (derived) throw new XOProtocolError("xo.auth.invalid", "derived projections are read-only"); if (typeof property !== "string") throw new TypeError("XO keys must be strings"); client.write("delete", [...path, property]).catch((error) => client.notify({ kind: "error", error })); return true; },
    apply(_target, _this, args) { if (args.length === 0) return api.value; if (args.length === 1) return api.set(args[0]); throw new TypeError("XO node accepts zero arguments to read or one to write"); },
  });
}

export function createXO(options) { const client = new Client(options); client.connect(); return client.proxy; }
export function closeXO(node) { if (!node || !node[INTERNAL]) throw new TypeError("value is not an XO proxy"); node[INTERNAL].close(); }

function decodeEnvelope(text, namespace) {
  let value; try { value = JSON.parse(text); } catch (error) { throw malformed(`invalid JSON: ${error.message}`); }
  const envelope = object(value, "envelope");
  for (const field of ["k", "mid", "ns", "p"]) if (!Object.hasOwn(envelope, field)) throw malformed(`envelope missing ${field}`);
  if (typeof envelope.k !== "string" || typeof envelope.ns !== "string") throw malformed("invalid envelope fields");
  uint(envelope.mid, "mid"); if (envelope.rid !== undefined) uint(envelope.rid, "rid");
  if (envelope.ns !== namespace) throw namespaceError("envelope namespace mismatch");
  return envelope;
}

function decodeEvent(value, namespace) {
  const event = object(value, "event");
  for (const field of ["event_id", "namespace", "origin_id", "base_revision", "revision", "operation", "path", "payload"]) if (!Object.hasOwn(event, field)) throw malformed(`event missing ${field}`);
  if (event.namespace !== namespace) throw namespaceError("event namespace mismatch");
  const operation = String(event.operation);
  if (!["set_value", "clear_value", "delete_subtree", "restore_subtree"].includes(operation)) throw malformed("unsupported event operation");
  const payload = object(event.payload, "event payload"); let data = MISSING;
  if (operation === "set_value") { if (Object.keys(payload).length !== 1 || !Object.hasOwn(payload, "new")) throw malformed("invalid set payload"); data = decodeValue(payload.new); }
  else if (operation === "restore_subtree") { if (Object.keys(payload).length !== 1 || !Object.hasOwn(payload, "node")) throw malformed("invalid restore payload"); data = decodeImage(payload.node); }
  else if (Object.keys(payload).length !== 0) throw malformed("operation payload must be empty");
  return { eventId: hex(event.event_id, "event_id"), originId: hex(event.origin_id, "origin_id"), baseRevision: uint(event.base_revision, "base_revision"), revision: uint(event.revision, "revision"), operation, path: wirePath(event.path), data };
}

function applyEvent(root, event) {
  if (event.operation === "delete_subtree") { if (event.path.length === 0) throw malformed("root cannot be deleted"); const parent = resolve(root, event.path.slice(0, -1), false); if (!parent || !parent.children.delete(event.path.at(-1))) throw resync("delete target is absent"); return; }
  if (event.operation === "restore_subtree") { if (event.path.length === 0) { root.value = event.data.value; root.children = event.data.children; } else resolve(root, event.path.slice(0, -1), true).children.set(event.path.at(-1), event.data); return; }
  resolve(root, event.path, true).value = event.operation === "clear_value" ? MISSING : event.data;
}

function decodeImage(value) {
  const image = object(value, "snapshot node"); if (!Array.isArray(image.$children)) throw malformed("snapshot children must be a list");
  const record = new Record(); if (Object.hasOwn(image, "$value")) record.value = decodeValue(image.$value);
  for (const item of image.$children) { if (!Array.isArray(item) || item.length !== 2 || typeof item[0] !== "string" || !item[0]) throw malformed("invalid snapshot child"); if (record.children.has(item[0])) throw malformed("duplicate snapshot child"); record.children.set(item[0], decodeImage(item[1])); }
  return record;
}

function resolve(root, path, create) { let record = root; for (const segment of path) { let child = record.children.get(segment); if (!child) { if (!create) return null; child = new Record(); record.children.set(segment, child); } record = child; } return record; }
function toObject(record) { if (!record) return undefined; const result = Object.create(null); for (const [key, child] of record.children) result[key] = toObject(child); if (record.value !== MISSING) result.$value = record.value; return result; }

function encodeValue(value, seen = new Set()) {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") { if (!Number.isFinite(value)) throw new TypeError("XO numbers must be finite"); if (Number.isInteger(value) && !Number.isSafeInteger(value)) throw new TypeError("XO integers must be safe JavaScript integers"); return value; }
  if (typeof value !== "object") throw new TypeError(`unsupported XO value type: ${typeof value}`);
  if (seen.has(value)) throw new TypeError("cyclic XO values are not supported"); seen.add(value);
  try { if (value instanceof Uint8Array) return { $xo: "bytes", value: bytesToBase64(value) }; if (Array.isArray(value)) return value.map((item) => encodeValue(item, seen)); const prototype = Object.getPrototypeOf(value); if (prototype !== Object.prototype && prototype !== null) throw new TypeError("XO values must be plain objects"); const result = Object.create(null); for (const [key, item] of Object.entries(value)) result[key] = encodeValue(item, seen); return result; } finally { seen.delete(value); }
}

function decodeValue(value) {
  if (Array.isArray(value)) return value.map(decodeValue); if (!value || typeof value !== "object") return value;
  if (Object.keys(value).length === 2 && value.$xo === "bytes" && typeof value.value === "string") return base64ToBytes(value.value);
  if (Object.keys(value).length === 2 && value.$xo === "tuple" && Array.isArray(value.value)) return Object.freeze(value.value.map(decodeValue));
  if (Object.hasOwn(value, "$xo")) throw malformed(`unknown XO codec tag: ${String(value.$xo)}`);
  const result = Object.create(null); for (const [key, item] of Object.entries(value)) result[key] = decodeValue(item); return result;
}

function normalizePrefixes(value) { if (!Array.isArray(value) || value.length === 0) throw new TypeError("prefixes must be a non-empty list"); return value.map(normalizePath); }
function normalizePath(value) { if (typeof value === "string") value = value === "" ? [] : value.split("."); if (!Array.isArray(value) || !value.every((part) => typeof part === "string" && part.length > 0 && !part.includes("\0"))) throw new TypeError("path must be a list of non-empty strings"); if (value.length > 64) throw new TypeError("path exceeds 64 segments"); return [...value]; }
function wirePath(value) { try { return normalizePath(value); } catch (error) { throw new XOProtocolError("xo.path.invalid", error.message); } }
function object(value, name) { if (!value || typeof value !== "object" || Array.isArray(value)) throw malformed(`${name} must be an object`); return value; }
function uint(value, name) { if (!Number.isSafeInteger(value) || value < 0) throw malformed(`${name} must be a non-negative safe integer`); return value; }
function positive(value, name) { if (!Number.isSafeInteger(value) || value <= 0) throw new TypeError(`${name} must be a positive integer`); return value; }
function hex(value, name) { if (typeof value !== "string" || !/^[0-9a-f]+$/i.test(value)) throw malformed(`${name} must be hexadecimal`); return value.toLowerCase(); }
function randomHex(length) { if (!globalThis.crypto || typeof globalThis.crypto.getRandomValues !== "function") throw new Error("secure browser randomness is required"); const data = new Uint8Array(length); globalThis.crypto.getRandomValues(data); return [...data].map((byte) => byte.toString(16).padStart(2, "0")).join(""); }
function bytesToBase64(bytes) { let binary = ""; for (const byte of bytes) binary += String.fromCharCode(byte); return btoa(binary); }
function base64ToBytes(value) { let binary; try { binary = atob(value); } catch (_) { throw malformed("invalid base64 bytes"); } return Uint8Array.from(binary, (character) => character.charCodeAt(0)); }
function malformed(message) { return new XOProtocolError("xo.protocol.malformed", message); }
function namespaceError(message) { return new XOProtocolError("xo.protocol.namespace_mismatch", message); }
function resync(message) { return new XOProtocolError("xo.resync_required", message); }
function connectionLost(message) { return new XOProtocolError("xo.connection_lost", message); }
