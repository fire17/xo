# XO network protocols

Lane owner: `XONetworkArchitect`. This file is the only file this lane writes.

Scope: one protocol family covering Redis replication, TCP/Unix RPC, server function exposure,
bounded generator streaming, browser WebSocket sync, reconnect/catch-up, origin/event dedupe,
framing/versioning/errors/deadlines/cancellation/auth, **simultaneous multi-adapter composition
on one XO root**, the transport boundary for lazy formula nodes, and a dependency-free JS client.

Out of scope, owned elsewhere: XO node/path/value semantics, revision generation, history,
formula evaluation semantics (`XOCoreArchitect`); the failure containment matrix
(`XOFailureArchitect`). This lane consumes their contracts and specifies only what the wire and
the attachment boundary dictate.

---

## 0. Fixed cross-lane contracts (settled this wave, not proposals)

### 0.1 Core schema — `XOCoreArchitect`'s final form, adopted verbatim

```
Event       { event_id, namespace, origin_id, base_revision, revision, operation, path, payload }
Diagnostics { timestamp_ns?, trace_id?, metadata? }        # single, optional, only when enabled
Transaction { events: (Event, ...) }                       # transaction_id DERIVES from events[0].event_id
operation   in { set_value, clear_value, delete_subtree, restore_subtree }
Snapshot    { schema:"xo.snapshot", version:1, namespace, revision, head_revision, root }
node        = { "$value"?: <tagged>, "$children"?: [[key, node], ...] }
```

Inherited rules, restated because the wire must not violate them:

- All eight Event fields are **required**; the only optional companion is one `Diagnostics`.
- **No** `parent_revision`, per-event `schema`, tx index, tx count, base `timestamp`, or base
  `metadata`. `parent_revision` belongs to history's `Revision`, never to the wire.
- `transaction_id` is **derived from `events[0].event_id`**, so the wire does not carry a
  separate id and cannot carry a contradictory one.
- Path is a **tuple in core, a list on the wire**, including the empty list for the root value.
- **A complete transaction is the atomic wire unit.** A singleton commit is a bare Event with no
  wrapper allocated.
- **Prior values / subtree images are local history or opt-in `EventView`; omitted by default.**
- State identity and ordering **never** depend on a timestamp.
- A remote apply **dedupes on `event_id` and never republishes as a fresh event**.
- Checkout is not an event operation: history checkout commits one atomic Transaction of
  primitive state ops.
- **`CommitOutcomeUnknown` requires reconcile by revision plus event/transaction id** — §10.5.

### 0.2 Failure vocabulary — `XOFailureArchitect`

The error code table (§7) and the stream terminal law (§5.4) are protocol-owned; the containment
matrix quotes these exact strings and adds no wire vocabulary.

### 0.3 Formula transport — fixed by Main

Local formula code is **never serialized** — no callable, expression string, bytecode, source, import path, or AST on any transport. Semantic source events travel normally. An explicitly observed materialized result travels as a non-authoritative `DerivedEvent` projection carrying its path, value/status, formula generation, and `cause_revision`; it does **not** advance base revision, enter history, or become a second state authority. A remote consumer may display/cache the projection but cannot apply it as an authored source mutation. **Observation is local bridge policy**: adapters union their local subscriptions/call bookkeeping and schedule one post-commit recomputation; this is never negotiated as executable formula behavior and no remote peer needs formula code.

### 0.4 Composition — fixed by Main (human requirement)

Redis replication, the RPC server, and the JS bridge **attach simultaneously to one XO root
through explicit capabilities and explicit lifecycle, never by class inheritance**. They are
independent adapters over **one event pipeline**. §2A is the normative section; G21–G24 validate
coexistence, no-echo, and ownership.

---

## 1. Evidence

Every claim is a read of the named file at the named lines. `xo1` = branch `xo1` of
`/Users/magic/wholesomegarden/xo-benedict` (HEAD `101a190`, via `git branch -a`).
`embedded` = `/Users/magic/wholesomegarden/magicllight/magicllight/core/airouter/pipelines/xo_benedict`.

### E0. Capability-by-inheritance — the composition defect

The old design expressed every capability as a **subclass of the state object**, so a program
chose exactly one:

- `class xoRedis(xoBenedict)` — `xo1/xo.py:1592`
- `class FreshRedis(xoBenedict)` — `xo1/xo.py:2103`
- `class Fresh(xoBenedict)` — `xo1/xo.py:2092`
- `class xoMetric(xoBackend)` — `xo1/xo.py:2559`
- `class FreshZero(xoBenedict)` — `xo1/freshServer.py`
- `class FreshClient(xoBenedict)` — `xo1/freshClient.py`
- `class xoDecorator(xoBenedict)`, `class expose(xoDecorator)`, `class Host(xoDecorator)` — `xo1/xoDecorator.py`

Consequences observed in the consumers:

1. **Capabilities could not coexist on one root.** `xo1/JS.py` had to instantiate
   `FreshRedis` *and* run a Flask/SocketIO server as a **separate program**, bridging them by
   hooking one magic key (`redis.all @= all`). Redis-sync and browser-sync were never two adapters
   on one tree; they were two processes joined by a convention.
2. **The RPC server was a different object than the state tree.** `embedded/router_server.py`
   builds `FreshServer = FreshZero(inc=inc)` and then re-exports `public = FreshServer.public`;
   `embedded/local.py` builds `local_server = freshClient.FreshClient(_inc=inc)`. A process wanting
   *both* replicated state and exposed functions had two roots.
3. **Behavior was decided by MRO, not by configuration.** `class Fresh(xoBenedict)` at
   `xo1/xo.py:2092` overrides `__onchange__`; `xoMetric` overrides it again at `xo1/xo.py:2563`.
   Two capabilities both wanting the change hook could not both have it.
4. **Lifecycle rode on `__init__`.** `xo1/xo.py:1783-1842` (`xoRedis.__init__`, class at
   `xo1/xo.py:1592`): constructing the object connects to Redis, registers in the class-level
   `xoRedis._clients` dict (`:1834`, read back at `:1842`), and starts a subscriber thread
   (`:1839` `self._redisSubscribe(key=self._namespace+"*", handler=self._directBind)`). There was
   no `attach`/`detach` — only "construct the right subclass, or don't have the feature."

This is the defect the human requirement names. §2A replaces the hierarchy with adapters.

### E1. Three transports, three encodings, no shared envelope

| Transport | Old implementation | Encoding |
| --- | --- | --- |
| Process↔process RPC | `xo1/freshServer.py:41` `Server(reqPort)` (`zeroless` = ZeroMQ REQ/REP) | `dill` pickle both ways (`xo1/freshServer.py:57,74,82,93`) |
| Process↔process state | `xo1/xoServer.py:29-30` PUB + REP | pickle for requests, `str(value).encode()` for publishes (`xo1/xoServer.py:205`) |
| Python↔Redis | `xo1/xo.py:1618` `psubscribe`; `xo1/xo.py:2020-2023` `set` + publish | `pk.dumps(value)` (`xo1/xo.py:2020`) |
| Python↔browser | `xo1/JS.py` `socketio.emit('update_any', ...)` | JSON `{'_id': dotted, 'value': v}` |

One logical mutation had three incompatible representations, so nothing could be routed,
replayed, logged, or tested once.

### E2. Pickle was the universal codec — the largest defect

`xo1/freshServer.py:15` `import dill as pk`; `xo1/freshServer.py:57` `payload = pk.loads(payload)`.
Identical at `embedded/freshServer.py:18,86`. `xo1/xoServer.py:176,180` pickles the request and
unpickles the reply.

`pk.loads` on socket bytes is arbitrary code execution by any peer that can connect, and the
servers bind with no authentication. Pickle also defined the feature set by accident: because
`dill` serializes functions, a **function object arriving from the network** was installed into
the registry:

```python
# xo1/freshServer.py:93-95
reply(pk.dumps(f"ECHO! {target} was not found in index (...)".encode()))
if True and len(payload["args"])>0:#Change to Auth
    public[target] = payload["args"][0]
```

The literal `#Change to Auth` marks a remote code-install path guarded by `if True`.

### E3. Server startup killed whatever held the port

`xo1/freshServer.py:38` `killport.kill_ports(ports=[reqPort, pubPort])` + `time.sleep(.2)`;
`xo1/xoServer.py:27` the same for `1980/19801` + `time.sleep(1)`; retained at
`embedded/freshServer.py:55`. Ports are hardcoded class attributes
(`xo1/xoServer.py:22-23`; `xo1/freshServer.py:36`; `xo1/xoServer.py:79-80`). Starting a server
SIGKILLs an unrelated process, and port identity is the only service identity.

The embedded variant avoided collisions with an `_inc` offset added to the port numbers
(`embedded/freshClient.py`: `request_port += _inc`; human canon runs `FreshClient(_inc=111)`,
`router_server.py` `inc = 101`, `fusion_server.py` `inc = 111`). Port arithmetic as discovery.

### E4. REQ/REP lockstep turned streaming into polling

`zeroless` `Client.request()` is `zmq.REQ` — verified in the extracted sdist,
`zeroless-1.0.0/zeroless/zeroless.py:196` `sock = self.__sock(zmq.REQ)` (its `reply()` peer is
`zmq.REP` at `:210`; sdist extracted to `/tmp/zl` for this check). ZeroMQ REQ is
strict lockstep: one send, one recv, no second request in flight
(`xo1/xoServer.py:176-177` shows the shape). So a generator could not stream over the call, and
the embedded variant invented a server-side parking lot polled by the client:

```python
# embedded/freshServer.py:141-152
def exec_generator(res):
    now = str(time.time())              # generator id = a timestamp string
    generators[now] = deque()           # module-global, unbounded
    reply(pk.dumps(generator(now)))
    for chunk in res:
        if chunk is not None:
            generators[now].append(chunk)
```
```python
# embedded/freshServer.py:94-99
if target in generators:
    q = generators[target]
    while len(q) == 0:
        time.sleep(0.1)
        ''' TODO # quit after timeout'''
```
```python
# embedded/freshClient.py:47-55
if "generator" in str(type(res)):
    gid = res["gid"]
    def get_generator(*a, **kw):
        res = self._root._client.request(_id+"*"+str(gid), *a, **kw)
        while "done" not in str(type(res)):     # class done(): pass  (freshServer.py:33)
            yield res
```

Six defects in one mechanism, all measurable:

1. **100 ms floor per chunk** — `time.sleep(0.1)` in the reply loop. An exact baseline, not an
   estimate.
2. **Serialized server** — the sleep runs inside the single REP loop, so one slow stream blocks
   every other client. Head-of-line blocking by construction.
3. **Unbounded memory** — `generators[now]`, module-global, uncapped, never deleted.
4. **Timestamp identity** — `str(time.time())`: colliding within a clock tick, guessable.
5. **Type-name strings as protocol** — `"generator" in str(type(res))`,
   `"done" not in str(type(res))`, `"function" in str(type(res))`
   (`embedded/freshServer.py:133,137`). A user class named `done` changes control flow.
6. **Silent lossy condense** — `embedded/freshServer.py:104-112`, `condense_every = 2` merges
   queued chunks under load. The transport altered the payload.

### E5. Browser sync was a global broadcast keyed by a magic name

```python
# xo1/programB.py
redis.all = ['a.b.c', 12345]
redis.all = ['html', "<h2 ...>Dynamic HTML!</h2>"]
# redis.all = ['eval', "alert('Py <> JS');"]
```
```python
# xo1/JS.py
def all(*v, **kw):
    socketio.emit('update_any', {'_id': v[0][0], 'value': v[0][1]})
redis.all @= all
```
```javascript
// xo1/freshSvelt/src/App.svelte:27-32
socket.on('update_any', msg => {
  if (msg['_id'] === "eval") { console.log("EEEE"); eval(msg['value']); }
  else { xo[msg['_id']] = msg['value']; }
});
```

Observed: (a) `_id == "eval"` is RCE in every connected browser, and it was **live, not
hypothetical** — `xo1/programB.py:205` `redis.all = ['eval', run_home]` ships a JS program to
every browser (`:8-9` are commented `alert` probes, `:205` is not); (b) `socketio.emit` with no
`to=` is a **global broadcast** (`xo1/JS.py:160`) — the room-scoped call sits commented one line
above at `:159`, and `XOSERVER.md:107` UPNEXT confirms `get data from client, id each client`
was never done; (c) sync
was one-directional — `XOJS.md:16` states `# getting data from client coming soon`; (d) only the
subtree the author remembered to push was synced (`XOSERVER.md:110` UPNEXT: `create a class
xoJS(xoRedis) in JS.py that publishes every key in subspace, without xo['all']`); (e) the bridge
was a **separate program** from the Redis-synced tree (E0.1).
Client dependencies: `freshSvelt/package.json:22-23` lists `socket.io-client` (+ `sirv-cli`) as
runtime deps, requiring a matching `flask_socketio` server (`xo1/JS.py:7`).

### E6. Redis: pickled values, dotted keys, spin thread, no revision

```python
# xo1/xo.py:2020-2023
val = pk.dumps(value)
res2 = self._root._redis.set(self._id + "." + str(key), val)
if not skip_publish:
    self._safePublish(self._id + "." + str(key), value)
```

`xo1/xo.py:1618` `psubscribe`, `xo1/xo.py:1625` `run_in_thread(sleep_time=.00001, daemon=True)`
— a spin-ish thread per client, unjoinable. `xo1/xo.py:1626-1628` catches every exception and
prints `Failed: Redis is not connected`, after which the object silently degrades to local-only.

Loop suppression was a `skip_publish` kwarg (`xo1/xo.py:1860`, `:2022`) plus
`sender = hash(self._root._redis)` (`xo1/xo.py:1859`) — a **process-local object hash**, so it
cannot survive a reconnect or identify a peer, and it is meaningless the moment two adapters share
a root. Key recovery was manual and pickled: `xo1/xo.py:2106` `save_keys()` / `:2115`
`load_keys()`, both round-tripping `pk.dumps(final)` under the key `self._id + ".keys()"`.
No revision, no atomic snapshot, no catch-up: a process that missed a publish could not learn
that it had.

### E7. What worked and must survive

From the recovered human canon (`/tmp/xo-codex-human.json`, session `019c3cff`):

```
>>> import freshClient
>>> server = freshClient.c
>>> server.index()
['foo.hi', 'index', 'new_func']
>>> server.foo.hi()
('hi', (), {})
```

- **Path-shaped remote calls** — `xo1/freshClient.py:22` calls
  `self._root._client.request(_id, *a, **kw)` on a target built by attribute access.
- **Path-shaped exposure** — `public.foo.hi = hi` and `@freshServer.nice`
  (`xo1/freshServer.py`); `xo1/xoDecorator.py` shows `@host.a.b.c`.
- **Live introspection** — `public.index` returns `list(public.flatten().keys())`, reflecting
  registrations made after connect (canon shows `'a.value.b.c'` appearing later).
- **A real 5-process chain in production use** (canon): `local.py`, `router_server.py`,
  `fusion_server.py`, `front_runner.py`, plus a REPL, streaming LLM chunks via
  `for a in c.query("what time is it?", None): print(a)`.
- **Assignment-is-sync to the browser** (`programB.py` → `App.svelte`), including live HTML.

Every one of these affordances is preserved below; every defect in E0–E6 is removed.

---

## 2. Decisions

**D0. Capabilities are adapters attached to one root, never subclasses of it.** Normative in
§2A. Replaces E0's hierarchy; enables the human requirement that Redis + RPC + JS bridge run on
one tree at once.

**D1. One frame, one envelope, one event object — three transports.** Redis, TCP/Unix RPC, and
WebSocket carry **byte-identical envelopes**; only framing differs. A tx replicated through
Redis and one arriving over a Unix socket deserialize by the same code path — which is also what
makes multi-adapter coexistence (§2A) mechanical rather than case-by-case.
*Rejected:* per-transport payload shapes (E1) — triples the test matrix, forbids routing/replay.

**D2. Tagged JSON as the only codec. Pickle is deleted, not sandboxed.** stdlib `json` plus a
tagged form for non-JSON types: `{"$t":"bytes","v":"<base64>"}`, `tuple`, `set`, `datetime`,
`decimal`. Unregistered types are **refused at send time** (`xo.codec.unsupported_type`, naming
path and type). A value never silently becomes a repr; no peer can cause code execution.
*Rejected:* allowlisted unpickler — still parses attacker-controlled opcodes, and the allowlist
would need the callables E2 relied on, which is the hole.
*Rejected:* msgpack/protobuf — a dependency; the JS client must be dependency-free.
**Accepted consequence:** callables are unsendable. Functions are *exposed by name* and *called
by path* (§4); definitions never cross the wire.

**D3. Length-prefixed, bounded frames; length checked before allocation.**
```
frame := MAGIC(2)="XO" VER(1)=1 FLAGS(1) LEN(4, uint32 BE) BODY(LEN)
FLAGS bit0 = body deflate-compressed
```
`LEN > max_frame_bytes` (8 MiB default) closes the connection with
`xo.protocol.frame_too_large` **before any buffer is allocated**.
*Rejected:* newline-delimited JSON — unbounded scan of untrusted input, no binary bodies.

**D4. One frame carries one complete unit: a bare Event or a complete Transaction.** A singleton
commit sends the bare Event and **allocates no wrapper**. A multi-op commit sends
`Transaction{events}`; `transaction_id` is **derived from `events[0].event_id`** and is not a
separate wire field. A replica can never observe half a transaction. An oversize Transaction is
refused **at author time** with `xo.tx_too_large`, never split.
*Rejected:* per-event frames + commit marker — creates a torn-state window needing per-transport
recovery tests.

**D5. Prior values are omitted by default; payload is op-discriminated.** On the wire:
`set_value` → `{new}`; `clear_value` → `{}`; `delete_subtree` → `{}`; `restore_subtree` →
`{node}` because the image *is* the operation. Prior values and subtree images are **local
history or opt-in `EventView`** (core's term), requested per subscription (`restorable: true`).
An applier never needs them; sending them doubles payload and leaks data to a peer that never
held it.

**D6. Diagnostics are one separate optional object, never per-event fields.** `Diagnostics`
(`timestamp_ns?`, `trace_id?`, `metadata?`) is encoded **only when the session enabled
diagnostics**, so the hot path allocates and encodes nothing. Time lives **only** here:
`Diagnostics.timestamp_ns` is the single home for wall-clock, it is **diagnostic only** — never
ordering, never dedupe — and the envelope carries no `ts` field at all (§3.2).

**D7. Version negotiated once per session, never per message.** `hello`/`welcome` exchange
`protocol` and `schema` with explicit `min_protocol`; mismatch fails the handshake with
`xo.protocol.version` and never degrades silently. Snapshots keep the explicit `schema`/`version`
because a snapshot is also an at-rest artifact.

**D8. Service identity is an address, never a port to be seized. Port killing is deleted.**
Default is a **Unix domain socket** at `$XDG_RUNTIME_DIR/xo/<namespace>.sock` (macOS:
`~/.xo/run/<namespace>.sock`): the observed 5-process chain is loopback-only, and Unix sockets
give filesystem permissions, no port collision, and lower latency. TCP is explicit opt-in
(`xo.serve(tcp=("127.0.0.1", 0))`) and **v1 refuses a non-loopback host outright** (§9, §15.1),
with **port 0 + an advertised address file** by default — so `_inc` arithmetic (E3) disappears. A
busy address raises `AddressInUse` naming the holder's pid where obtainable; it never kills it.

**D9. Multiplexed, non-blocking RPC. REQ/REP lockstep is deleted.** Every request carries a
monotonic `mid`; responses echo it as `rid`. Many requests and streams are in flight on one
connection, interleaved. This removes E4's head-of-line blocking *and* its polling: chunks are
**pushed**.
*Rejected:* keeping ZeroMQ — a C dependency, and `zeroless` pinned REQ/REP.

**D10. Streams are pushed with credit-based backpressure; the parking lot is deleted.** No global
dict, no timestamp ids, no `time.sleep` poll. The consumer grants credit (`start.credit`, default
64) and tops up (`credit` envelopes); the server pulls the generator **only while credit
remains**, so exhaustion applies backpressure to the producer instead of growing a deque. A source
that cannot be paused hits `max_stream_queue` (256) and raises `xo.backpressure`. Chunks are never
merged or altered — E4's silent condense is deleted.

**D11. Streaming is declared, not detected by stringifying types.** The registry records
`streaming: true` at registration time (`inspect.isgeneratorfunction`/`isasyncgenfunction`, or
explicit). Responses are `chunk` frames terminated by exactly one `end`. The
`"generator" in str(type(res))` test and the `class done: pass` sentinel are deleted.

**D12. Redis is a replication *log plus snapshot*, not a value store keyed by dotted names.**
Per namespace: `xo:{ns}:head` (int), `xo:{ns}:log` (**Redis Stream**, one entry per commit),
`xo:{ns}:snap` (latest `Snapshot`), Pub/Sub `xo:{ns}:tx` (wake-up). Commit is one atomic Lua
script that also performs the **expected-revision check**: `head != base_revision` returns a
conflict and the writer gets `xo.conflict` — explicit, never last-writer-wins.
**Redis Streams is the load-bearing choice**: it is the only Redis primitive giving a consumer a
resumable id, which is what E6 lacked, what makes catch-up possible, and what makes
`CommitOutcomeUnknown` reconcilable (§10.5).
*Rejected:* pickled `SET` per key + bare publish (E6) — no ordering, no gap detection, no atomic
snapshot, no conflict signal, no reconcile.
*Rejected:* keyspace notifications — they describe key changes, not XO commits, and cannot carry
`event_id`/`origin_id`.

**D13. Browser sync is the same protocol over a WebSocket, prefix-scoped and bidirectional.
`eval` and the magic `all` key are deleted.** The browser subscribes to path prefixes, receives
the same envelopes, applies them to a local mirror, and may send its own events back (closing
`XOJS.md`'s `getting data from client coming soon`). There is **no `eval` message kind** — code
cannot be a message. HTML remains possible purely as *data the application chooses to render*, so
`programB.py`'s live-HTML demo still works; that is the app's decision, not the transport's.
*Rejected:* Socket.IO (E5) — a dependency on both ends and non-standard framing.
*Rejected:* global broadcast (E5's actual behavior).
*Rejected:* the bridge as a separate program hooked to one magic key (E0.1, E5) — it is now an
adapter on the same root (§2A).

**D14. Deadlines are absolute and propagated; there are no hidden retries.** A request may carry
`dl` (absolute unix seconds); a server receiving an expired request refuses with
`xo.deadline_exceeded` **without invoking the function**. The transport **never retries a
request** — a call that failed mid-flight has unknown side effects. *Connections* reconnect
(bounded exponential backoff with jitter, observable via `on_state`); *requests* do not.

**D15. Auth is a handshake token with transport-appropriate defaults.** Unix socket: peer
credentials + `0600` file, no token needed. TCP/WebSocket: a 32-byte shared secret **required**,
compared with `hmac.compare_digest`, sent once in `hello`, never logged. No token + non-loopback
bind = refuse to start (fail closed). `role: "observer"` is first-class; read, write, exposure,
and materialization are separate capabilities.
*Rejected:* `#Change to Auth` + `if True` (E2's actual behavior).

**D16. Dedupe by `event_id` in a bounded ring, with `origin_id` as a fast pre-filter, and
`origin_id` is per-root not per-adapter.** A bounded LRU of seen `event_id`s (65 536) per
namespace, **owned by the pipeline and shared by every adapter** (§2A.4); inbound duplicates are
dropped. An event whose `origin_id` is ours is dropped without a set lookup. This is the correct
replacement for E6's `hash(self._root._redis)` sender — an adapter-scoped identity that becomes
outright wrong once three adapters share a root. Applying a remote event **never republishes**, so
loops break structurally; dedupe is defense-in-depth for diamond topologies (a peer connected both
directly and through Redis, or a browser also served by a Redis-replicated sibling).

**D17. No import side effects; no module-global singletons; lifecycle is explicit.** Importing
`xo` starts nothing: no thread, no socket, no Redis connection, no port kill. Contrast:
`xo1/freshServer.py` constructs `FreshZero()` at import (killing ports and binding);
`xo1/freshClient.py:27` ends with `c = FreshClient()`, connecting on import; `xo1/xoDecorator.py`
instantiates and registers at import; `xo1/xo.py:1783-1842` connects and starts a thread inside
`__init__`. Every adapter has `attach`/`detach`, is a context manager, and `close()` joins its
threads. No daemon threads (E6 used `daemon=True`, so shutdown was unobservable).

### What must NOT be unified

Deliberate boundaries; collapsing any is a regression.

1. **Local in-process events vs. wire events.** The local path stays a direct call with **zero
   encoding**. Encoding happens at the adapter edge, once, only if that adapter has a subscriber.
   *(Most important non-unification; gates G1, G21.)*
2. **The pipeline vs. its adapters.** One fan-out point owns ordering, dedupe, and `origin_id`;
   adapters own transport. Merging them recreates E0 (behavior decided by whichever capability
   subclassed last).
3. **Replication vs. RPC.** Replication is one-way, fan-out, idempotent, ordered by revision. RPC
   is request-scoped with a waiting caller, deadlines, cancellation. They share frame, envelope,
   codec — nothing else. A call is not an event and never enters the log.
4. **Redis Pub/Sub vs. the Stream.** Pub/Sub is the low-latency wake-up and is *allowed to lose
   messages*. The Stream is durable resumable ordering. Correctness rests only on the Stream.
5. **Snapshot vs. event.** A snapshot is a whole-tree artifact with its own schema; an event is a
   delta. Expressing catch-up as "a huge event" would break the frame bound and tx semantics.
6. **Browser trust vs. peer trust.** A browser is semi-trusted: prefix-scoped, no exposure rights,
   no history rewrite. A same-uid Unix peer is trusted. One model for both would cripple local IPC
   or over-trust the web.
7. **Transport errors vs. application exceptions.** `xo.protocol.*` kills the connection;
   `xo.internal` is a normal response to one request.
8. **Formula definition vs. formula result.** §6. Source events and optional derived projections
   travel; **code never does**, and there is no second formula protocol.
9. **Authored state vs. derived projection.** Fixed by Main (§15.1). An Event is authored,
   revision-bearing, logged, replayable, and conflict-checked; a `DerivedEvent` is none of those.
   They share the connection and the codec and nothing else. Unifying them — letting a
   materialized value ride as an ordinary `set_value` — would make every applier a formula
   authority, put derived values in the replication log where replay would resurrect them as
   authored history, and let a stale recompute win an expected-revision check. *(Gates G14–G16,
   G26.)*

---

## 2A. Composition: one root, many adapters (normative)

The human requirement, made mechanical. Redis, RPC, and the JS bridge are **peers**, attach in
any order, at any time, and detach independently.

### 2A.1 Shape

```
        ┌──────────────────────────── one XO root (namespace "app") ───────────────────────────┐
        │  tree + revision counter + root lock + origin_id                                     │
        └───────────────────────────────────┬──────────────────────────────────────────────────┘
                                            │ commit(Transaction | Event)          ONE pipeline:
                                            v                                      • ordering
                              ┌──────────────────────────────┐                     • dedupe ring
                              │        event pipeline        │                     • origin filter
                              │  (owns order, dedupe, origin)│                     • encode-once
                              └───┬──────────┬──────────┬────┘
                    attach()      │          │          │      attach()
                                  v          v          v
                        ┌───────────────┐ ┌──────────┐ ┌────────────────┐
                        │ RedisAdapter  │ │RpcAdapter│ │ BridgeAdapter  │   ... more, same contract
                        │ replicate     │ │ serve    │ │ WebSocket/JS   │
                        └───────────────┘ └──────────┘ └────────────────┘
```

No adapter subclasses the root. No adapter subclasses another. The root does not import any
adapter — adapters depend on the root, never the reverse, so `import xo` can start nothing (D17).

### 2A.2 Explicit capabilities

Each adapter is attached with a capability set; the pipeline enforces it, not the adapter.

| Capability | Meaning | Redis | RPC | Bridge (browser) |
| --- | --- | --- | --- | --- |
| `replicate_out` | receives local commits for transport | yes | no | yes (prefix-scoped) |
| `replicate_in` | may apply remote commits to the root | yes | no | only under granted prefixes |
| `expose` | may serve `public` paths / `index()` | no | yes | never |
| `call_out` | may issue outbound calls | no | yes | no |
| `materialize` | may mark formula nodes observed (§6) | opt-in | yes | only if granted |
| `history_read` / `history_write` | revision/checkout access | read | read | read only |

A capability an adapter does not hold is **refused at the pipeline**, with the same named error a
remote peer would get (`xo.auth.invalid`, `xo.formula.not_materialized`, …). An adapter cannot
widen its own rights.

### 2A.3 Explicit lifecycle

```
xo.attach(adapter, capabilities=...) ->
    DETACHED --attach()--> ATTACHING --ready--> ACTIVE --detach()--> DRAINING --> DETACHED
                              │                                          │
                              └── failure ─> DETACHED (named error)      └── joins its threads
```

Rules:

1. **`attach` is explicit and idempotent per adapter instance**; a second `attach` of the same
   instance raises rather than silently double-subscribing.
2. **Attach order is irrelevant.** An adapter attaching at revision *R* onto a root at revision
   *R+n* catches up by its own transport rules (§5.2/§10.3) before reaching `ACTIVE`; it never
   forces the root to wait.
3. **`detach` is complete and local.** `DRAINING` finishes in-flight work, closes its generators
   through the terminal law (§5.4), joins its threads, and unregisters from the pipeline. The root
   and every other adapter keep running, unchanged.
4. **One adapter's failure never detaches another.** A Redis outage leaves RPC and the bridge
   serving; the failed adapter reports `xo.connection_lost` / enters `BACKOFF` and says so through
   `on_state`. (`XOFailureArchitect` owns the containment matrix; this is the attachment surface
   it acts on.)
5. **`root.close()` detaches every adapter in reverse attach order, then joins.** No daemon
   threads; shutdown is observable (contrast E6).

### 2A.4 One pipeline — ownership rules that make coexistence correct

1. **`origin_id` belongs to the root, not to an adapter.** One identity per process-root, stable
   across every adapter and every reconnect. This is precisely what E6's
   `hash(self._root._redis)` could not be.
2. **The dedupe ring is pipeline-owned and shared.** An event that arrived over Redis and then
   over the bridge is applied once. Per-adapter rings would let a diamond topology double-apply.
3. **Encode once, fan out.** A commit is encoded to a body at most once per *codec configuration*
   (diagnostics on/off, restorable on/off), then handed to every adapter that wants that
   configuration. Three attached adapters do not mean three encodes.
4. **No adapter re-enters the pipeline for an event it just applied.** `replicate_in` applies with
   the remote `event_id`/`origin_id` intact and the pipeline does not re-emit it outward as a fresh
   event (core's never-republish rule). The **echo-free property is structural**, not a per-adapter
   flag like `skip_publish` (E6).
5. **The root lock serializes commits; adapters never hold it during I/O.** An adapter formats and
   transmits outside the lock, so a slow socket cannot stall local mutation or another adapter.
6. **Per-adapter bounded queues.** Each `replicate_out` adapter has its own
   `max_subscriber_queue` (4096). A slow browser fills its own queue and gets `xo.backpressure`;
   Redis replication is unaffected. Backpressure is per-adapter by construction.

### 2A.5 The composition the human asked for, in code

```python
import xo

app = xo.XO(namespace="app")                     # root only: no I/O, no threads

redis  = xo.RedisAdapter(url="redis://127.0.0.1:6379")
rpc    = xo.RpcAdapter(unix="app.sock")
bridge = xo.BridgeAdapter(ws=("127.0.0.1", 7802), token=TOKEN,
                          prefixes=[["ui"]], role="observer")

@app.public.query                                 # exposure is a root-level registry,
def query(prompt):                                # not a property of the RPC adapter
    yield from run_model(prompt)

with app.attach(redis,  caps={"replicate_out","replicate_in","history_read"}), \
     app.attach(rpc,    caps={"expose","call_out","materialize","history_read"}), \
     app.attach(bridge, caps={"replicate_out","replicate_in:ui","materialize:ui"}):

    app.ui.chat.count.value = 4      # ONE mutation ->
                                     #   Redis: atomic Lua commit, revision-checked
                                     #   Bridge: envelope to prefix-["ui"] subscribers
                                     #   RPC: nothing (holds no replicate_out)
    app.wait()
# detach in reverse order; every thread joined; root still usable
```

Three capabilities, one tree, one mutation, three correct behaviors — the thing E0/E5 could not
express. `public` lives on the **root**, so exposure survives detaching the RPC adapter and can be
served by a second RPC adapter (e.g. Unix + TCP simultaneously) without duplicating the registry.

Validated by G21–G24 (§13).

---

## 3. Wire format

### 3.1 Frame

```
 0        2        3        4                     8
 +--------+--------+--------+---------------------+---------------- ...
 | "XO"   |  VER   | FLAGS  |   LEN (uint32 BE)   |  BODY (LEN bytes)
 +--------+--------+--------+---------------------+---------------- ...
```

`FLAGS` bit0 = `zlib`-compressed body, set only above `compress_over` (4096 B). Over WebSocket
the header is omitted (the WebSocket frame *is* the length-prefixed frame); over Redis it is
omitted (the Stream field value is the body). **The body is byte-identical on all three**, which
is what lets one encode serve three adapters (§2A.4.3).

### 3.2 Envelope

| Field | Type | Meaning |
| --- | --- | --- |
| `k` | str | kind: `hello`,`welcome`,`sub`,`unsub`,`snapshot`,`event`,`tx`,`derived`,`ack`,`call`,`result`,`start`,`chunk`,`credit`,`end`,`cancel`,`ping`,`pong`,`error` |
| `mid` | int | message id; per-connection monotonic, never reused |
| `rid` | int | correlation: the `mid` this responds to |
| `ns` | str | namespace — **one per root, one per session** (§15.1); a mismatch is `xo.protocol.namespace_mismatch` and closes the connection |
| `dl` | float | absolute deadline, unix seconds (requests only) |
| `tr` | str | opaque trace id, propagated unchanged |
| `p` | obj | kind-specific payload |

Canonical implementation: `src/xo/wire.py` - `Envelope`, `encode_envelope`, `decode_envelope`,
`commit_envelope`, `derived_envelope`, `item_from_envelope`. The field names above are
`Envelope.as_mapping()` verbatim: `k`/`mid`/`ns`/`p` always emitted, `rid`/`dl`/`tr` only when
set, so an unused correlation or deadline costs zero bytes. **Transport lanes import this module
and add only framing (§3.1); no lane re-serializes an event.**

There is deliberately **no `ts` field**. An earlier draft of this report carried one as
"diagnostic only"; the canonical module instead puts `timestamp_ns` inside `Diagnostics`
(`events.py:17-20`), attached to the event that has a reason to be timed. This report defers to
that shape - one home for time, and no per-envelope clock read on a path that already forbids
ordering by wall time.

The codec emits object keys **sorted** (`json.dumps(..., sort_keys=True, separators=(",",":")`,
`codec.py:49-55`), so encoding is deterministic and byte-comparable - which is what makes the
byte-identity thresholds in G1/G26 checkable at all. The examples below are written in logical
field order for reading; the bytes on the wire are alphabetical.

`k`, `mid`, `ns` always present. Ordering is `revision` for state and `rid` for requests -
**never** a timestamp. `k="event"` carries one bare Event; `k="tx"` carries one complete
Transaction (`EventGroup` in code; `Transaction` is an alias of it, `events.py:58`, so the two
names in this report denote one type); `k="derived"` carries one non-authoritative projection and
is **not** state (§6 F6).

`ns` is on every envelope even though a session carries exactly one namespace: it costs a few
bytes and turns a cross-namespace misconfiguration — the wrong socket path, a stale address file,
a peer that reconnected to a different root — into an immediate named close instead of silent
state corruption. **One namespace per root is fixed** (§15.1): the field validates, it does not
multiplex.

### 3.3 Wire examples

Handshake:

```json
{"k":"hello","mid":1,"ns":"app","p":{"protocol":1,"min_protocol":1,"schema":1,
  "origin_id":"7f3a9c21e0b84d55","client":"xo-py/1.0","role":"peer",
  "token":"<32-byte hex>","restorable":false,"diagnostics":false}}
```
```json
{"k":"welcome","mid":1,"rid":1,"ns":"app","p":{"protocol":1,"schema":1,
  "origin_id":"c41d8ff2a9b34e17","head_revision":8123,
  "limits":{"max_frame_bytes":8388608,"max_inflight":256,"max_stream_queue":256,
            "default_credit":64,"max_path_segments":64}}}
```

`origin_id` is the **root's** identity, identical no matter which adapter opened the connection
(§2A.4.1).

Subscribe, then the snapshot that anchors catch-up:

```json
{"k":"sub","mid":2,"ns":"app","p":{"prefixes":[["ui","chat"],["metrics"]],
  "since_revision":0,"restorable":false,"materialize":[]}}
```
```json
{"k":"snapshot","mid":9,"rid":2,"ns":"app","p":{"schema":"xo.snapshot","version":1,
  "namespace":"app","revision":8123,"head_revision":8123,
  "root":{"$children":[["ui",{"$children":[["chat",{"$value":"ready",
          "$children":[["count",{"$value":3}]]}]]}]]}}}
```

The snapshot node carries `$value` **and** `$children` on the same node — the value-plus-children
invariant is expressible on the wire, which E5's `{'_id','value'}` form could not do.

Singleton commit — one bare Event, **no wrapper allocated**, all eight fields present:

```json
{"k":"event","mid":10,"ns":"app","p":{
  "event_id":"3b1f9c02","namespace":"app","origin_id":"c41d8ff2a9b34e17",
  "base_revision":8123,"revision":8124,
  "operation":"set_value","path":["ui","chat","count"],"payload":{"new":4}}}
```

Multi-op commit - one complete Transaction in one frame. Every event in it **shares commit
identity**: same `namespace`, `origin_id`, `base_revision`, and `revision`, differing only in
`event_id`, `operation`, `path`, and `payload`. That is a construction-time invariant, not a
convention (`EventGroup.__post_init__`, `events.py:40-51`, raises on a mismatched member), and it
follows from §0.1: one commit advances the revision exactly once, so a transaction cannot contain
a revision staircase. There is **no `transaction_id` field** - it derives from
`events[0].event_id` = `"77c4"` - and no index, no count:

```json
{"k":"tx","mid":11,"ns":"app","p":{"events":[
    {"event_id":"77c4","namespace":"app","origin_id":"c41d8ff2a9b34e17",
     "base_revision":8124,"revision":8125,
     "operation":"set_value","path":["ui","chat","last"],"payload":{"new":"hi"}},
    {"event_id":"e19b","namespace":"app","origin_id":"c41d8ff2a9b34e17",
     "base_revision":8124,"revision":8125,
     "operation":"clear_value","path":["ui","chat","draft"],"payload":{}}]}}
```

`clear_value` carries `{}` — prior values stayed local (D5). `restore_subtree` carries `{node}`
because the image *is* the operation; `delete_subtree` carries `{}` unless the subscription opted
into `restorable` (`EventView`).

RPC call and result — `server.foo.hi()` from E7, unchanged at the call site:

```json
{"k":"call","mid":12,"ns":"app","dl":1756543216.0,"p":{"path":["foo","hi"],"args":[],"kwargs":{}}}
{"k":"result","mid":31,"rid":12,"ns":"app","p":{"value":["hi",[],{}]}}
```

Bounded stream — `c.query("what time is it?", None)` from the canon, now pushed:

```json
{"k":"call","mid":13,"ns":"app","dl":1756543271.0,
 "p":{"path":["query"],"args":["what time is it?",null],"kwargs":{},"stream":true,"credit":64}}
{"k":"start","mid":40,"rid":13,"ns":"app","p":{"streaming":true}}
{"k":"chunk","mid":41,"rid":13,"ns":"app","p":{"seq":0,"value":"It "}}
{"k":"credit","mid":14,"rid":13,"ns":"app","p":{"credit":32}}
{"k":"chunk","mid":42,"rid":13,"ns":"app","p":{"seq":1,"value":"is 10:24."}}
{"k":"end","mid":43,"rid":13,"ns":"app","p":{"reason":"complete","count":2}}
```

Cancel and typed errors:

```json
{"k":"cancel","mid":15,"rid":13,"ns":"app","p":{"reason":"consumer stopped"}}
{"k":"end","mid":44,"rid":13,"ns":"app","p":{"reason":"cancelled","count":2}}
{"k":"error","mid":45,"rid":13,"ns":"app","p":{"code":"xo.backpressure",
  "message":"stream queue at bound (256)","retryable":true,"detail":{"limit":256}}}
{"k":"error","mid":46,"rid":21,"ns":"app","p":{"code":"xo.conflict",
  "message":"expected revision 8123, head is 8130","retryable":true,
  "detail":{"base_revision":8123,"head_revision":8130}}}
```

Derived projection — a materialized formula value, deliberately **not** an Event (§6 F2/F6). It
carries no `event_id`, no `base_revision`, and no `operation`, because it is not a commit and
cannot be applied as one:

```json
{"k":"derived","mid":12,"ns":"app","p":{
  "path":["cart","total"],"value":41.97,
  "generation":3,"cause_revision":8124,"origin_id":"c41d8ff2a9b34e17"}}
```

A formula that raised carries `status` instead of `value`, and the source commit still stands:

```json
{"k":"derived","mid":13,"ns":"app","p":{
  "path":["cart","total"],"status":"error",
  "generation":3,"cause_revision":8124,"origin_id":"c41d8ff2a9b34e17",
  "error":{"code":"xo.formula.error","message":"ZeroDivisionError: division by zero"}}}
```

The absent fields are the contract: no `revision` to advance, no `event_id` to log, nothing for
`XADD` to accept. Supersession is `(path, generation, cause_revision)` — §6 F6.

### 3.4 Path on the wire

Always `list[str]` — 1:1 with core's canonical segment tuple — **including the empty list for the
root value**. Bounded by `max_path_segments` (64) and `max_segment_bytes` (256) →
`xo.path.invalid`. This deletes the dotted-string ambiguity of E1/E5/E6: a key containing `.` is
now representable and unambiguous, which `self._id + "." + str(key)` (`xo1/xo.py:2021`) could not
do.

---

## 4. Exposure and the remote proxy

### 4.1 Exposure — `@public.api.path` preserved, root-owned

```python
import xo

app = xo.XO(namespace="app")
public = app.public                 # registry lives on the ROOT, not on an adapter (§2A.5)

@public.query                       # bind at ["query"]
def query(prompt, pipeline=None):
    yield from run_model(prompt)    # generator -> streaming, detected once at registration

@public.foo.hi                      # bind at ["foo","hi"]
def hi(*a, **kw):
    return ("hi", a, kw)

public.foo.hi = hi                  # assignment form, identical effect (E7 compatibility)

with app.attach(xo.RpcAdapter(unix="app.sock"), caps={"expose"}):
    app.wait()                      # explicit; no import side effect, no port kill
```

`public.index()` returns the live registry as **path arrays** —
`[["foo","hi"], ["index"], ["query"]]` — plus `{"streaming": bool, "doc": str, "params": [...]}`
per entry, preserving E7's runtime discovery including post-connect registrations. Because the
registry is root-owned, two RPC adapters (Unix + TCP) serve one identical index.

Exposure is **local-only**: no wire kind registers a callable. E2's
`public[target] = payload["args"][0]` is deleted with no replacement, deliberately.

### 4.2 Remote proxy — `remote.api.path(...)` preserved

```python
remote = xo.connect(unix="app.sock", namespace="app")   # or tcp=("127.0.0.1", 7801)

remote.foo.hi()                     # ('hi', (), {})
remote.index()                      # [["foo","hi"], ["index"], ["query"]]

for chunk in remote.query("what time is it?", None):    # pushed, bounded, cancellable
    print(chunk)

with remote.query("long prompt") as s:                  # early exit -> cancel + one terminal
    for chunk in s:
        if enough(chunk): break
```

Attribute access builds a path; `__call__` sends `call`. No `_inc` offsets (D8); `.` inside a
segment is safe (§3.4). `remote.path.__doc__` and `dir(remote.path)` are served from the fetched
index, so the proxy stays introspectable in a REPL — which is how the canon actually drove it.

### 4.3 Compatibility API

Surviving names: `public.<path>` assignment and decorator use, `public.index()`,
attribute-built remote paths, calling a remote path, iterating a streaming remote path.

`xo.compat` provides `FreshClient(...)`/`FreshZero(...)`/`FreshRedis(...)` shims that construct a
root and attach the corresponding adapter, accepting and **ignoring** `_inc`, `request_port`,
`publish_port` with a one-time `DeprecationWarning` naming the replacement. Deliberately **not**
reproduced, each raising a named error instead of pretending: pickled payloads
(`xo.protocol.malformed`), remote function install (`xo.auth.invalid`), `eval` messages (unknown
kind → `xo.protocol.malformed`), the magic `all` broadcast key (now an ordinary path with ordinary
subscription semantics), port killing (`AddressInUse`), capability-by-subclass (the shims compose
adapters instead; `class MyThing(FreshRedis)` keeps working for attribute access but gains no
transport by inheritance).

---

## 5. Lifecycle and state machines

### 5.1 Connection (per adapter, independent)

```
                 +--------------+
                 | DISCONNECTED |<--------------------------+
                 +------+-------+                           |
                        | connect()                         | close() / fatal
                        v                                   | (xo.protocol.*, xo.auth.*)
                 +--------------+   timeout / refused        |
                 |  CONNECTING  |--------------------+       |
                 +------+-------+                    |       |
                        | socket established         v       |
                        v                     +--------------+
                 +--------------+             |   BACKOFF    |
                 |  HANDSHAKING |             +------+-------+
                 +------+-------+                    ^ jitter retry
        version/auth fail |  welcome                  |
             +------------+------+                    |
             |                   v                    |
             |            +--------------+            |
             |            |  CATCHING_UP |            |
             |            +------+-------+            |
             |                   | at head            |
             |                   v                    |
             |            +--------------+  socket err|
             |            |    READY     |------------+
             |            +--------------+
             v
        (fatal, no retry)
```

`BACKOFF` is bounded exponential with jitter (100 ms → 30 s); every transition reaches
`on_state`. No hidden reconnect: a connection that gives up says so. In-flight requests fail with
`xo.connection_lost` and are **not** replayed (D14). This machine is **per adapter** — one
adapter in `BACKOFF` does not move any other adapter or the root out of service (§2A.3.4).

### 5.2 Subscription and catch-up

```
sub(prefixes, since_revision)
   |
   +-- since_revision == 0 -----------> snapshot @ R, then events R+1..
   +-- gap closable from log ---------> replay since_revision+1..head
   +-- log trimmed past the gap ------> error xo.resync_required, then snapshot @ head, then events
```

The client never chooses: it sends `since_revision`, the server picks replay or snapshot. A
trimmed log is a **named, observable** resync, not silent data loss — the condition E6 could not
even detect. A late-attaching adapter (§2A.3.2) uses exactly this path.

### 5.3 RPC

```
call(mid) -> [deadline check] -> [capability check] -> [path lookup] -> [invoke]
                  |                     |                   |             |
      xo.deadline_exceeded      xo.auth.invalid       xo.not_found   result | error(xo.internal)
```

`xo.internal` carries the exception **type name and message only** — never a traceback, never a
pickled exception.

### 5.4 Stream — the terminal law

```
call(stream=true, credit=C)
        |
        v
     start(rid) ---- chunk(seq=0..) ----+
        ^                               |
        |     credit(rid, +n) <---------+   (consumer tops up)
        |
        +-- producer exhausted -------> end(reason="complete")
        +-- cancel(rid) received -----> close generator, THEN end(reason="cancelled")
        +-- deadline passed ----------> close generator, THEN end(reason="deadline_exceeded")
        +-- queue at bound ----------> error(xo.backpressure), THEN end(reason="error")
        +-- producer raised ---------> error(xo.internal),     THEN end(reason="error")
        +-- adapter detached --------> close generator, THEN end(reason="cancelled")
```

Invariants (agreed with `XOFailureArchitect`, protocol-owned):

1. **Exactly one terminal `end` per `rid`.** Zero or two is a protocol violation, not a degraded
   mode.
2. **Cleanup precedes the terminal ack.** The generator is closed (`.close()`, so `finally`/`with`
   blocks run) *before* `end` is emitted.
3. **Late chunks are discarded by `rid`.** The consumer retires the `rid` on terminal receipt;
   chunks for retired/unknown `rid`s are dropped at the demux layer and counted in a metric —
   never an error, never delivered to application code.
4. **`mid`/`rid` are never reused** within a connection, so a late chunk cannot alias a new
   stream. (Contrast E4, whose id was `str(time.time())` and collided within a tick.)
5. **Connection loss or adapter detach retires every `rid`** owned by it and runs the same close
   path — no orphan producer survives the socket or the detach (§2A.3.3).

---

## 6. Formula / computed nodes — transport boundary

The rule is **fixed** (§0.3); this section states its consequences only. Formula *semantics*
belong to `XOCoreArchitect`. No second formula protocol exists.

**F1. Formula code never crosses the wire, in any form.** Not a callable, not an expression
string, not bytecode, not a serialized AST. This is D2 applied — the codec has no representation
for code — which is why E2's remote function install has no successor. A formula is *defined
locally, in the process that owns it.*

**F2. Only two things travel: semantic source events, and optionally a non-authoritative
projection of the materialized value.** Source mutations replicate as ordinary Events (§3.3). An
explicitly observed materialized result travels as a `DerivedEvent` — the `k="derived"` envelope
shown in §3.3 — a *projection*, not a commit: it carries `path`, `value` or `status`, the formula
`generation`, and the `cause_revision`
it was computed from. It does **not** advance base revision, does not enter history, and is not a
second state authority. A remote applier needs no formula engine; it may display or cache the
projection but **cannot** apply it as an authored source mutation.

**F3. Observation is local bridge policy; recompute is post-commit.** The bridge/RPC adapter
decides, from its own subscription and call bookkeeping, that a formula node is observed — a `sub`
naming the path in `materialize`, or a `call` that reads it — and schedules recompute **after** the
source commit is durable, never inside it. That decision is never negotiated on the wire.
Dropping the subscription, detaching the adapter, or disconnecting drops the observation, so a
formula nobody watches is not computed on anyone's behalf: laziness survives the network instead
of being defeated by it. With several adapters attached (§2A), observation is the **union** across
adapters and a node stops being observed only when the last observer goes away.

**F4. A formula failure never fails the source commit.** The source transaction stays committed;
the observing peer receives `error{code:"xo.formula.error"}` with type name + message only. A peer
that reads or subscribes to a formula node without materialization receives
`xo.formula.not_materialized` — an explicit refusal rather than a stale or `null` value.

**F5. Capability boundary.** `materialize` is a capability separate from read (§2A.2):
`role:"observer"` may subscribe to values but may request materialization only within its granted
prefixes, and a browser never gains it implicitly. Bounded: `max_materialized_nodes` per
subscription (default 256) → `xo.limit.concurrency`. Materialized values obey the codec rules, so
a formula returning an unencodable object yields `xo.codec.unsupported_type` naming the path —
never a silent repr.

**F6. The projection is deliberately not an Event, and that asymmetry is the point.** A
`DerivedEvent` is delivered on the same connection, framed identically, and dedupes identically,
but it is a distinct envelope kind so that no applier can mistake derived output for authored
state. Consequences that follow mechanically: it never appears in `xo:{ns}:log`, so replay and
catch-up (§10.2) reconstruct only authored state and a reconnecting peer re-derives projections
from its own re-declared observations rather than replaying stale ones; it never participates in
the expected-revision check (§10.4), so a projection can never cause `xo.conflict`; `revision`
gaps are computed over Events only, so a dropped projection never triggers `xo.resync_required`.
A projection is superseded by `(path, generation, cause_revision)`: a newer `cause_revision` for
the same path replaces an older one, and an out-of-order arrival is discarded, not applied.

---

## 7. Errors

`k="error"`, payload `{code, message, retryable, detail}`. Codes are stable strings; tracebacks
and object graphs never cross the wire.

| Code | Meaning | Connection |
| --- | --- | --- |
| `xo.protocol.version` | unsupported protocol/schema at handshake | closed |
| `xo.protocol.malformed` | undecodable frame/envelope, unknown kind, or missing required Event field | closed |
| `xo.protocol.frame_too_large` | declared `LEN` over bound | closed |
| `xo.protocol.namespace_mismatch` | envelope `ns` differs from the session's namespace (one per root, §15.1) | closed |
| `xo.tx_too_large` | authored Transaction cannot fit a frame (refused at author time) | kept |
| `xo.auth.required` / `xo.auth.invalid` | missing/bad token, or capability not held | closed / kept |
| `xo.path.invalid` | path too deep/long, or bad segment | kept |
| `xo.codec.unsupported_type` | no registered codec — refuses instead of pickling | kept |
| `xo.not_found` | call to an unexposed path | kept |
| `xo.deadline_exceeded` | absolute deadline passed | kept |
| `xo.cancelled` | peer cancelled (also an `end` reason) | kept |
| `xo.backpressure` | stream/subscriber queue at bound — **named refusal, not a drop** | kept |
| `xo.conflict` | expected-revision check failed | kept |
| `xo.commit_outcome_unknown` | commit sent, outcome unobserved; **reconcile by revision + event/tx id** (§10.5) | kept |
| `xo.resync_required` | revision gap not closable from the log | kept |
| `xo.limit.concurrency` | in-flight / materialization cap reached | kept |
| `xo.formula.error` | formula raised during post-commit materialization; source commit stands | kept |
| `xo.formula.not_materialized` | formula node observed without materialization requested | kept |
| `xo.connection_lost` | in-flight request lost with the socket; **not** retried | n/a |
| `xo.internal` | exposed function raised; type name + message only | kept |

---

## 8. Resource bounds

Every bound is a named, inspectable setting with a default; none is unbounded, and each closes an
observed defect. Per-adapter bounds are enforced per attachment (§2A.4.6).

| Setting | Default | Bounds | Defect closed |
| --- | --- | --- | --- |
| `max_frame_bytes` | 8 MiB | pre-allocation from declared length | unbounded pickle payload |
| `max_inflight` | 256 | requests+streams per connection | REQ/REP allowed 1 (E4) |
| `default_credit` | 64 chunks | pushed chunks before top-up | poll loop (E4) |
| `max_stream_queue` | 256 chunks | per-stream buffer | unbounded `generators[gid]` (E4) |
| `max_subscriber_queue` | 4096 events | **per-adapter** fan-out buffer | none existed (E6) |
| `max_adapters` | 16 | attachments per root | inheritance allowed exactly 1 (E0) |
| `max_path_segments` / `max_segment_bytes` | 64 / 256 B | path size | none existed |
| `max_materialized_nodes` | 256 | formula observation per subscription | n/a (new capability) |
| `max_connections` | 512 | accept queue + fd pressure | none existed |
| `log_retention` | 10 000 commits | Redis stream `MAXLEN ~` | unbounded key growth (E6) |
| `snapshot_every` | 512 commits | snapshot cadence / replay work | manual `save_keys()` (E6) |
| `dedupe_ring` | 65 536 ids | **pipeline-owned**, shared by all adapters | `hash(redis)` sender (E6) |
| `handshake_timeout` | 5 s | pre-auth resource hold | none existed |
| `idle_ping` / `idle_timeout` | 15 s / 45 s | half-open detection | none existed |

Threads: one accept thread per listener, one I/O thread per connection (or one selector thread per
adapter, configurable), one Redis reader per namespace. All non-daemon, all joined by the owning
adapter's `detach()`, and by `root.close()`. **Zero threads at import** (D17).

---

## 9. Security model

| Surface | Default | Rule |
| --- | --- | --- |
| Unix socket | on, `0600` in a `0700` dir | peer uid must match unless `allow_uids` set |
| TCP | off | **v1 binds loopback only** — a non-loopback host is refused at `serve()`, not merely warned; token **required** even on loopback |
| WebSocket | off | **v1 binds loopback only**; token + `Origin` allowlist required; per-connection prefix scope |
| Codec | tagged JSON only | no path deserializes a callable; `xo.codec.unsupported_type` otherwise |
| Exposure | root-owned, local only | no wire kind registers a callable (E2 deleted) |
| Capabilities | least privilege per adapter | enforced at the pipeline; an adapter cannot widen its own set (§2A.2) |
| Formula | code never on the wire | `materialize` is a separate capability, prefix-scoped (§6) |
| Browser | `role:"observer"` unless granted | writes only under granted prefixes; never exposes, never rewrites history |
| Errors | code + message | no traceback, no object graph, no path listing on `xo.not_found` |
| Tokens | `hmac.compare_digest` | never logged, never echoed in errors |
| Code execution | none | there is no `eval` kind; E5's `eval` branch has no successor |

Trust ladder, most to least: same-uid Unix peer → loopback TCP + token → browser WebSocket +
token + origin + prefix scope. Attaching a low-trust adapter never raises the trust of another:
capabilities are per attachment.

**v1 has no remote tier, by decision (§15.1).** RPC and WebSocket bind Unix or loopback only, so
the protocol never carries plaintext state across a network XO does not control. Remote
confidentiality is delegated to an explicitly configured TLS terminator in a later version. The
consequence worth stating plainly: `allow_uids` and the token are the *entire* v1 authorization
story, and both are local. This is a smaller promise than "secure remote access", and it is the
one this design can actually keep — every observed legacy deployment (the 5-process chain, the
browser bridge) was loopback anyway, so v1 loses no demonstrated capability.

---

## 10. Redis replication

### 10.1 Keys

| Key | Type | Purpose |
| --- | --- | --- |
| `xo:{ns}:head` | string(int) | authoritative head revision |
| `xo:{ns}:log` | stream | one entry per commit, field `b` = envelope body |
| `xo:{ns}:snap` | string | latest `Snapshot`, for fast/trimmed catch-up |
| `xo:{ns}:tx` | pubsub | low-latency wake-up (lossy by design) |
| `xo:{ns}:origins` | hash | `origin_id` → last-seen commit + heartbeat (diagnostics) |

### 10.2 Commit — one atomic Lua script

```lua
-- KEYS[1]=head KEYS[2]=log  ARGV[1]=base ARGV[2]=body ARGV[3]=new_head ARGV[4]=maxlen
local head = tonumber(redis.call('GET', KEYS[1]) or '0')
if head ~= tonumber(ARGV[1]) then
  return {'conflict', head}                      -- -> xo.conflict, never last-writer-wins
end
redis.call('SET', KEYS[1], ARGV[3])
local id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[4], '*', 'b', ARGV[2])
return {'ok', id}
```

`SET` + `XADD` + the revision check are one atomic unit. Pub/Sub publish happens **after** `ok`,
so a subscriber woken by it can never read a revision that does not exist yet.

### 10.3 Catch-up

1. `GET xo:{ns}:head`; equal to ours → subscribe, `READY`.
2. Behind → `XRANGE xo:{ns}:log (last_id +` and replay. Entries are complete units (D4), so
   replay is safe at any interruption point.
3. Needed id trimmed away → `GET xo:{ns}:snap`, apply, then `XRANGE` from the snapshot's stream
   id. This is the `xo.resync_required` path.
4. Then `XREAD BLOCK` from the last applied id, with Pub/Sub as the fast path; anything arriving
   twice is deduped by `event_id` in the **pipeline-owned** ring (D16, §2A.4.2).

Reader: **one** thread per namespace, `XREAD BLOCK 1000`, non-daemon, joined by `detach()`.
Contrast E6: `run_in_thread(sleep_time=.00001, daemon=True)` per client, unjoinable.

### 10.4 Conflict behavior

```
W1: commit(base=8123) -> ok, head=8124
W2: commit(base=8123) -> conflict(head=8124)
      -> xo.conflict raised to W2's caller with base_revision + head_revision
      -> W2 catches up (XRANGE) and only then MAY retry
```

The transport never auto-retries a conflict (D14). Two helpers are offered **by name, opted into
explicitly**: `xo.retry_on_conflict(fn, attempts=3)` for idempotent recomputation, and
`node.merge(...)` for last-writer-wins.

### 10.5 `CommitOutcomeUnknown` — reconcile by revision + id

Core's requirement. The window: the Lua script may have executed while the reply was lost
(connection died between `EVALSHA` and its response). The writer **must not** blindly retry — the
commit may already be durable, and a blind retry would either double-apply or spuriously conflict.

```
commit(base=R, event_id=E) --- reply lost --->  outcome UNKNOWN
        |
        v
  reconcile:
    head = GET xo:{ns}:head
    if head == R                      -> not applied      -> safe to resend the SAME event_id
    if head == R + n                  -> scan XRANGE (R, +] for E  (event_id, or events[0] for a tx)
          found  -> already applied   -> adopt it, do NOT resend (dedupe would drop it anyway)
          absent -> another writer won -> xo.conflict, catch up, then decide
    if head unreadable                -> stay xo.commit_outcome_unknown, surface it, never guess
```

Three properties this relies on, none accidental: the revision is monotonic and authoritative;
`event_id` is stable and carried inside the body (so a durable commit is *findable*); and
`transaction_id` derives from `events[0].event_id`, so a transaction is found by the same scan
with no extra field. The resend is safe precisely because dedupe is by `event_id` (D16) — a
duplicate that races the reconcile is dropped rather than double-applied.

**`xo.commit_outcome_unknown` is a surfaced, named state, never silently swallowed** (contrast
E6's blanket `except: print("Failed: Redis is not connected")`).

---

## 11. Dependency-free JS client

One file, no build step, no dependency — deleting `socket.io-client`, `flask-socketio`, and
`eventlet` from the stack (`freshSvelt/package.json:22-23` listed `socket.io-client` as a runtime
dependency). Uses only `WebSocket`, `Proxy`, `JSON`, `TextEncoder`: baseline in every current
browser and in Bun/Node. The URL is `ws://` on loopback because v1 binds loopback only (§9,
§15.1); serving a browser on another host is a later TLS-terminator concern, not a client change —
the client takes whatever URL it is given.

```javascript
import { connect } from "./xo.js";

const xo = await connect("ws://127.0.0.1:7802/xo", {
  namespace: "app", token: TOKEN, prefixes: [["ui"]],
});

xo.ui.chat.count.value;          // 4
xo.ui.chat.value;                // "ready"   (parent keeps its own value)

xo.ui.chat.draft.value = "hello";        // -> event back to Python (E5's missing direction)

xo.ui.chat.on("change", e => render(e.path, e.payload.new));
xo.on("state", s => badge(s));           // connecting | catching_up | ready | backoff

const items = await xo.call(["search"], ["shrooms"]);
for await (const chunk of xo.stream(["query"], ["what time is it?"])) print(chunk);

// derived (formula) values are explicitly observed and explicitly NOT state:
const total = xo.observe(["cart","total"]);   // sends `materialize`; returns a handle
total.derived;                                 // 41.97 | undefined until first projection
total.on("derived", d => render(d.value));     // d = {value|status, generation, cause_revision}
total.release();                               // last observer -> recompute stops server-side
```

A projection never lands in the state tree: `xo.cart.total.value` stays `undefined` while
`total.derived` carries the computed number. That separation is deliberate and mirrors the wire
(§6 F6) — a derived value cannot be written back, cannot be replayed, and a stale
`cause_revision` is dropped by the client exactly as it is by a Python peer. `xo.observe` on a
path with no formula, or without the `materialize` capability, surfaces
`xo.formula.not_materialized` rather than a silent `undefined`.

Properties, each tied to a decision: applies snapshot then events, tracks `revision`, requests
replay on reconnect and accepts `xo.resync_required` → snapshot (D12, §10.3); dedupes on
`event_id` and drops own-`origin_id` echoes (D16); grants credit and cancels on `break`/`return`
from `for await` (D10, §5.4); **no `eval` path** (D13); bounded local queues using the same
`xo.backpressure` code. `xo.toString()` renders a node's value, preserving the
`{@html xo.html.toString()}` idiom in `App.svelte` — that demo keeps working, without `eval` and
without the magic `all` key, and now served by an adapter on the same root as Redis rather than a
separate program (E0.1).

---

## 12. Integration scenarios (real, from the evidence)

**S0 — the composition the human asked for.** One process attaches Redis + RPC + bridge to one
root (§2A.5) while a second process attaches Redis + RPC to its own root, and a browser connects
to the bridge.
Then: impossible — one root could be `FreshRedis` **or** `FreshZero`, and `JS.py` had to be a
separate program hooked to `redis.all`.
Now: `app.ui.chat.count.value = 4` produces one commit → Redis atomic Lua commit, bridge envelope
to `["ui"]` subscribers, RPC untouched (no `replicate_out`). The peer process applies from the
Redis log; the browser applies from the bridge; **nothing echoes** because apply-never-republishes
plus one pipeline-owned dedupe ring. Detaching Redis leaves the browser live; detaching the bridge
leaves replication live; `close()` joins everything.
Gate: G21–G24.

**S1 — the 5-process AIrouter chain** (canon: `local.py`, `router_server.py`, `fusion_server.py`,
`front_runner.py`, plus a REPL).
Then: hardcoded ports with `_inc` offsets, killing whatever held them, streaming chunks through
the polled parking lot at a 100 ms floor on a serialized server; and each process had a *separate*
root for state versus RPC (E0.2).
Now: each attaches an `RpcAdapter` on a Unix socket named by namespace (no `_inc`, no kill) **to
the same root that holds its state**; the REPL's `for a in c.query(...)` receives pushed chunks
under credit; a slow pipeline no longer blocks another client's `index()`; `Ctrl-C` sends
`cancel`, the generator's `finally` runs, exactly one terminal arrives.
Gate: canon transcript reproduced end to end with per-chunk latency measured (§13).

**S2 — two processes sharing state through Redis** (`programB.py` + `JS.py` pattern).
Then: pickled values under dotted keys, no ordering, missed publishes undetectable, loop
suppression via `skip_publish` + `hash(redis)`.
Now: atomic Lua commit with expected-revision; peers apply from Pub/Sub or `XREAD`; dedupe by
`event_id`; `kill -9` and restart resumes by `XRANGE`, or snapshot + replay if trimmed, and says
which via `xo.resync_required`; a lost reply reconciles by revision + `event_id` (§10.5) instead
of blind retry.
Gate: real Redis, killed mid-commit, converged byte-identical; concurrent writers produce exactly
one `xo.conflict`.

**S3 — browser live update** (`programB.py` → `App.svelte`).
Then: `redis.all = ['html', ...]` → global broadcast → `eval` branch in the client.
Now: `app.ui.banner.html.value = "<h2>…</h2>"` → only holders of the `["ui"]` prefix receive it;
the app renders the string because it chose to; a client without the prefix receives nothing; there
is no `eval` kind. The browser also writes back, closing `XOJS.md`'s
`getting data from client coming soon`.
Gate: real browser, two tabs, one scoped out; a cancelled browser stream leaves no live server
generator.

**S4 — a formula observed across the wire** (§6). A Python process defines
`total = formula(lambda n: sum(c.value for c in n.parent.items))`; a browser subscribes with
`materialize: [["cart","total"]]` through the bridge while an RPC peer also reads it.
Now: formula code stays in Python; source `set_value`s replicate; after each durable source commit
the observed formula recomputes once and one non-authoritative `DerivedEvent` projection is fanned
out to both observers. Neither observer needs a formula engine; neither applies it as a source
mutation nor advances revision. Observation is the union across adapters; when both drop it,
recompute stops. A raising formula yields `xo.formula.error` while the source commit stands.
Restart the Python process and replay from the log: authored state returns exactly, and
`cart.total` is absent until something observes it again — laziness survives a restart, not just
the network.
Gate: G14–G16, G26, plus union-observation (one recompute, two deliveries).

**S5 — the hostile peer.** A process sends a `dill` payload, an unknown kind, an 800 MB declared
`LEN`, an Event missing `origin_id`, and a call to an unexposed path.
Then: the pickle would execute and `if True` would install a function.
Now: `xo.protocol.malformed` (closed), `xo.protocol.malformed` (closed),
`xo.protocol.frame_too_large` (closed **before** allocation), `xo.protocol.malformed` (required
field missing, closed), `xo.not_found` (connection kept, no path listing leaked). No pickle, no
exposure over the wire, no `eval`, no formula code accepted, and a bridge-attached browser cannot
reach a path outside its prefix even if it holds a valid token.
Gate: adversarial suite; each input yields its named code and documented disposition.

---

## 13. Measurable gates

Loopback, same host, Python 3.11+, median of ≥10 000 samples, p50/p99 reported. Thresholds are set
against legacy numbers the evidence makes computable — the 100 ms per-chunk floor is a *literal*
`time.sleep(0.1)` at `embedded/freshServer.py:97`, an exact baseline.

| # | Gate | Threshold | Legacy baseline |
| --- | --- | --- | --- |
| G1 | Local mutation, no attached adapter | **0 bytes encoded, 0 syscalls, 0 wrapper allocated for a singleton commit** | already true; must not regress |
| G2 | Unix RPC round trip, small payload | **p50 < 250 µs, p99 < 1.5 ms** | pickle + REQ/REP lockstep |
| G3 | Unix stream per-chunk delivery | **p50 < 200 µs, p99 < 2 ms** | **~100 ms floor** — target ≥500× |
| G4 | Stream throughput, 1 KB chunks | **> 100 000 chunks/s** | ≤ 10/s per stream |
| G5 | Head-of-line: 5 s stream in flight, concurrent RPC | **p99 < 2 ms (unaffected)** | blocked for the full 5 s |
| G6 | Redis commit → remote apply | **p50 < 2 ms, p99 < 10 ms** | comparable, but unordered |
| G7 | Snapshot + replay to head, 10 000 nodes | **< 250 ms** | no mechanism existed |
| G8 | Producer 10× faster than consumer, 10 min | **RSS flat, bounded by `max_stream_queue`** | unbounded deque growth |
| G9 | Cancel → `finally` observed → one terminal | **100 % over 10 000 iterations, 0 orphan generators** | no cancel existed |
| G10 | `kill -9` a replica mid-commit, restart | **byte-identical state, 100 % over 100 trials** | no catch-up existed |
| G11 | Concurrent writers at the same base | **exactly one `ok`, others `xo.conflict`, 0 lost updates** | last-writer-wins, silent |
| G12 | Trimmed-log catch-up | **`xo.resync_required` then snapshot, converged, 0 silent gaps** | undetectable |
| G13 | Adversarial inputs (S5) | **each yields its named code; 0 code execution; 0 unbounded allocation** | pickle RCE, `if True` install |
| G14 | Formula: no observer | **0 recomputes triggered by replication** | n/a (new) |
| G15 | Formula: observer attached / detached | **recompute on source commit; 0 after last observer drops; formula code bytes on wire = 0** | n/a (new) |
| G16 | Formula raises | **`xo.formula.error` delivered; source commit still committed; 0 rollbacks** | n/a (new) |
| G17 | Idle cost, 1 connection + Redis subscribed | **< 0.1 % CPU** | `sleep_time=.00001` spin thread |
| G18 | Import cost | **0 threads, 0 sockets, 0 Redis connections, 0 killed processes** | connect + bind + kill at import |
| G19 | JS client | **0 runtime deps; browser + Bun; < 12 KB minified** | `socket.io-client` + matched server libs |
| G20 | Clean shutdown | **`close()` detaches all, joins every thread; 0 daemon threads; 0 leaked fds** | daemon threads, unobservable |
| **G21** | **Coexistence** — Redis + RPC + bridge attached to one root; one mutation | **1 commit; 1 revision; exactly 1 Redis log entry; exactly 1 envelope per subscribed bridge client; 0 to RPC; ≤1 encode per codec config; local p99 unchanged vs. G1 within 10 %** | impossible (E0) |
| **G22** | **No echo** — 3-adapter diamond (peer via Redis *and* direct, browser also fed by a replicated sibling) | **each `event_id` applied exactly once per root; 0 republished remote events; 0 unbounded growth in the shared dedupe ring over 1 M events** | `skip_publish` + `hash(redis)`, unverifiable |
| **G23** | **Ownership / isolation** — kill Redis, then detach the bridge, mid-traffic | **RPC + bridge keep serving with Redis down; the failed adapter alone reports `BACKOFF`/`xo.connection_lost`; detaching the bridge leaves Redis + RPC unaffected; every detached thread joined; 0 orphan streams** | one failure = one dead object |
| **G24** | **Attach/detach churn** — attach and detach all three 1 000× under continuous mutation | **0 leaked threads/fds/subscriptions; revision monotonic throughout; a late-attaching adapter converges to head; `attach` twice raises** | no lifecycle existed |
| G25 | `CommitOutcomeUnknown` reconcile — sever the connection between `EVALSHA` and its reply, 1 000× | **0 double-applies, 0 spurious conflicts; every case resolves to applied / not-applied / surfaced `xo.commit_outcome_unknown`** | blanket `except: print(...)` |
| **G26** | **Derived projection is not authoritative** (Main's ruling, §15.1) — observe a formula, commit sources, then replay from the log and reconnect | **0 `DerivedEvent`s in `xo:{ns}:log`; base revision advances only on authored commits; replay reconstructs authored state byte-identically with 0 derived values applied; 0 `xo.conflict` and 0 `xo.resync_required` attributable to a projection; a projection with a stale `cause_revision` is discarded, not applied; a reconnecting observer re-derives from its re-declared observation** | n/a (new) |

---

## 14. Implementation sequence

Each step independently verifiable; nothing later is needed for anything earlier to be correct.

1. **Codec + frame.** Tagged JSON, frame read/write with bound check. Gate: round-trip property
   tests incl. refusal of unsupported types and over-limit `LEN`.
2. **Envelope + handshake.** Kinds, `mid`/`rid`, version negotiation, token, required-field
   validation. Gate: mismatched protocol fails closed with `xo.protocol.version`.
3. **Pipeline + adapter contract.** `attach`/`detach`, capability enforcement, shared dedupe ring,
   root-owned `origin_id`, encode-once fan-out, per-adapter queues. Gate: G1, G24 (with a stub
   adapter, before any transport exists).
4. **RPC adapter (Unix).** Root-owned `public` registry, `index()`, call/result, deadlines,
   `xo.not_found`, `xo.internal`. Gate: E7's canon transcript reproduced.
5. **Multiplexing.** Concurrent in-flight, `max_inflight`, `xo.limit.concurrency`. Gate: G5.
6. **Streaming.** `start`/`chunk`/`credit`/`end`, cancel, the terminal law incl. detach. Gate: G3,
   G8, G9.
7. **Replication out/in over the pipeline.** `sub`/`unsub`, prefix filter, bare-Event vs
   Transaction framing, prior-value omission, opt-in `EventView`, optional diagnostics. Gate: G1
   again with a subscriber attached.
8. **Redis adapter.** Lua commit, stream log, snapshot, `XREAD` reader, Pub/Sub fast path,
   reconcile path. Gate: G6, G10, G11, G12, G25.
9. **Multi-adapter validation.** Redis + RPC + a stub bridge on one root. Gate: G21, G22, G23.
10. **Formula materialization channel.** Observation lifecycle as local bridge policy, union
    across adapters, post-commit recompute trigger, the `derived` envelope kind as a
    non-authoritative projection, `xo.formula.*`. Gate: G14, G15, G16, G26.
11. **TCP transport (loopback only).** Same adapter code path, explicit opt-in, token enforcement,
    non-loopback host refused at `serve()` (§9). No TLS in v1 — deferred to a configured
    terminator (§15.1).
12. **Bridge adapter + JS client.** Same envelopes, prefix scope, bidirectional apply. Gate: G19,
    then G21–G23 re-run with the real bridge, + S3.
13. **`xo.compat`.** Shims that compose adapters, deprecation warnings, named refusals for deleted
    behaviors.

---

## 15. Decisions fixed by Main, and what remains open

Stated, not guessed. The formula transport rule (§0.3), the composition requirement (§0.4, §2A),
and the core schema (§0.1) are **fixed**, not open.

### 15.1 Closed by Main after this report's first draft

These were the open items; Main has ruled on four. Recorded here with the sections each ruling
touches, so no stale alternative survives in the document.

| Was open | Ruling | Sections carrying it |
| --- | --- | --- |
| Multi-namespace per connection / per root | **One namespace per root.** `ns` stays per-envelope (cheap, and it validates), but a session and a root each carry exactly one namespace; cross-namespace transactions are not a v1 concern. | §3.2 (`ns` validation), §10.1, §12 S1 |
| TLS / non-loopback confidentiality | **v1 RPC and WebSocket bind Unix or loopback only.** Remote confidentiality is delegated to an explicitly configured TLS terminator later; XO ships no `ssl` wrapping in v1 and no non-loopback bind. | §9 (binding table), §14 step 11 |
| Materialized value: revision-bearing or post-commit cache | **Non-authoritative `DerivedEvent` projection.** It does not advance base revision, does not enter history, and is not a second state authority. | §0.3, §3.2–3.3 (`derived` kind), §6 (F2, F6), §7, §11, §12 S4, G26 |
| Capability grammar spelling | **Deferred deliberately** — consolidated after the core SDK exists. The enforcement point (the pipeline, §2A.2) is decided and does not move; only the spelling is pending. | §2A.2, §9 |

### 15.2 Still open

1. **Async surface.** This report specifies blocking clients and threaded adapters, matching the
   observed consumers (`router.py:13-14` carries `"async": False`). An `asyncio` adapter over the
   same protocol is straightforward; whether it ships in v1 is scope, not protocol — the wire and
   the adapter contract are identical either way.
2. **Redis Cluster.** The Lua script touches two keys in one namespace; under Cluster they need a
   hash tag (`xo:{ns}` already supplies one if `ns` is the tag). Untested against a real cluster;
   the evidence shows single-instance use.
3. **Compression threshold.** `compress_over = 4096` is reasoned, not measured. G2/G3 should be
   re-run with it disabled to confirm it never costs latency on the small payloads that dominate.
