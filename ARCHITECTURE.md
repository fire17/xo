# XO architecture

## Decision

XO is one small state primitive with an explicit capability runtime. Persistence, history, formulas, validation, service exposure, and bridges attach to one root through contracts; they are not a subclass ladder or a bundle of copied applications.

```text
                              browser / JS
                                  │
                           WebSocket bridge
                                  │
Python ── local events ── XO state tree ── Redis backend ── other processes
                                  │
                           revision history
                                  │
                         RPC service registry
                                  │
                    TCP / Unix socket microservices
```

Vertical projects such as AAA, MagicLLight, AIrouter, and XO-Svelte remain consumers and evidence sources. Their XO forks are migration inputs, never additional runtime authorities.

## Invariants

1. **Value and children coexist.** Every node may hold one value and any number of children. Reassigning the value never destroys descendants.
2. **One path model.** Attribute, item, dotted-path, Redis, RPC, history, and JavaScript operations resolve through the same canonical tuple path.
3. **One mutation model.** Set, delete, remote sync, restore, and history checkout become typed events. Backends and bridges consume events; they do not reimplement mutation semantics.
4. **No echo loops.** Every event carries an immutable event ID and origin ID. A replica applies each event at most once and never republishes a remote event as new work.
5. **Write-through persistence.** With a strict backend attached, durable commit succeeds before the local mutation becomes visible. Failure leaves local state unchanged.
6. **Deterministic snapshots.** A snapshot is canonical JSON-compatible data plus namespace, revision, and schema version. Ordering does not affect identity.
7. **Formula code stays local.** Derived nodes may cache materialized values, but formula functions/closures never enter snapshots, Redis, RPC, or WebSocket payloads.
8. **Safe network boundary.** Network protocols never unpickle remote bytes and never evaluate remote JavaScript. Custom values require an explicitly registered codec.
9. **Local-first operation.** Bare XO has no required service, third-party dependency, background worker, or optional-capability allocation. Redis, RPC, WebSocket, history, formulas, validation, and compatibility attach independently through explicit capability specs.
10. **Composable capability fusion.** Any compatible capabilities may inhabit one root simultaneously. Composition is validated from declared requirements, provisions, conflicts, lifecycle ownership, and ordering—never Python MRO or cooperative `super()` convention.
11. **One authority per semantic role.** A profile may have many event observers/bridges but exactly one commit coordinator, one canonical event stream, and at most one authoritative strict durability chain. Capabilities never shadow or duplicate core state.
12. **Bounded work.** Incoming frames, paths, messages, snapshots, dependency graphs, subscriber queues, and RPC concurrency have explicit limits.
13. **Measured compatibility.** Legacy names exist only where an observed consumer requires them, and each is backed by a behavior test—not a silent alternate implementation.

## Package map

```text
src/xo/
  __init__.py          stable public surface
  core.py              XO nodes, paths, snapshots, mutations
  events.py            immutable Event and EventBus
  codec.py             safe tagged JSON codec and extension registry
  history.py           revision DAG, undo/redo/checkout
  formula.py            lazy formulas and dependency graph
  backends/
    base.py            storage/sync protocol
    redis.py           raw RESP Redis persistence + Pub/Sub
  rpc/
    protocol.py        framed request/response envelope
    server.py          function registry, TCP/Unix server, streaming
    client.py          dynamic remote proxy
  web/
    websocket.py       RFC 6455 bridge
  compat.py            observed xoBenedict/FreshRedis/xoBranch/FreshZero APIs
  cli.py               inspect, serve, benchmark, doctor
js/
  xo.js                dependency-free JS state proxy + WebSocket client
```

The core imports only the Python standard library. Optional layers may import sibling modules but never flow backward into `core.py`.

## Capability fusion — composition, not inheritance

The historical sequence `xoBenedict → xoRedis/FreshRedis → xoFunctional`, alongside separate `xoBranch` and `FreshZero` classes, proved individual behaviors but made every new combination depend on inheritance order, shared class globals, and every override calling `super()` correctly. That cannot produce arbitrary safe hybrids. The unified system therefore treats capabilities as root-scoped collaborators over one sealed semantic kernel.

```python
from xo import XO, history, rpc_server, service, validation, websocket
from xo.backends import backend
from xo.backends.redis import RedisBackend

state = XO.compose(
    "app",
    validation({"ui.count": lambda value: isinstance(value, int)}),
    history(),
    backend(RedisBackend("redis://127.0.0.1:6379/0")),
    service(),
    rpc_server("unix:///tmp/app.xo"),
    websocket(port=8765, writable=(("ui",),)),
)

# Curated profile; every supplied service/bridge is still an ordinary capability.
state = XO.recommended(
    "app",
    durability=backend(RedisBackend("redis://127.0.0.1:6379/0")),
    services=(rpc_server("unix:///tmp/app.xo"),),
    projections=(websocket(port=8765, writable=(("ui",),)),),
)
```

`XO()` remains the bare atomic object. `XO.compose(...)` performs a deterministic build and returns that same public type backed by a root `CapabilityRuntime`. A capability is a frozen specification that creates one root-scoped runtime object; child node references never instantiate capabilities. Compatibility constructors such as `FreshRedis`, `xoBranch`, and `FreshZero` may translate legacy arguments into a composition profile, but contain no alternate state implementation.

Each capability declares machine-checkable metadata:

```python
class CapabilitySpec(Protocol):
    key: str                         # stable instance key
    provides: frozenset[str]         # e.g. history, durability, rpc, web_projection
    requires: frozenset[str]
    conflicts: frozenset[str]
    before: frozenset[str]           # partial-order constraints
    after: frozenset[str]
    def build(self, context: BuildContext) -> Capability: ...

class Capability(Protocol):
    def prepare(self) -> None: ...   # allocate/probe; no externally visible state
    def start(self) -> None: ...     # only after every capability prepared
    def close(self) -> None: ...     # idempotent, reverse dependency order
```

The builder rejects missing requirements, duplicate singleton provisions, conflicts, and ordering cycles before starting anything. It topologically compiles sparse hook arrays once; mutations do not scan a registry or dispatch through Python MRO. Capability attachment is transactional: build all → prepare all → atomically publish runtime → start all; failure closes prepared components in reverse order and exposes no half-profile. Runtime detachment is allowed only for capabilities declaring it safe and after quiescence; otherwise profiles are immutable until root close. This keeps lifecycle and performance understandable.

The semantic pipeline exposes narrow, typed seams rather than a universal callback bag:

1. **validators/normalizers** inspect a proposed transaction before IDs or persistence; deterministic order, no mutation or I/O unless explicitly permitted;
2. **commit coordinator** owns strict compare-and-swap and unknown-outcome recovery; exactly one compiled chain;
3. **state/history/formula phase** is canonical core behavior; capabilities cannot reorder it;
4. **event observers** consume accepted transactions after unlock (`History`, metrics, audit, local subscribers);
5. **projections/bridges** publish the same events (`Redis` live sync, WebSocket/JS) with origin/event dedupe;
6. **services** expose allow-listed functions and state operations while owning their sockets independently.

History is an observer plus inverse planner; Redis strict persistence is the commit coordinator plus a remote-event source; RPC service and WebSocket are independent endpoints; validation contributes a pre-commit policy. Formula dependency tracking is a dormant core facility because value reads and invalidation require kernel-level semantics, while observation/export of derived values is a capability. This division allows every useful fusion without allowing extensions to corrupt atomicity.

The recommended profile is data, not another implementation: `History + Service` by default, plus optional strict durability, RPC/service transports, WebSocket projections, and validation. Named profiles are inspectable configuration *except secrets and callables* and expand into ordinary specs. Users can replace or add a capability without changing the XO object model.

Composition gates:

- pairwise and representative N-way contract tests cover `History×Redis`, `Redis×WebSocket`, `Service×WebSocket`, `Validation×History`, formulas observed through bridges, and the full recommended profile;
- every permutation of input spec order compiles to the same declared execution order or fails with a precise cycle/conflict;
- one mutation creates one semantic transaction regardless of capability count; no echo, duplicate history node, or repeated formula invalidation;
- prepare/start failure at each capability leaves no ports, threads, Redis subscriptions, history records, or attached partial runtime;
- close is reverse-ordered, idempotent, bounded, and leaves no owned thread/socket;
- optional capabilities disabled have zero per-node allocation and no registry scan on the scalar-set hot path;
- third-party capabilities use only the public capability SDK and pass the same lifecycle/concurrency/serialization conformance suite.

### Fixed v1 boundaries

- One namespace per root. Cross-namespace transactions are rejected; coordination belongs above XO.
- RPC and WebSocket listeners bind only Unix sockets or loopback in v1. Remote deployment requires an explicit external confidential/authenticated terminator; plaintext non-loopback startup is refused.
- Derived formula values are non-authoritative projections (`DerivedEvent`) tied to a cause revision; recomputation does not advance the base state revision or enter history.
- Numeric path/frame/snapshot/queue/formula/capability/shutdown limits are mandatory configuration with conservative defaults, then tuned only from adversarial measurements. “Unbounded” is never an allowed sentinel on network or background-work surfaces.
- A strict durability timeout after send is `CommitOutcomeUnknown`, freezes ordinary access in recovery-required state, and reconciles by revision plus event/transaction ID. It is never treated as definite failure or retried with a fresh ID.
- Namespace epoch plus contiguous revisions fences replicas. A gap requires catch-up/resnapshot; same epoch/revision with a different canonical hash freezes writers as split-brain.

## Core object

`XO` implements `MutableMapping[str, XO]`. A node stores:

- `_value`: an internal `MISSING` sentinel or a user value;
- `_children`: insertion-ordered child map;
- `_parent` and `_key`: stable identity without rebuilding paths on every access;
- `_root_state`: shared lock, revision, event bus, backend, history, and origin;
- `_path_cache`: computed once per node.

### Access

```python
state = XO("app")
state.user.name = "Tami"
state.user.name.meta.updated_by = "voice"

assert state.user.name.value == "Tami"
assert state["user.name"] is state.user.name
assert state.peek("missing") is None       # no mutation
```

A missing public attribute creates an empty child to preserve XO's fluent expando behavior. Inspection uses `peek`, `contains_path`, or snapshots so reads do not accidentally grow state.

### Mutation lifecycle

```text
resolve path
→ construct Event(old,new,revision+1,origin,event_id)
→ validate codec and limits
→ backend.commit(event), if strict backend
→ apply under root lock
→ append history
→ notify local subscribers
→ bridge to peers
```

Remote events enter at `apply_remote`: deduplicate, validate causal namespace/schema, optionally persist without re-publishing, apply, notify, and advance the observed revision.

### Subscriptions

- `node.subscribe(callback, recursive=False)`
- `root.subscribe(callback, pattern="user.*")`
- compatibility: `node @= callback`

Callbacks receive one immutable `Event`. Exceptions are isolated and reported to an error hook; one subscriber cannot block or corrupt another. Synchronous callbacks are the default low-latency path. Queue/executor dispatch is explicit.

## Redis backend

XO speaks Redis RESP directly over a socket; `redis-py` is not required.

For namespace `demo`:

- hash: `xo:{demo}:values`, field = encoded canonical path, value = tagged JSON;
- metadata: `xo:{demo}:meta`, revision and schema;
- channel: `xo:{demo}:events`.

A Lua commit atomically checks the expected revision, applies `HSET`/`HDEL`, increments the revision, and publishes the event. This prevents half-saved local state and gives concurrent writers an explicit conflict instead of last-writer ambiguity. The listener reconnects with bounded exponential backoff and catches up from the hash snapshot before resuming live events.

Redis semantics:

- one connection for commands, one dedicated Pub/Sub connection;
- connect and operation timeouts;
- origin/event deduplication;
- no pickle;
- namespace isolation;
- `close()` is idempotent and joins the listener;
- no background thread until sync is explicitly started.

## Revision history

History is an append-only DAG, not a destructive undo stack.

- Each accepted mutation produces a revision with parent revision ID and event.
- `undo()` checks out the parent without deleting the abandoned future.
- A mutation after undo creates a new branch.
- `redo()` requires an unambiguous child or an explicit revision.
- Snapshots can be materialized at any revision and hashed.
- `BranchXO`/`xoBranch` enables history by default and exposes branch navigation compatible with observed legacy use.

History records state semantics only. Persistence and network delivery receipts are evidence attached to the revision, not additional state revisions.

## Lazy formulas

`node.formula(function)` registers a local computed value. Evaluation runs inside a context-local dependency recorder; each XO value read contributes its canonical path and observed revision. A successful computation atomically replaces the node's dependency set and cached value.

Source mutation never runs formula code while the mutation lock is held. It marks direct and transitive dependents dirty. An unobserved formula recomputes on the next value read. A formula with a local subscriber, persistence materialization policy, or remote bridge observer is scheduled once after the source transaction commits. Multiple invalidations coalesce.

```python
state.subtotal.formula(lambda: state.price.value * state.quantity.value)
state.total.formula(lambda: state.subtotal.value + state.tax.value)
```

Required semantics:

- dynamic dependencies are replaced after each successful recomputation;
- cycle detection reports the complete formula path chain;
- concurrent readers use single-flight computation, then share the cache;
- formula exceptions are cached against the dependency revision vector and re-raised without hot-loop recomputation until an input changes or `invalidate()` is called;
- if a dependency revision changes during compute, the result is discarded and retried within a bounded policy;
- formula writes to XO are rejected by default during evaluation; explicit effects belong in subscribers;
- only materialized values and their normal events may persist/replicate; function objects and dependency closures never cross a codec or transport boundary;
- restoring history invalidates formulas from affected source paths, and derived recomputation creates derived events without rewriting the restored source revision.

The old `xoFunctional` (`xo.py:2584-2668`, commit `ca092af`) eagerly ran on every change and used `skip_target` flags to avoid recursion. The unified design preserves the formula interaction, not that control flow. The `$=` syntax experiment remains provenance only; normal Python uses `.formula(...)`.

## RPC and microservices

`service()` owns one root-scoped registry; `rpc_server()` owns a bounded Unix or loopback TCP listener that serves that same registry.

```python
from xo import XO, rpc_server, service
from xo.rpc import Client

state = XO.compose("app", service(), rpc_server("unix:///tmp/xo.sock"))

@state.public.image.thumbnail
def thumbnail(image_id: str) -> str:
    return f"thumb:{image_id}"

state.start()
with Client("unix:///tmp/xo.sock", namespace="app") as remote:
    image = remote.image.thumbnail("42")
state.close()
```

`Client` produces the dynamic proxy; root close retires RPC connections and the listener with the rest of the capability runtime.

Protocol properties:

- 4-byte big-endian frame length + versioned tagged-JSON envelope;
- request IDs, deadlines, typed errors, and protocol version on every exchange;
- unary values and bounded generator/iterator streaming;
- explicit `ping`, `describe`, `call`, `get`, `set`, and `delete` operations;
- per-connection and global concurrency limits;
- loopback/Unix default; non-loopback requires an authentication token;
- function exposure is allow-list only;
- no implicit import, attribute traversal, pickle, or `eval`.

## Python ↔ JavaScript sync

`WebSocketBridge` translates the same XO event envelope to RFC 6455 text frames. The dependency-free JS client exposes a Proxy with value-and-children semantics and reconnect/catch-up behavior.

Handshake:

1. client sends `hello(protocol, namespace, origin, last_revision)`;
2. bridge responds with `snapshot` or an ordered catch-up sequence;
3. both sides exchange `set`, `delete`, `event`, `ping`, and `ack` envelopes;
4. unsupported schema/protocol fails explicitly.

Browser writes are opt-in per bridge and may be limited to path prefixes. HTML rendering remains an application decision. The old remote `eval` path is deliberately not preserved because it turns any connected writer into arbitrary browser code execution.

## Compatibility surface

Observed names map to the unified implementation:

| Legacy name | Unified behavior |
|---|---|
| `xoBenedict` | `XO` with fluent child creation and value coexistence |
| `xoRedis` / `FreshRedis` | `XO` constructed with `RedisBackend` |
| `xoBranch` | `BranchXO` with a revision DAG |
| `FreshZero` | `Service` + `Server` |
| `FreshClient` | `Client` dynamic proxy |
| `xoJS` | JS proxy/client in `js/xo.js` |

Compatibility is source-level where practical, not bug-for-bug. Ambiguous behavior—global singleton roots, mutation-on-inspection, process-killing port takeover, unsafe pickle, remote `eval`, and silent retry—is rejected and documented in migration errors.

## Performance budgets

Canonical budgets are measured on warmed Apple M3 Max runs and recorded with environment metadata. CI also enforces a portable profile for heterogeneous shared runners: the same ceilings except `import xo` ≤ 50 ms and clean formula read ≤ 5 µs, covering observed Python 3.11–3.14 runner variance without weakening the canonical local regression gate.

| Surface | Budget |
|---|---:|
| `import xo` | ≤ 25 ms median |
| create root | ≤ 10 µs median |
| existing child lookup | ≤ 1 µs median |
| local scalar set including event | ≤ 5 µs median |
| 5-segment path set | ≤ 15 µs median |
| snapshot of 10,000 scalar leaves | ≤ 20 ms median |
| local Redis durable set | ≤ 1 ms median |
| loopback RPC unary call | ≤ 1 ms median |
| Python→JS local update | ≤ 10 ms median |

A candidate cannot release if it exceeds the legacy measured median on an equivalent behavior, violates the canonical budget on the reference workstation, or violates the portable ceiling in CI without an explicit, evidence-backed exception.

## Failure containment

| Failure | Behavior |
|---|---|
| Redis unavailable before strict write | mutation fails; local state unchanged |
| Redis disconnect during live sync | local-only mode only if configured; otherwise writes fail closed; reconnect catches up |
| duplicate/reordered event | event ID dedupe; revision conflict triggers resnapshot |
| subscriber raises | error hook receives failure; remaining subscribers continue |
| RPC client vanishes mid-stream | stream is cancelled and resources close |
| oversized/malformed frame | connection closes with typed protocol error |
| browser reconnects after gap | revision-based catch-up or full snapshot |
| callback recursively writes | allowed under reentrant lock; new event/revision; recursion depth guarded |
| concurrent local writers | root lock serializes mutations; deterministic revisions |
| concurrent Redis writers | expected-revision Lua check; explicit conflict and resync |

## Release construction

1. Differentially characterize legacy behavior before replacing it.
2. Implement the core and local event contract.
3. Add history, Redis, RPC, and WebSocket as independent conformance layers.
4. Migrate observed consumers through compatibility tests.
5. Run real Redis, real socket, real Bun/JS, crash, concurrency, and performance suites.
6. Package only after the source tree, wheel, and clean-venv install behave identically.
7. Preserve all legacy sources read-only under provenance records; do not publish archived secrets or raw transcripts.
