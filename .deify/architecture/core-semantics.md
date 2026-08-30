# XO core state semantics

**Status:** proposed definitive core contract  
**Scope:** state, events, snapshots, history, and computed nodes only. Redis, RPC, and browser transports consume this contract but do not redefine it.  
**Model:** `apiplan-openai/gpt-5.6-sol`

## 1. Decision in one page

XO is a rooted, namespaced tree. A node has an optional value **and** ordered named children; neither replaces the other. The public object is a lightweight `Node` reference into a `Tree`, not a `dict` subclass. Canonical paths are tuples of string segments. Missing fluent attributes produce virtual references, not mutations, so `xo.a.b.c = 3` remains magical while inspection cannot silently grow state.

Every accepted write is one linearizable commit. A singleton write allocates one immutable semantic `Event`; a multi-write commit uses a separate `Transaction` wrapper. The root serializes commits, validates and (in strict mode) durably compare-and-swaps before exposing state, applies all operations atomically, records history, invalidates computed dependencies, releases its lock, eagerly refreshes only observed computed nodes, then dispatches callbacks in FIFO order. Callbacks never veto, transform, or roll back committed state.

History is an append-only content DAG over a linear commit-revision stream. Undo/redo/checkout move a history cursor by committing an atomic state diff; abandoned futures remain branches. Formula functions are process-local configuration: reads capture dependencies automatically, dependency writes mark caches dirty, unobserved formulas recompute lazily on read, observed formulas recompute after commit, and no executable code enters a snapshot, event log, Redis, or wire message.

The core is stdlib-only. Portable snapshots use an explicit safe tagged codec; pickle and implicit code serialization are forbidden. Arbitrary local Python objects require explicit local-only policy and cannot silently cross a persistence or network boundary.

## Normative capability composition

XO's extension mechanism is **composition around one final core**, never an inheritance stack. History/branching, Redis durability, formula evaluation, RPC serving, JavaScript sync, validation, and future behaviors are `Capability` objects attached to the same root. They neither subclass `XO` nor wrap it in successively different state semantics. `xoBenedict`, `FreshRedis`, and `xoBranch` are compatibility constructors/proxies that assemble profiles and return the same `XO` type.

The root owns one `CapabilityRuntime` compiled from explicit capability specs. Every child `Node` shares that exact root/runtime. The kernel exclusively owns records, paths, identity, lock, revision/event allocation, primitive apply, and local subscriber order. Capabilities receive immutable plans/views/receipts; they cannot mutate records, advance revision, manufacture committed Events, recursively acquire the root lock, or call each other by concrete type.

```python
class Capability(Protocol):
    capability_id: str
    provides: frozenset[str]
    requires: frozenset[str]
    conflicts: frozenset[str]
    before: frozenset[str]
    after: frozenset[str]
    def preflight(self, context: AttachContext) -> PreparedCapability: ...
    def close(self) -> None: ...
```

Capabilities may implement typed roles only:

| Role | Cardinality / contract |
|---|---|
| `NORMALIZER` | Many, ordered; pure proposal rewrite before Event allocation, declared path scope, no I/O. |
| `VALIDATOR` | Many, ordered; read-only accept/reject after normalization. Future Pydantic is an adapter here (or explicit normalizer for coercion); core never imports it. |
| `DURABILITY` | **Zero or one coordinator**; strict CAS/commit/reconcile for a complete Event/Transaction. Redis claims it in strict mode. |
| `RECORDER` | Many, ordered; prepares inverse/log material before durability and records already-prepared material after apply. History claims it. |
| `INVALIDATOR` / `DERIVER` | Many; bounded in-lock dirty bookkeeping, then post-lock computed refresh. Formulas claim these roles. |
| `OBSERVER` | Many; post-lock projection only. Redis publish, JS sync, audit, and state-stream RPC belong here. |
| `SERVICE` | Many with disjoint bind/service claims; RPC/WebSocket call public core APIs and never start during import/build. |

One commit follows this fixed lifecycle:

```text
CORE_PLAN -> NORMALIZE* -> CORE_REVALIDATE -> VALIDATE*
 -> PREPARE_RECORDERS* -> PREPARE/COMMIT_DURABILITY?
 -> CORE_APPLY -> RECORD_APPLIED* -> INVALIDATE*
 -> unlock -> DERIVE_OBSERVED* -> CORE_SUBSCRIBERS -> OBSERVE_COMMIT*
```

Ordering uses the declared dependency DAG, never MRO, import order, magic numeric priorities, or last-wins replacement. The builder rejects missing requirements, dependency cycles (`CapabilityOrderError`), multiple durability authorities, duplicate codec/formula/service/bind/exclusive-path claims, incompatible overlapping normalizers, or an observer duplicating its durability coordinator (`CapabilityConflictError`). Validators see normalized proposals and a stable `ReadView` at `base_revision`. Recorder fallible preparation occurs before durable commit; its post-apply step must be bounded/non-failing. Post-apply derivation/subscriber/observer failures are isolated in `CommitReceipt` and never roll back state.

```python
bare = XO("scratch")  # atomic stdlib-only kernel; zero capability objects/hooks/optional allocation

state = (
    XO.builder("app")
      .use(Validation(adapter=my_schema))
      .use(History(checkpoint_every=1_000))
      .use(Formulas())
      .use(Redis(url="redis://127.0.0.1", mode="strict"))
      .use(RPC(unix="/tmp/app.xo.sock"))
      .use(WebSocket(loopback_port=8765, derived=("total",)))
      .build()
)
state.start()  # network workers start explicitly
```

`.use()` is the generic extension API; named builder methods add the same specs. `Profile.hybrid(...)` is the recommended inspectable recipe: History + Formulas + optional strict Redis + RPC Service + WebSocket/JS sync, with validation first when supplied. It expands to a frozen manifest before build; it is not another class. Bare XO compiles to a direct kernel dispatch path with no hook list traversal, diagnostics/history/formula graph allocation, thread, service, or capability object.

Runtime attach/detach occurs only through `state.reconfigure()` at a quiescent revision: compile and conflict-check a candidate graph off-side, preflight/bootstrap it, pause new commits, atomically swap the runtime, then start/stop services outside the root lock. Any failure rolls back to the old runtime. Durability handoff additionally proves namespace, revision, and snapshot hash or raises `CapabilityHandoffError`; required dependencies, active calls/streams, observed formulas, or an unreconciled commit block detach. Compatibility subclasses may only construct/delegate to profiles; multiple inheritance and dynamic XO class generation are prohibited.

## 2. Evidence: what existed, not what is proposed

| Observed legacy fact | Evidence | Consequence retained or rejected |
|---|---|---|
| A scalar value and descendants coexist (`c = 3`, then `c.d.e = ...`). | `xo-benedict/VISION.MD:105-120`; scalar reassignment updates the existing child value in `xo.py:595-632`. | **Retain as the defining law.** |
| Missing public attribute access creates an XO child. | `xo.py:1442-1462`. | Preserve fluent chaining, but return an unattached virtual reference; reject mutation-on-inspection. |
| Attribute and item access share dynamic key behavior and dotted keypaths. | `xo.py:327-410`; keypath parsing in `benedict/dicts/keypath/keypath_util.py:25-34,68-79`; separator-containing raw keys are rejected at `keypath_util.py:9-20`. | One tuple path model; dotted strings are explicit syntax, not storage identity. |
| Assigning a scalar child wraps it as an XO node with a `value` slot. | `xo.py:595-661`; output shape in `VISION.MD:113-120`. | Retain semantically without exposing a physical `"value"` child. |
| `None` is an intended stored value. | `xo.py:621-632,668-672`. | `None` is data; a public `MISSING` sentinel represents no value. |
| `node(value)` sets a value; a callable stored as value is invoked. | `xo.py:290-312`. | Preserve only in `xo.compat`; core uses `set` and a separate service registry to remove call/set ambiguity. |
| `node @= callback` subscribes; callbacks receive the new value and path metadata; callback failure is printed and isolated. | `xo.py:1464-1513`; example `VISION.MD:139-147`. | Preserve via compatibility syntax; core callbacks receive typed events and deterministic error reporting. |
| Legacy callbacks run before the final `dict` write and `__onchange__` may rewrite/veto the value. | `xo.py:549-576,719-744`; `Fresh.__onchange__` at `xo.py:2092-2100`. | Reject for observers. Pre-commit validation/transformation must be explicit; observers are post-commit. |
| FreshRedis saves and publishes changed values and suppresses republish on inbound updates. | `xo.py:2127-2161` and inbound `skip_publish` at `xo.py:2339-2346`. | Retain strict/local persistence modes and no-echo remote apply, with typed events rather than ad-hoc flags. |
| Redis state uses pickle and a separately saved flattened key list. | `xo.py:2106-2126,2144-2155,2493-2500`. | Reject unsafe pickle and dual key/value authority; one versioned snapshot/event representation is authoritative. |
| Deletion writes empty bytes then deletes one Redis key, with an unresolved whole-tree question. | `xo.py:2539-2552`. | Resolve into two distinct operations: clear a value or delete a subtree. |
| xoBranch stores alternate branches and moves a marker with left/right/home/end. | `xoDeque.py:324-376,453-486,623-650,764-790`. | Retain behavior as a revision DAG and history cursor, not mutable branch copies embedded in every node. |
| xoBranch creates a new branch on changes and exports flattened bracketed branch paths. | `xoDeque.py:453-486,623-650,674-744`. | Retain branching after checkout; reject bracket syntax as canonical paths. Compatibility parses it at the edge. |
| Global class-level root/state leaked between instances. | `xo.py:30-56`. | Reject. Every `XO(...)` owns or explicitly shares one root state. |
| Formula-like behavior existed: `xoFunctional` transformed/targeted changes, a functional test exercised it, and a magic formula syntax was explored. | `xo.py:2584-2668`; commits `ca092af` and `6adbac9`; `idea/ideal.wip.py:12,40,60`. | Promote the useful intent to a first-class lazy computed dependency graph; do not preserve eager hook recursion or serialize functions. |
| The recovered human canon explicitly describes XO-Svelte instant updates, FreshServer process exposure, AAA external memory manipulation, and FreshRedis assignment `xo.all = ['a.b.c', value]`. | `/tmp/xo-codex-human.json:19-32`. | Preserve these as consumers of the core event contract, not special cases inside core mutation semantics. |

The existing `ARCHITECTURE.md:25-34,62-107,131-142,207-223` identifies the right center of gravity. This report corrects four draft problems: missing reads must not mutate; child order cannot be declared non-semantic while exposing insertion order; checkout is not a magical fifth mutation; and rich transaction/history metadata must not tax every scalar set.

## 3. Canonical data model

### 3.1 Root and node records

```python
MISSING = _Missing()                 # public singleton, never encoded as null
Path = tuple[str, ...]               # () is the root

class NodeRecord:                    # internal, slots-based
    value: object = MISSING
    children: dict[str, NodeRecord]  # insertion order is semantic
    token: object                    # identity/liveness token
    value_revision: int

class RootState:
    namespace: str
    origin_id: int                   # random 128-bit value, once per process root
    revision: int                    # monotonically increasing commit revision
    lock: threading.RLock
    runtime: CapabilityRuntime          # EMPTY_RUNTIME for bare XO; shared by every child

```

The root always exists. Empty non-root nodes may exist only because an explicit `ensure()` or `clear_value()` created one; fluent reads alone never do. Child insertion order is observable through iteration and therefore appears in snapshots and content hashes. The draft's “ordering does not affect identity” claim is rejected: ignoring an observable order would make round-trip restore lossy.

A `Node` reference caches its canonical path. Once it resolves an attached record it also binds that record's token. Deleting/replacing that record marks the token detached. A subsequent value read, traversal, or mutation through the old reference raises `StaleNode`; `node.path` and `node.exists` remain safe diagnostics. A virtual reference that has never resolved may bind a later-created record. The root reference survives restore; a whole-tree swap stales every resolved descendant reference.

### 3.2 Values and ownership

`MISSING`, `None`, and an empty container are distinct. Assignment always sets **one node value**, regardless of the Python type; it never guesses that a mapping should become descendants. Nested import is explicit (`update_tree`/`restore`). This removes the legacy dict-or-value ambiguity.

XO observes assignments, not arbitrary in-place mutation inside a stored Python object. Code needing tracked container edits must compute and reassign (`node.update_value(fn)`) rather than mutate a retrieved list/dict behind XO's back. This is a deliberate performance boundary: transparent tracked wrappers or deep-copy-on-every-read would regress the hot path and still miss custom objects.

Two value policies are explicit:

- **portable (default when history/persistence/sync is enabled):** values must pass the registered safe codec before commit;
- **local_objects (explicit):** arbitrary Python objects are opaque references. They may be observed locally, but deterministic snapshot, durable history, strict backend attachment, or export fails with `CodecError`. No fallback to pickle exists.

Formula callables and RPC functions are configuration registries, never node values in the portable core.

### 3.3 Path rules

1. Storage identity is only `tuple[str, ...]`; `()` addresses the root value.
2. Segments are non-empty Unicode strings, bounded by configured path depth/byte limits, and contain neither NUL nor `.`.
3. `Path.parse("a.b.c")` is the sole dotted parser. Empty components, leading/trailing dots, brackets, numeric-index coercion, and escaping guesses raise `InvalidPath`.
4. `node["a.b"]` and `node.at("a.b")` deliberately invoke dotted parsing; `node.at(("a", "b"))` is already canonical.
5. A single literal child is selected with `node.child("name")`. Because `.` is forbidden in stored keys, string parsing is lossless.
6. Attribute access is relative one-segment access only and works for valid public Python identifiers. Names beginning `_` and real API descriptors (`value`, `set`, `items`, etc.) belong to Python; colliding user keys remain reachable through item/`child` access.
7. Namespace is envelope identity, never the first path segment.
8. Legacy `/`, bracket indexes, and flattened Redis IDs are accepted only by compatibility importers, which must either map losslessly or raise.

### 3.4 Public access behavior

```python
from xo import XO, MISSING

state = XO("app", history=True)
assert state.revision == 0

probe = state.user.profile          # virtual; no node, event, or revision
assert not probe.exists
assert state.peek("user.profile") is None

state.user.name = "Tami"           # materializes ancestors; SET_VALUE
state.user.name.meta.updated_by = "voice"
assert state["user.name"].value == "Tami"
assert state.at(("user", "name")) is state["user.name"]

state.user.name = None
assert state.user.name.value is None
assert state.user.name.has_value

state.user.name.clear_value()       # descendants survive
assert state.user.name.value is MISSING
assert state.user.name.meta.exists

state.user.name.delete()            # value and descendants removed
assert state.peek("user.name") is None
```

`.value` returns `MISSING` for an attached valueless node and raises `MissingPath` for an unresolved virtual path. `get(default=None)` is the non-raising value read. `peek(path)` returns an attached `Node` or `None`, never creates. `contains_path`, `snapshot`, `repr`, `dir`, iteration, equality, and serialization are non-mutating.

An explicit set is an event even when the new value compares equal to the old value. The core never invokes arbitrary user `__eq__` to suppress writes. A caller that needs conditional/no-op behavior uses `compare_and_set` with an expected revision/value policy.

## 4. Mutation and event contract

### 4.1 The only state operations

| Operation | Payload | Result |
|---|---|---|
| `SET_VALUE` | new value | set/replace the value; preserve every child |
| `CLEAR_VALUE` | `MISSING` | remove only the value; preserve node and children |
| `DELETE_SUBTREE` | `MISSING` | detach the node, value, and all descendants |
| `RESTORE_SUBTREE` | canonical `NodeImage` | atomically replace/create a subtree, used by restore/inverse application |

Checkout is **not** another operation. It computes an atomic transaction of these primitives. Intermediate ancestor materialization is a consequence of the target path and does not emit extra events.

### 4.2 Minimal transport-neutral event

```python
@dataclass(frozen=True, slots=True)
class Event:
    event_id: int             # 192-bit (root origin_id || monotonic event sequence)
    namespace: str
    origin_id: int
    base_revision: int
    revision: int             # revision produced by the whole commit
    operation: Operation
    path: Path
    payload: object           # operation-discriminated above
    diagnostics: Diagnostics | None = None
```

These names are canonical across core and transports. `Event` has no parent-history field, schema field, transaction index/count, timestamp dictionary, delivery receipt, or wire correlation data. `parent_revision` belongs to a history `RevisionRecord`. Schema is negotiated by a codec/session and stated on snapshots. Timestamp, trace ID, and metadata live in one optional `Diagnostics`; when diagnostics are disabled, no clock call or metadata allocation occurs.

`event_id` uses the root's random origin prefix plus a monotonic counter; no entropy syscall or UUID string formatting occurs per mutation. Hex/text conversion happens only at a wire boundary. Paths are cached tuples. This is the allocation rationale behind the hot-path schema.

A commit containing two or more events allocates exactly one wrapper:

```python
@dataclass(frozen=True, slots=True)
class Transaction:
    events: tuple[Event, ...]        # non-empty, ordered, same ns/base/revision/origin

    @property
    def transaction_id(self):        # no extra ID allocation
        return self.events[0].event_id
```

Singletons are represented by `Event`, not a one-element `Transaction`. A transaction is the indivisible persistence/replication unit. A transaction that exceeds configured event/byte limits is rejected before commit rather than split. Prior values and deleted images are held only in an enabled history inverse record or an opt-in subscriber `EventView(include_previous=True)`; they do not double every event or leak by default over the wire.

A computed result uses a separate non-state `DerivedEvent(event_id, namespace, origin_id, cause_revision, path, formula_generation, status, payload, diagnostics=None)`. It never advances `revision`, enters history, or masquerades as a persisted mutation.

### 4.3 Commit ordering and root state machine

```text
IDLE
  └─ begin/lock ─> PLANNING
       ├─ invalid path/value/limit/conflict ─> IDLE (no effects)
       └─ plan + inverse + Event(s) ─> PERSISTING (strict only)
             ├─ definite reject/failure ─> IDLE (no effects)
             ├─ outcome unknown ─> RECOVERY_REQUIRED
             └─ durable receipt / local mode ─> APPLYING
                    ├─ apply all + revision + history + dirty marks ─> QUEUED
                    └─ invariant failure after durable success ─> RECOVERY_REQUIRED
  unlock ─> FORMULA_PHASE ─> DISPATCHING ─> IDLE
```

Exact order:

1. Resolve and validate canonical paths, operation limits, stale handles, formula-write rules, and codec portability.
2. Acquire the root lock; verify `expected_revision` (if supplied), build inverse material, allocate event IDs, and assign one new revision.
3. In strict mode call backend CAS with `base_revision` while holding the writer lock. A definite failure leaves local state/revision/history/event queue unchanged.
4. Apply every operation under the lock. Publish the new revision only after all operations succeed.
5. Append the history record (if enabled), invalidate formula dependents, and enqueue the commit for dispatch.
6. Release the root lock. Recompute observed dirty formulas, then dispatch base events in transaction order followed by derived events.
7. A remote apply follows the same validation/apply path, requires matching namespace/base revision, deduplicates `event_id`, and is marked already durable so it is never re-published as fresh work.

If a strict backend may have committed but the acknowledgement is lost, raise `CommitOutcomeUnknown`, enter `RECOVERY_REQUIRED`, and make ordinary reads/writes raise `RecoveryRequired`. Resolution queries/resnapshots by revision and event/transaction ID: if durable, apply that exact planned commit; if absent, retain old local state. This is distinguishable from `PersistenceError`, which means the backend definitively did not commit. A process crash after durable success is recovered from the backend snapshot/log; local RAM is never treated as authority.

### 4.4 Concurrency

- One root lock linearizes reads, singleton writes, transactions, restore, and checkout. Different roots do not share locks.
- The lock is held across strict CAS. This intentionally trades strict-mode read latency for a simple no-split-brain guarantee; local mode never performs I/O under the lock.
- Multi-root atomicity is unsupported and raises before work.
- `expected_revision` provides optimistic conflict detection. Concurrent strict processes also CAS the backend revision; no last-writer-wins or automatic merge is invented.
- Remote duplicates within the bounded recent-ID set return `DUPLICATE` without notification. An older duplicate outside the set necessarily has a stale base revision and returns `ConflictError`, never reapplies.
- User callbacks and formula functions never run while the root lock is held.

## 5. Subscriptions and reentrancy

```python
def changed(event):
    print(event.operation, event.path, event.revision)

sub = state.subscribe(changed, path=("user",), descendants=True)
with sub:                         # strong registration, deterministic removal
    state.user.name = "Tami"
```

Subscriptions are exact-path or prefix (`descendants=True`), optionally include derived events, and return an idempotent handle. Registration order is callback order. For a transaction, the entire final state is visible before its first callback; events are delivered in operation order. Subscriber lists are snapshotted per dispatch: subscribe/unsubscribe during a callback affects the next queued commit.

Synchronous dispatch is the core default. Exceptions are wrapped as `SubscriberError`, sent to the configured error hook/commit receipt, and do not stop later subscribers or roll back state. Asynchronous queues/executors are adapter concerns with explicit backpressure; the core does not silently spawn a thread.

A callback may commit recursively. The nested commit is accepted after the current callback returns and enqueued behind the currently dispatching commit, so events cannot interleave. A context-local dispatch budget (default 64 nested commits per outer drain, configurable downward) is checked **before** a recursive commit; overflow raises `ReentrantMutationError` without creating a revision. The already committed parent remains committed. Formula evaluation may not mutate its dependency tree and raises `FormulaMutationError` before commit.

The legacy `@=` form and `(value, *_args, _id=...)` callback signature live in `xo.compat`; they adapt from this event stream.

## 6. Safe snapshots and codecs

```python
snap = state.snapshot()                 # immutable Snapshot
blob = snap.to_bytes()                  # canonical UTF-8 tagged JSON
state.restore(blob, expected_revision=state.revision)
```

```text
Snapshot {
  schema: "xo.snapshot",
  version: 1,
  namespace: str,
  revision: int,             # live commit-stream revision
  head_revision: int|null,   # history cursor, if history enabled
  root: NodeImage
}
NodeImage {
  $value?: TaggedValue,      # absence means MISSING; null means None
  $children: [[key, NodeImage], ...]  # insertion order preserved
}
```

The canonical encoder supports null, bool, arbitrary-size int (tagged when needed cross-language), finite float, str, bytes, list, tuple, and string-keyed dict. Cycles/shared-reference identity, NaN/infinity, callables, modules, file/socket handles, and unknown objects fail closed with `CodecError`. Custom codecs require an explicit stable type tag and version; registration collisions fail. There is no pickle/dill fallback.

`content_hash` covers canonical root bytes, including child order but excluding namespace/revision/history cursor. `snapshot_hash` covers the full snapshot envelope. Thus equivalent content across revisions can be recognized without pretending two provenance envelopes are identical.

Restore decodes and validates the complete image off-side (schema, namespace policy, codec tags, path/depth/node/byte bounds), prepares strict persistence, then swaps/applies atomically as one commit. Decode/backend failure exposes nothing. Whole-root restore stales all resolved descendant handles. Formula definitions/caches are not serialized; existing process-local formulas remain registered and become dirty after restore.

## 7. Revision DAG, undo, redo, checkout

Core revisions remain one monotonic stream for CAS and replication. History adds an independent content cursor without weakening that property:

```python
@dataclass(frozen=True, slots=True)
class RevisionRecord:
    revision_id: int          # commit revision that originally created this content node
    parent_revision: int | None
    forward: Event | Transaction
    inverse: tuple[InverseOp, ...]
    content_hash: bytes | None
```

Normal content mutation appends a `RevisionRecord` whose parent is the current history cursor, then moves the cursor to it. `undo()` selects its parent; `redo()` selects a child; `checkout(revision_id)` selects any known node. Navigation computes the minimal state diff and commits it atomically at a **new live commit revision**, then moves the cursor. Navigation is logged separately for audit but does not create a duplicate content node. A mutation after undo creates a new `RevisionRecord` parented to the selected older node, preserving the abandoned future as another child.

`redo()` with zero children raises `HistoryError`; with more than one raises `AmbiguousRedo` and reports candidate IDs. Checkout of an unknown/pruned revision raises `RevisionNotFound`. Failure leaves both state and cursor unchanged. Unchanged node records survive a diff checkout; replaced/deleted records become stale. Checkpoints bound replay cost; pruning is explicit policy and may never remove the current cursor or data needed by retained descendants.

Durable history stores codec bytes/inverse images, not Python object references. Local-object policy permits only explicitly ephemeral history and labels it non-exportable. Formula definitions and cached results never enter the DAG; dependency changes dirty them, and observed formulas refresh after the checkout commit.

Compatibility maps `left/home` to undo/oldest-ancestor, `right/end` to redo/selected tip, `current` to the cursor, and explicit branch index selection to a reported revision ID. Legacy numeric/bracket branch positions are presentation, not stable identity.

## 8. First-class computed/derived nodes

```python
state.price = 12
state.quantity = 3
state.total.derive(lambda: state.price.value * state.quantity.value)

assert state.total.value == 36       # first read captures dependencies and caches
state.price = 20                     # marks total dirty; no formula runs if unobserved
assert state.total.value == 60       # lazy recompute

sub = state.total.subscribe(changed) # observation makes dirty refresh post-commit
state.quantity = 4                   # base commit, refresh, then DerivedEvent(value=80)
```

A formula is a sidecar `FormulaRecord(path, fn, generation, dependencies, state, cache)` owned by the attached `Formulas` capability. Registration is process-local configuration and does not advance state revision. A derived path may have children but no explicit base value; `set` at a derived path raises `DerivedWriteError` until `undefine()` is explicit. Deleting/restoring state does not serialize or implicitly destroy formula configuration.


Semantics:

1. A `contextvars` evaluation frame records every `(root, path, value_revision)` read through `.value`, including reads of other formulas. Cross-root dependencies are rejected in v1 with `CrossTreeDependencyError`.
2. After a successful evaluation, graph edges are atomically replaced by the captured set. Dependency commits transitively mark dependents `DIRTY`; dirtying is part of the base commit but is not another event/revision.
3. An unobserved dirty formula recomputes only on value read. A formula with a subscriber or exported bridge projection is recomputed in the post-commit formula phase before callbacks. Base callbacks therefore see refreshed observed caches.
4. One thread evaluates a formula generation; concurrent readers wait on its condition and share the cache/error (`single-flight`). The function runs without the root lock.
5. Evaluation is optimistic: captured dependency versions are rechecked under lock before publishing. If stale, discard and retry up to the configured bound (default 3); continued churn raises `FormulaStaleError` and leaves it dirty for a later read.
6. The context-local evaluation stack detects direct/indirect cycles and raises `FormulaCycleError` with the canonical path chain.
7. An exception becomes a cached `FormulaEvaluationError` for that formula generation and dependency-version vector. Reads re-raise without rerunning. A dependency change or formula replacement invalidates the error. A failed run keeps the union of prior and newly observed dependencies so it can recover.
8. A formula attempting to mutate its tree raises `FormulaMutationError` before any commit. Side effects outside XO are the user's responsibility and explicitly discouraged.
9. Successful dirty recomputation always emits one `DerivedEvent` when observed; it does not call arbitrary `==` merely to suppress delivery. Unobserved lazy reads emit no event.
10. Formula result codec failure affects only an exported derived projection (`CodecError`/derived error event); it cannot roll back the base mutation.
11. Snapshot/history contain base state only. No source, bytecode, closure, import path, or cached result is serialized. A receiving process must register its own formula or consume exported derived values as non-authoritative projections.

This is deliberately smaller and safer than treating formulas as general reactive effects: only value reads become dependencies, only cached values are produced, and mutations/effects are outside formula execution.

## 9. Falsifiable invariants

1. A node's value mutation never removes, replaces, or reorders its children.
2. `None` round-trips as a value; only absence of `$value`/`MISSING` means valueless.
3. Fluent reads, `peek`, membership, repr, iteration, snapshot, and equality never change node count, revision, history, or subscriptions.
4. Every internal path is a canonical tuple; no backend/bridge/history component receives a dotted path.
5. One committed transaction advances the root revision exactly once; all its events share base/revision/namespace/origin.
6. A singleton commit allocates no `Transaction`; a multi-event transaction is never partially visible or partially delivered.
7. An explicit equal-value set still commits; the core never invokes user equality to decide whether to emit.
8. A definite pre-commit/strict persistence failure changes no local record, revision, history cursor, formula cache, or subscriber queue.
9. An unknown strict outcome freezes ordinary access until authoritative reconciliation; it is never reported as a definite failure.
10. A remote event is applied at most once and is never republished as a newly authored event.
11. History and diagnostics disabled means no inverse image, timestamp call, trace map, metadata map, or revision-record allocation on the scalar-set hot path.
12. No subscriber or formula callable runs while the root lock is held.
13. All state in a transaction is final before its first callback; nested callback commits queue after the current dispatch item.
14. Subscriber failure cannot roll back state or prevent later subscribers.
15. `CLEAR_VALUE` preserves node identity/children; `DELETE_SUBTREE` stales every resolved handle in the detached subtree.
16. Snapshot bytes for the same ordered portable tree/schema/namespace/revision are identical across runs; malformed/unknown tags never partially restore.
17. Undo/redo/checkout never delete an abandoned future; mutation after checkout creates a DAG branch.
18. Live commit revision is monotonic even when the history cursor moves backward.
19. Dependency mutation only dirties a formula; unobserved formulas execute zero user code until read.
20. Computed cache publication validates all captured dependency versions; no mixed-revision result is cached.
21. Formula cycles/errors are stable cached outcomes until their dependency vector or formula generation changes.
22. No formula/RPC callable is present in a snapshot, history payload, Redis value, or wire event.
23. Portable boundaries never invoke pickle/dill or an unregistered decoder.
24. A stale handle cannot mutate a newly recreated node at the same path.

## 10. Exact failure surface

| Error | Meaning / state after error |
|---|---|
| `InvalidPath` | syntax/segment/depth/byte violation; no commit |
| `MissingPath` | strict read/delete target absent; no commit |
| `StaleNode` | resolved handle's identity token detached/replaced; obtain a fresh handle |
| `ConflictError` | expected/base revision mismatch; no automatic merge |
| `CodecError` | unsupported/unsafe/malformed value or snapshot; no fallback |
| `PersistenceError` | backend definitely rejected/did not commit; local unchanged |
| `CommitOutcomeUnknown` | durable result ambiguous; root enters recovery-required state |
| `RecoveryRequired` | access blocked until authoritative reconcile/resnapshot |
| `InvariantViolation` | impossible local apply failure, especially after durable commit; freeze and resnapshot |
| `ReentrantMutationError` | callback recursion budget exceeded before nested commit |
| `SubscriberError` | committed state remains; error hook/receipt records callback failure |
| `HistoryError` / `RevisionNotFound` / `AmbiguousRedo` | navigation failed; state/cursor unchanged |
| `DerivedWriteError` | attempted explicit value write over a formula |
| `FormulaCycleError` | cached cycle chain for current formula/dependency generation |
| `FormulaEvaluationError` | cached user-function failure; base state unaffected |
| `FormulaMutationError` | formula attempted tree mutation; nothing committed |
| `FormulaStaleError` | dependency churn exceeded optimistic retry bound; remains dirty |
| `CrossTreeDependencyError` | v1 formula attempted unsupported cross-root capture |

## 11. Proposed benchmark/allocation gates

These are release gates to measure, not claims that have already passed. Use warmed runs, at least 30 samples, pinned environment, median/p95/p99, and differential legacy characterization. Correctness gates dominate speed.

| Core surface (history/diagnostics off unless named) | Budget |
|---|---:|
| `import xo` | median ≤ 25 ms |
| create root | median ≤ 10 µs |
| existing child reference/read | median ≤ 1 µs |
| missing fluent reference | median ≤ 1.5 µs and zero state mutation |
| local scalar `SET_VALUE` including Event | median ≤ 5 µs, p99 ≤ 20 µs |
| five-segment set with cached path | median ≤ 15 µs |
| 5-operation local transaction | median ≤ 30 µs |
| clean computed read overhead (excluding value operation) | median ≤ 2 µs |
| dirty computed bookkeeping excluding user function | median ≤ 10 µs + function time |
| invalidate 1,000 dependency edges | median ≤ 1 ms |
| portable snapshot of 10,000 scalar leaves | median ≤ 20 ms; peak extra RSS ≤ 3× encoded bytes + tree image |
| history-enabled scalar set | ≤ 35% slower than the same implementation with history off |

Promotion also requires no equivalent-behavior median regression greater than 5% against the measured legacy champion unless an explicit capability exception is accepted. Allocation profiling must prove: diagnostics-off performs no diagnostics allocation/clock call; history-off performs no inverse/history allocation; singleton performs no transaction tuple/wrapper allocation; path tuples are reused by resolved handles; formula graph adds no per-node object until a formula exists. If the 5 µs set budget conflicts with globally unique event IDs, use the root-prefix-plus-counter integer representation above—never drop IDs or defer semantic event creation.

## 12. Contract tests required before Redis/RPC/WebSocket work may build

1. **Value/children matrix:** set, overwrite, clear, delete, `None`, empty containers, child order, root value, and scalar-plus-descendant coexistence.
2. **Non-mutating inspection:** every inspection API and long fluent probe leaves exact node count/revision/snapshot hash unchanged.
3. **Path conformance:** attribute/item/dotted/tuple forms converge; invalid dots/brackets/NUL/empty/oversize/colliding API names fail exactly.
4. **Identity/staleness:** virtual bind, clear identity preservation, subtree delete/recreate, root restore, and unchanged checkout nodes.
5. **Assignment ambiguity:** mappings are values under `set`; `update_tree` is explicit and atomic.
6. **Event golden vectors:** exact field names/types, operation payloads, 192-bit ID uniqueness, singleton vs transaction allocation shape, and tuple→wire-list lossless conversion including root `()`.
7. **Transaction atomicity:** injected validation/apply failure at every operation observes all-old, never prefix-new; one revision only.
8. **Strict outcomes:** definite reject, confirmed success, lost acknowledgement with durable-found/durable-absent reconciliation, and post-durable local invariant failure.
9. **Remote apply:** duplicate, stale base, wrong namespace, reordered transaction, no echo, bounded dedupe eviction.
10. **Subscriber ordering:** exact/prefix registration, transaction event order, final-state visibility, unsubscribe/subscribe during dispatch, exception isolation.
11. **Reentrancy:** nested writes queue without interleaving; budget failure creates no revision and preserves parent commit.
12. **Codec adversary suite:** canonical vectors in Python and JS, `None` vs missing, huge ints, float edges, bytes/tuple/dict order, unknown/colliding tags, cycles, bombs, and proof no pickle path exists.
13. **Snapshot atomicity:** deterministic bytes/hashes, bounds, corrupt/truncated/foreign namespace/version, round-trip child order, and zero partial visibility.
14. **History DAG:** linear undo/redo, mutation-after-undo branch, ambiguous redo, explicit checkout, navigation failure, checkpoint/replay equality, monotonic live revisions.
15. **Formula capture:** dynamic dependencies, formula-on-formula transitive invalidation, branch-dependent dependency replacement, and no execution on unobserved dirtying.
16. **Formula safety:** direct/indirect cycles, cached errors and recovery, mutation prohibition, cross-root rejection, stale-result retries, single-flight under concurrent readers.
17. **Observed formula ordering:** base transaction applies, computed graph refreshes once, callbacks see new cache, base events precede derived events, and derived failure does not roll back base.
18. **No-code serialization:** snapshots/history/events with formulas contain neither callable/source/import path/cache; receiving process remains safe and explicit.
19. **Thread race suite:** concurrent readers/writers/transactions/formula reads under barriers; linearizability checked against a serial model.
20. **Performance/allocation suite:** every budget above plus differential legacy runs; optional features off must have measured zero hot-path tax as specified.
21. **Capability graph:** missing requirements, cycles, conflicting claims, duplicate durability, and phase inversion fail before root exposure with deterministic diagnostics.
22. **Fusion profile:** History + Formulas + strict Redis + RPC + WebSocket + validation run simultaneously over one root/revision; one set yields one history record, one durable commit, one formula invalidation, and one notification per bridge—never duplicates.
23. **Transactional reconfiguration:** inject failure at each preflight/bootstrap/swap/start/stop/handoff point; observers always see the complete old or new runtime and failed attach leaves old behavior intact.
24. **Bare-core tax:** compare compiled EMPTY_RUNTIME against capability-enabled roots; prove no capability object/list traversal, optional allocation, clock call, thread, or service in bare XO.

No persistence, RPC, or browser adapter is conformant until these tests pass against the same core implementation and consumes the golden event/snapshot vectors unchanged.

## 13. Staged implementation plan

1. **Characterize, do not port:** freeze behavior probes for the evidence rows, especially scalar-plus-children, path collisions, callback order, FreshRedis no-echo, branch navigation, and formula examples.
2. **Path + node kernel:** implement slots-based root/records/references, virtual fluent paths, `MISSING`, identity tokens, four primitive operations, and local linearizable commits. No backend or history yet.
3. **Minimal events + subscriptions:** add root-prefix event IDs, singleton/transaction split, FIFO post-lock dispatch, reentrant queue/budget, and golden vectors. Prove hot-path budgets here.
4. **Portable codec + snapshot:** tagged canonical encoder, limits, detached decode/build, atomic restore, hashes, and hostile corpus. Do not add pickle compatibility.
5. **Capability runtime:** compile specs/roles/claims into the zero-capability direct path or a frozen hook DAG; prove conflict/order checks and transactional preflight/rollback before optional behavior lands.
6. **History capability:** inverse capture/checkpoints/cursor navigation/branch-after-checkout entirely through `RECORDER` contracts.
7. **Formula capability:** context-local read capture, invalidation/derivation roles, lazy/eager observation, single-flight/version validation, cycles/error cache, derived events, and no-code proofs.
8. **Durability + service capabilities:** implement backend CAS/receipt/unknown-outcome recovery, Redis observer, RPC Service, and WebSocket against `Event|Transaction`/Snapshot and public APIs only; exercise the hybrid fusion profile.
9. **Compatibility shell:** `xoBenedict`, `FreshRedis`, `xoBranch`, `@=`, callable-node syntax, flattened/bracket import, and legacy callback arguments become profile-building proxies/adapters with differential tests—never XO subclasses or alternate cores.
10. **Layer gate:** only after all §12 contracts, capability-fusion tests, and budgets pass may transport adapters claim conformance.


## 14. Rejected alternatives

- **Subclass `dict` again:** conflates the value slot, child map, methods, and user keys; identity/staleness/transactions remain unfixable.
- **Multiple-inheritance mixins or generated XO subclasses:** capability fusion becomes MRO-dependent, conflicts surface at runtime, child type propagation is fragile, and bare XO pays inherited machinery. Compile explicit capability specs into one root runtime instead.
- **Materialize on missing read:** preserves an accidental legacy side effect at the cost of inspectability and revision truth. Virtual references retain the magic without the damage.
- **Treat assigned dict as descendants:** type-dependent mutation is ambiguous. Import/update is explicit.
- **One global singleton root:** observed leakage, impossible independent namespaces/tests.
- **Let callbacks transform/veto after notification:** confuses observer and validator. Explicit pre-commit validation is separate; subscribers are facts after commit.
- **Put old/new/history parent/tx indexes/timestamps/metadata on every Event:** avoidable hot-path allocation and wire leakage. Inverse/history, optional views, optional diagnostics, and a batch wrapper pay only when used.
- **Emit a special CHECKOUT mutation:** duplicates semantics and forces every adapter to understand history. Checkout compiles to ordinary atomic state operations.
- **Copy the legacy branch tree per write:** memory grows with repeated structure and marker identity is unstable. An append-only DAG plus checkpoints is simpler.
- **CRDT/LWW merge in v1:** silently invents conflict semantics. Revision CAS and explicit resync are honest; domain-specific merge can be layered later.
- **Persist formula source/bytecode/import paths:** executable supply-chain risk and non-reproducible closures. Formula code is local configuration; derived values are projections.
- **Run formulas eagerly on every mutation:** recreates xoFunctional's recursive/eager cost. Dirty invalidation plus observed-only eager refresh retains immediacy where somebody can see it.
- **Deep-copy/wrap every value to intercept in-place edits:** allocation/performance regression and incomplete custom-object semantics. XO's observable boundary is assignment.
- **Unsafe pickle/dill compatibility in core:** remote-code execution and non-portable snapshots. Legacy imports must run in an explicitly isolated migration tool, never runtime decode.

## 15. Unresolved decisions

None at the semantic boundary. Limits (path bytes/depth, transaction bytes, history checkpoint cadence, subscriber recursion budget) are configuration defaults to tune from measurement, not architecture ambiguities. The only intentionally deferred capability is cross-root computed dependencies; v1 rejects them explicitly rather than guessing at distributed invalidation.
