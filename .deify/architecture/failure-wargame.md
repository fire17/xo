# XO Failure Wartable

**Date:** 2026-08-30  
**Status:** architecture evidence and proposed decisions, not implementation authority. Core vocabulary follows `core-semantics.md`; wire vocabulary follows `network-protocols.md`.

## Verdict

**CONDITIONAL NO-GO.** The current `ARCHITECTURE.md` has the right center—value-plus-children, one path and mutation model, strict persistence, safe codecs, bounded optional layers, allow-listed RPC, and source-observed compatibility—but is not implementation-ready at its failure seams. Before implementation, the integrated architecture must pin:

1. ambiguous durable commit outcomes and recovery-only mode;
2. epoch/fencing plus contiguous snapshot/live reconciliation;
3. post-commit callback scheduling and stale handles;
4. conflict behavior for divergent browser peers;
5. cleanup-before-terminal streaming and process lifecycle;
6. lazy formula freshness without executable serialization;
7. **arbitrary capability fusion without subclass inheritance**, including collision, ordering, attach rollback, and shared lifecycle;
8. semantic-first migration and benchmark gates.

This is not a request for a framework. Harden one primitive. Formula code and capability implementation objects are local process configuration: **never state, never pickle, never wire data**.

---

## 1. Observed legacy facts

| Observed fact | Exact evidence | Confidence | Architectural consequence |
|---|---|---:|---|
| A node preserves a scalar value alongside descendants. | `xo-benedict/VISION.MD:4-9,105-120` | High | Value slot and child map must be independent. |
| Missing attribute access creates a child. | `xo-benedict/xo.py:1442-1462` | High | Preserve fluent access; make inspection/repr/snapshot non-creating. |
| Subscribers and `__onchange__` execute in setter flow; callback exceptions print and continue. | `xo-benedict/xo.py:520-578,719-744,1494-1513` | High | New callbacks run only after accepted commit and root-lock release. |
| Redis set and publish are separate, use dill/pickle, and suppress echoes through connection hashes/skip flags. | `xo-benedict/xo.py:1984-2024,2127-2161,2208-2305,2525-2537` | High | Use one authoritative CAS event, safe codec, durable IDs, contiguous replay. |
| Redis construction starts an uncaptured daemon listener without a close/join contract. | `xo-benedict/xo.py:2182-2200,2372-2435` | High | Explicit attach/start, owned resources, bounded truthful close, fork reset. |
| History is implemented as inheritance layered over core/Redis behavior and mutable branch indices appear in paths. | `xo-benedict/xoDeque.py:13-34,118-188,453-505,599-617,764-790` | High | History must be a capability over the same mutation engine, not a competing subclass mutation path. |
| Branch naming repeatedly required fixes. | Repository commits `a09c1b5`, `1e7b8d3`, `9ea4639`, `a81eb05` from `git log -- xo.py xoDeque.py` | High | Revision identity must be immutable and independent of display/path position. |
| Decorator-shaped service paths and dynamic client proxies are demonstrated behavior. | `xo-benedict/freshServer.py:21-43,117-141`; `freshClient.py:6-24`; `/tmp/xo-codex-human.json:20-32` | High | Preserve syntax over an explicit registry; never arbitrary attribute traversal. |
| Legacy RPC unpickles input, kills occupied ports, silently retries, and hides failures. | `xo-benedict/freshServer.py:5-18,36-100`; `xoServer.py:21-30,169-195` | High | Framed tagged JSON, typed errors, bind failure, deadlines, no silent retry. |
| Embedded streaming uses process-global unbounded deques and spin-waits with a timeout TODO. | `magicllight/.../xo_benedict/freshServer.py:26-34,89-123,137-157`; client `freshClient.py:40-59` | High | Credit window, cancellation, resource ownership, one terminal per request. |
| MagicLLight relies on embedded imports, `_inc` port arithmetic, discovery, and generator iteration. | `magicllight/core/airouter/front_runner.py:9-10,73-89`; `fusion_server.py:12-30,34-47,64-73` | High | Migration must exercise real workflows, not class aliases alone. |
| Browser demo allows all CORS, shares a last session, sends HTML, and demonstrates remote eval. | `xo-benedict/JS.py:51-60,68-75,105-164`; `svelte_appB.py:6-8,195-197`; `freshSvelt/src/xo.js:71-185` | High | Per-peer identity/prefix authorization; no HTML/eval semantics in XO. |
| Packaging is inconsistent/heavy and cold start was known debt. | `xo-benedict/xo.py:1-17,1589`; `pyproject.toml:5-7,91-149,177-181`; embedded `pyproject.toml:1-14`; `VISION.MD:173-174` | High | Stdlib-only core; clean sdist/wheel/cold-import gates. |
| `xoFunctional` executes root-shared callables from `__onchange__` and writes target paths using recursion skip flags. | `xo-benedict/xo.py:2584-2653`; embedded `xo.py:2587-2656` | High | Recover formulas as a dependency-graph capability, never callback recursion. |
| Formula syntax was explored by source/operator transformation. | `xo-benedict/idea/ideal.wip.py:7-21,38-48,52-76` | High | Stable API is explicit formula registration; `$=` rewriting is not core. |
| Human canon prioritizes blazing speed, Redis autosave/realtime, low-level RPC, exposed functions, and Python↔JS realtime sync. | `origins.md:5-12,24-41,79-149` | High | Safety mechanisms must remain composable and preserve the fast local path. |

---

## 2. Architecture decisions

### 2.1 Durable outcome is three-state, not Boolean

- `PersistenceError` means **definitely not committed**; local state and revision remain unchanged.
- A transport failure after Redis may have accepted the CAS raises `CommitOutcomeUnknown`, puts the root into `RECOVERY_REQUIRED`, and makes ordinary access raise `RecoveryRequired` until event/Transaction ID plus revision reconcile or a snapshot is installed.
- Failure to apply locally after confirmed durable commit is `InvariantViolation` and enters the same recovery-only state.
- Never retry an unknown outcome using a new ID.

### 2.2 Order is revision-contiguous

Event-ID dedupe is an optimization, not ordering proof. A replica applies only `revision == local_revision + 1`. A lower revision is stale/duplicate; a higher revision produces `xo.resync_required`. Namespace epoch/incarnation fences different writable lineages. Same `(namespace, epoch, revision)` with different canonical hashes freezes writers; XO never resolves it by last-writer-wins.

### 2.3 Callback recursion is scheduled, not nested

Commit pipeline:

```text
validate/build transaction
→ strict backend CAS
→ atomic local apply
→ history receipt
→ release root lock
→ FIFO callback wave
```

A callback mutation enters the next wave. Causal depth/count is bounded. Overflow rejects only the child attempt with `ReentrantMutationError`; the accepted parent remains committed.

### 2.4 Capability fusion replaces inheritance

History, Redis, RPC server/client bridge, browser/JS bridge, formulas, and future Pydantic integration are capabilities attached to one root/mutation engine. No `RedisXO(BranchXO(ServerXO(...)))`, no MRO-based semantics, and no capability may override a core mutation method.

```python
state = XO.build(
    capabilities=[
        History(),
        RedisSync(url="redis://127.0.0.1/0", namespace="demo"),
        FormulaGraph(),
        RPCServer("unix:///tmp/demo.xo"),
        WebSocketBridge(prefix=("ui",)),
        # Future: PydanticValidation(Model)
    ]
)

# Equivalent staged construction; attach is transactional:
state = XO()
state.attach(History())
state.attach(FormulaGraph())
state.attach(RedisSync(...))
state.start()
state.close()
```

Every capability declares immutable metadata before attach:

```python
CapabilitySpec(
    id="redis-sync",
    version=1,
    requires={"events", "codec"},
    provides={"strict-persistence", "replication"},
    conflicts={...},
    before={...},
    after={...},
    hooks={Hook.AFTER_COMMIT: priority},
    resources={"redis-connection", "listener-thread"},
)
```

Build/attach performs a deterministic topological plan and fails **before mutation or resource publication** for duplicate IDs, unsatisfied requirements, cycles, incompatible protocol/schema/codec policies, multiple strict persistence authorities, competing owners of the same endpoint/resource, or hook slots that require exclusive ownership. Tie-breaking is never import order; equal-priority unordered handlers use stable capability ID only where order is provably semantically irrelevant, otherwise attachment fails as ambiguous.

Attach is prepare/commit/rollback:

1. validate the entire prospective graph and configuration;
2. prepare resources privately without subscribing/publishing/mutating state;
3. atomically install the immutable capability plan;
4. activate in topological order;
5. on any failure, deactivate activated additions in reverse order, release every prepared resource, restore the prior plan, revision, and observable behavior.

Detach is refused while dependents exist or work/resources are active unless the capability supports a bounded drain. `close()` is idempotent, closes in reverse dependency order, and truthfully reports failure. A fork invalidates all process-owned capability resources; the child must rebuild/reattach them. A capability can observe/validate/derive/transport core events only through declared hooks; it cannot add a second state authority.

**Why this pays rent:** it permits exactly the arbitrary combinations the human wants while keeping one mutation law. Future Pydantic support becomes a pre-commit validator capability rather than a new XO class hierarchy.

### 2.5 Formula behavior follows core vocabulary

`FormulaGraph` owns `FormulaRecord` objects. Base commit marks dependencies dirty only. Lazy unobserved reads and observed post-commit refresh use context-variable dependency capture, single-flight evaluation, and version-vector validation. Successful materialization emits `DerivedEvent`, not a base state revision. No formula code or cache is serialized.

Exact errors are `DerivedWriteError`, `FormulaCycleError`, `FormulaEvaluationError`, `FormulaMutationError`, `FormulaStaleError`, and `CrossTreeDependencyError`.

### 2.6 Stream terminal law

Every request ID receives exactly one `kind="end"` with reason `complete`, `cancelled`, `deadline_exceeded`, or `error`. Cancel/disconnect/deadline closes the producer and runs cleanup **before** terminal. Request IDs are never reused per connection. Late chunks for retired/unknown IDs are dropped and counted.

### 2.7 Exact wire failure vocabulary

`xo.protocol.version`, `xo.protocol.malformed`, `xo.protocol.frame_too_large`, `xo.auth.required`, `xo.auth.invalid`, `xo.path.invalid`, `xo.codec.unsupported_type`, `xo.not_found`, `xo.deadline_exceeded`, `xo.cancelled`, `xo.backpressure`, `xo.conflict`, `xo.resync_required`, `xo.limit.concurrency`, `xo.internal`.

---

## 3. Rejected alternatives

| Alternative | Rejection |
|---|---|
| Local apply then asynchronous strict persistence | Leaks accepted effects that cannot be rolled back. Only an explicitly named volatile mode may do this. |
| Treat timeout as non-commit and retry with new ID | Can double-commit after a lost reply. |
| Last-writer-wins or automatic offline rebase | Hides data loss; application/user must resolve rejected intent. |
| Unbounded reorder, dedupe, stream, or formula queues | Turns disorder into memory exhaustion. Bound and refuse/resync. |
| Callbacks under a reentrant lock | Deadlock, recursion, tail latency, partial-observation hazard. |
| Capability behavior through subclass/MRO order | Combination semantics depend on inheritance spelling and can silently override mutation laws. |
| “Middleware list” ordered by attach/import order | Order becomes accidental and changes across consumers. Declare dependencies/order or reject ambiguity. |
| Attach partially, then leave successful pieces when one fails | Creates unknowable half-capability state. Attach is transactional. |
| Serialize formula callables/source/bytecode/import paths | Remote execution and non-portable state. Never. |
| Eagerly recompute all formulas on every mutation | Diamond duplication and invalidation storms destroy the local-set budget. |
| `$=` source rewriting as core API | Invalid Python/tooling ambiguity; explicit API pays its rent. |
| Token-only authentication over plaintext non-loopback TCP | Authentication without confidentiality leaks token and data. |
| Warm-median-only benchmark | Rewards cache priming, omitted semantics, hidden tails, and cold-start regressions. |

---

## 4. Twelve-move wargame

| Move | Half-success that lies | Prevention / recovery |
|---:|---|---|
| 1 Path parse | Dotted key resolves differently by surface. | Canonical tuple `Path`; one parser and cross-surface corpus. |
| 2 Capability plan | History+Redis+formula “works” only in one attach order. | Validate a deterministic dependency graph; reject collisions/cycles before attach. |
| 3 Mutation build | Local value passes, attached codec/validator rejects later. | Run all declared pre-commit validation against one candidate Transaction. |
| 4 Redis CAS | Commit succeeds but reply vanishes. | `CommitOutcomeUnknown` → `RECOVERY_REQUIRED`; reconcile same ID/resnapshot. |
| 5 Local apply | Redis advances while process remains stale. | Prebuild apply; local failure is `InvariantViolation` plus freeze. |
| 6 History/callbacks | History or recursive observer mutates commit semantics. | Receipts derive from accepted event; post-lock FIFO waves. |
| 7 Formula | Mutation computes the entire dependency graph. | Dirty-only invalidation; lazy/observed bounded refresh; single-flight. |
| 8 Publish | Duplicate/out-of-order event looks accepted. | Contiguous revision plus ID/body checks; gaps resync. |
| 9 Remote mutation | Unauthorized prefix/schema partly applies. | Parse/authorize/build complete Transaction off-side. |
| 10 Stream | Client disappears while producer continues. | Connection owns request; credits; cleanup before one terminal. |
| 11 Reconnect | Snapshot races a live event and loses the seam. | Establish bounded stream >R, snapshot at R, atomic install/drain. |
| 12 Upgrade/detach | Schema upgrade or capability removal leaves mixed semantics. | Writer fence and transactional migration/attach/detach rollback. |

---

## 5. Addressed scenario register

Every row includes prevention, detection, recovery, and a destructive gate.

| # | Pri | Scenario | Prevention | Detection | Recovery / changed gate |
|---:|:---:|---|---|---|---|
| 1 | P0 | Scalar set destroys descendants. | `set_value` touches value slot only. | Snapshot/handle identity. | Restore prior Transaction; D1. |
| 2 | P0 | Clear value aliases subtree delete. | Exact four primitive ops. | Event operation assertion. | Reject ambiguous compatibility call; D2. |
| 3 | P0 | Detached handle mutates ghost tree. | Generation invalidation. | `StaleNode`. | Explicit re-resolve from root; D3. |
| 4 | P1 | Repr/IDE/serializer grows fluent tree. | Non-creating inspection. | Node/revision/event counts. | Remove imported empty artifacts; D4. |
| 5 | P0 | User key collides with method/internal/JS prototype. | Reserved attribute API; arbitrary item/Path segments. | Collision corpus Python+JS. | Migrate access syntax, never key; D5. |
| 6 | P0 | Dot/slash/bracket/Unicode paths diverge. | Tuple Path and one binary-safe encoder. | Cross-surface property tests. | Exact path error; D6. |
| 7 | P0 | Recursive subscriber deadlocks/storms. | Post-lock next-wave writes. | Causal depth/count. | Reject child, keep parent; D7. |
| 8 | P1 | Slow/raising subscriber stalls/hides. | Explicit synchronous/queued mode, latency budget, isolation. | Callback latency/error counters. | Owned unsubscribe/degraded health; D8. |
| 9 | P0 | Redis definitely fails before commit. | Prevalidate, finite CAS timeout, no apply. | Definite rejection. | `PersistenceError`; zero local change; D9. |
| 10 | P0 | Redis accepts but reply is lost. | Immutable event/Transaction ID and three-state outcome. | EOF/timeout after possible send. | `CommitOutcomeUnknown`, freeze, prove/resnapshot; D10. |
| 11 | P0 | Crash after CAS before local apply. | Durable event is authoritative. | Restart behind Redis. | Catch up before access; D11. |
| 12 | P0 | Confirmed durable commit then local apply fails. | Prebuild/infallible atomic swap. | Apply exception. | `InvariantViolation`, freeze/recover; D12. |
| 13 | P0 | Crash after apply before history/notify. | Derived receipts and watermarks. | Receipt lag. | Rebuild/replay by immutable ID; D13. |
| 14 | P0 | Two Redis primaries accept same base. | One lineage plus epoch/fence. | Same revision/different hash. | Freeze, preserve both, choose authority explicitly; D14. |
| 15 | P0 | Exact duplicate applies twice. | Revision floor + bounded ID/body cache. | Duplicate counter. | Drop/ack; D15. |
| 16 | P0 | Same ID arrives with different body. | Canonical body hash bound to ID. | ID/hash mismatch. | Freeze/security escalation; D15. |
| 17 | P0 | R+2 arrives before R+1. | Apply only `local+1`. | Revision gap. | `xo.resync_required`; D16. |
| 18 | P0 | Old event replays after ID-cache eviction. | Revision watermark is authoritative. | `revision <= local`. | Drop/count; contradictory hash resyncs; D17. |
| 19 | P0 | Namespace encoding collides. | Length-prefixed/binary-safe ID plus metadata. | Namespace/epoch mismatch. | Refuse attach/migrate fresh; D18. |
| 20 | P0 | Schema migration interleaves old writer. | Lease/fence, copy+validate, atomic pointer/CAS swap. | Version/watermark mismatch. | Abort/forward-fix from immutable snapshot; D19. |
| 21 | P1 | Codec disappears or changes. | Stable codec ID/version; retain tagged raw; no fallback. | Codec error. | Quarantine/trusted codec install; D20. |
| 22 | P0 | Offline browser overwrites newer peer. | `base_revision`; no auto-rebase. | `xo.conflict`. | Resnapshot; retain rejected intent; D21. |
| 23 | P0 | Snapshot/live seam loses or duplicates event. | Start bounded >R stream before snapshot R; atomic drain. | Gap/hash mismatch. | `xo.resync_required`; D22. |
| 24 | P0 | Malformed/oversized/deep input exhausts or partly applies. | Preallocation cap, strict parser, depth/node/path limits, reject duplicate keys/NaN. | Exact wire error. | Typed refusal/close; zero mutation; D23. |
| 25 | P0 | Client reaches hidden attributes or injects code. | Allow-listed registry; state never endpoint; no code input. | Not-found/auth/path/codec error. | Close/revoke repeated attempts; D24. |
| 26 | P0 | Token crosses plaintext non-loopback link. | Local/Unix default; approved confidentiality or refuse bind. | Startup address check. | Fail closed; security gate. |
| 27 | P0 | Stream consumer stops and queue grows. | Credit window and hard queue bound. | Credit exhaustion. | Backpressure, close producer, one terminal; D25. |
| 28 | P0 | Cancel/deadline/disconnect leaks generator. | Connection owns request; close before terminal. | Active request after loss. | Terminal after cleanup; discard late; D26. |
| 29 | P1 | Zero/two stream terminals or ID reuse aliases. | Monotonic non-reused IDs and terminal state machine. | Terminal/late metrics. | Retire connection; D27. |
| 30 | P1 | Listener thread blocks shutdown forever. | Owned non-daemon, socket wake, bounded join. | Alive after deadline. | Close-failed, hard descriptor close, no reuse; D28. |
| 31 | P0 | Fork child reuses parent's sockets/origin/thread state. | PID ownership and at-fork reset. | PID mismatch. | Close inherited descriptors, new origin, reconnect; D29. |
| 32 | P1 | Import connects, parses argv, or starts thread/client. | Definitions only; lazy optional modules; no global roots. | Cold side-effect trap. | Packaging blocker; D30. |
| 33 | P0 | Wheel/sdist omits JS/modules or shadows legacy. | `src/` layout, explicit package data, clean artifacts. | Manifest, `xo.__file__`, checksums. | Block release; D31. |
| 34 | P0 | Compatibility restores singleton/killport/pickle/eval/silent retry. | Map intent only; reject unsafe mechanics. | Consumer characterization matrix. | Keep consumer pinned; never weaken core; D32. |
| 35 | P0 | MagicLLight fork remains second runtime authority. | Unified imports; remove executable fork after parity. | Provenance and real workflow. | Roll back cutover, not duplicate runtime; D33. |
| 36 | P1 | `_inc` arithmetic reaches wrong service. | Explicit endpoint URI and `describe` identity/epoch. | Identity mismatch. | Refuse and repair endpoint map; D34. |
| 37 | P0 | Benchmark wins by omitting semantics. | Semantic checksum before timing. | State/revision/event/history mismatch. | Result invalid, not faster; D35. |
| 38 | P0 | Dynamic formula dependencies become stale/cyclic. | Context capture, atomic edge replacement, compute-stack cycle detection. | Exact dependency/cycle path. | Exact formula error/dirty retry; D36-D37. |
| 39 | P0 | Formula diamond/invalidation storm duplicates work. | Version-vector single-flight, dirty-only coalescing, bounded observed refresh. | Compute count/queue depth. | `FormulaStaleError` or bounded refusal; D38-D39. |
| 40 | P0 | Formula raises/mutates/sources move/code-less peer trusts cache. | Immutable view, mutation guard, version validation, no code/cache serialization. | Formula status/fingerprint/vector. | Exact errors; trusted local recompute; D40-D41. |
| 41 | P0 | Capability IDs/hooks/resources collide. | Validate complete `CapabilitySpec` graph before prepare. | Duplicate/exclusive-provider/resource collision. | Fail build/attach with prior root untouched; D42. |
| 42 | P0 | Capability behavior changes with list/import order. | Explicit dependency/order graph; reject semantically ambiguous ties. | Permutation differential hash/trace. | Reject plan or pin declared edge; D43. |
| 43 | P0 | Capability attach fails after opening resources/subscribing. | Private prepare then atomic plan install/activate; reverse rollback. | Fault at every attach seam. | Restore exact prior plan/state/resources; D44. |
| 44 | P0 | Detach/close removes provider before dependent or leaks work. | Dependency-aware drain; reverse-order idempotent close. | Active dependents/resources after operation. | Refuse detach or report close-failed; D45. |
| 45 | P0 | Combined history+Redis+server+JS+formula yields duplicate events/loops. | One core commit pipeline; declared hooks; remote/derived events never republished as new base work. | Event/Transaction causal trace. | Freeze on duplicate authority; capability-combination gate; D46. |
| 46 | P0 | Future validator capability partly changes semantics or conflicts with codec/formula. | Pre-commit validation-only contract, declared requirements/conflicts, no mutation. | Candidate Transaction differs before/after validation or ordering permutation. | Fail attach/build; no compatibility shim; D47. |


## 6. Prioritized risk register

Likelihood and impact use 1–5. Evidence cites observed legacy behavior or a concrete omission in the current draft; confidence describes the evidence, not future frequency.

| Rank | Risk | L | I | Evidence / confidence | Leading signal | Pre-approved response |
|---:|---|---:|---:|---|---|---|
| 1 | Ambiguous Redis acceptance creates duplicate or divergent truth. | 4 | 5 | Legacy persistence separates set/publish and hides retries: `xo-benedict/xo.py:1984-2024`; `xoServer.py:169-195`. High. | Timeout/EOF after send; Redis ahead of local. | `CommitOutcomeUnknown` → recovery-only mode; reconcile same ID or resnapshot. |
| 2 | Capability fusion becomes attach-order/MRO by another name or partially attaches. | 4 | 5 | Legacy features are inheritance/global-state layers: `xoDeque.py:13-34`; `freshServer.py:21-43`; current draft has optional layers but no collision/order/rollback protocol. High. | Permutations change hook trace/hash; resource remains after failed attach. | Reject invalid/ambiguous graph before prepare; transactional prepare/activate/reverse rollback. |
| 3 | Snapshot/live seam silently loses an event. | 4 | 5 | Legacy Pub/Sub has no replay: `xo-benedict/xo.py:2182-2305`; draft promises catch-up without pinning the seam. High. | Gap or same revision/different hash. | Contiguous catch-up or watermark snapshot; `xo.resync_required`. |
| 4 | Formula callback recursion publishes stale derived truth or storms. | 4 | 5 | `xoFunctional` computes inside `__onchange__`: `xo-benedict/xo.py:2613-2651`. High. | Duplicate flights, repeated same-vector compute, CPU/queue spike. | Dirty-only `FormulaGraph`, version-vector single-flight, exact formula errors. |
| 5 | Stream leaks producers and memory on disconnect/backpressure. | 4 | 4 | Global unbounded deques/spin wait: embedded `freshServer.py:26-34,89-157`. High. | Queue growth, orphan request/generator, missing terminal. | Credit bound, cleanup-before-one-terminal, connection retirement. |
| 6 | Recursive/slow callbacks deadlock or couple commit latency. | 4 | 4 | Setter-path callbacks: `xo-benedict/xo.py:520-578,1494-1513`. High. | Wave-depth/latency/error counters. | Post-lock FIFO waves; reject only overflowing child; owned unsubscribe. |
| 7 | Path or namespace ambiguity corrupts routing/history. | 3 | 5 | Dot/bracket rewriting plus repeated branch naming fixes: `xoDeque.py:118-188,599-617`; commits `1e7b8d3`, `9ea4639`, `a81eb05`. High. | Cross-surface fixture mismatch. | Tuple Path, binary-safe encoder, exact rejection. |
| 8 | Malformed/untrusted clients execute code or exhaust resources. | 3 | 5 | Dill/pickle and permissive browser/eval evidence: `freshServer.py:5-18`; `JS.py:51-60,105-164`. High. | Protocol/auth/limit refusal counters. | Strict framed tags, hard bounds, allow-list/prefix authorization, no code. |
| 9 | Consumer migration leaves a second authority or breaks real streaming/discovery. | 4 | 4 | Direct embedded imports, `_inc`, generators: `front_runner.py:9-10,73-89`; `fusion_server.py:12-30,34-47,64-73`. High. | Wrong import provenance or workflow diff. | Real-flow compatibility gate; clean cutover; rollback consumer, not core. |
| 10 | Schema/codec upgrade mixes writers or makes durable data unreadable. | 3 | 5 | Legacy payloads are executable pickle and current draft lacks a fenced migration procedure. High. | Schema/codec/watermark mismatch. | Lease and writer fence; copy/validate; atomic swap; no codec fallback. |
| 11 | Redis failover creates two valid-looking lineages. | 2 | 5 | Current CAS proposal has no observed epoch/fencing implementation; topology risk is architectural. Medium-high. | Same epoch/revision, different canonical hash. | Freeze, preserve both, explicit authority selection, rotate epoch. |
| 12 | Shutdown/fork/import lifecycle leaks or duplicates work. | 3 | 4 | Daemon/global threads and import-time clients: `xo.py:2182-2200,2372-2435`; `freshServer.py:21-43`. High. | PID mismatch, live owned resource after close, cold-import side effect. | At-fork invalidation, explicit rebuild, bounded truthful reverse close. |
| 13 | Benchmark rewards omitted semantics, warm caches, or excluded teardown. | 4 | 3 | Legacy import/dependency cost is visible: `xo.py:1-17,1589`; cold-start debt `VISION.MD:173-174`; draft budgets are unmeasured targets. High. | Speedup with changed semantic checksum or hook trace. | Invalidate result; measure equivalent cold/tails/memory/teardown/full stack. |

---

## 7. Symptom-keyed recovery playbooks

### `CommitOutcomeUnknown` / local unchanged but Redis may be ahead

1. Enter `RECOVERY_REQUIRED`; block ordinary access and all new writes.
2. Inspect attempted immutable ID, base revision, Redis epoch/revision.
3. Exact ID/body present: install through its revision and report committed.
4. Proven absent while Redis remains at base: retry the **same** immutable ID.
5. Otherwise install a verified snapshot. Same ID/different body or same revision/different hash escalates immediately.

### `xo.conflict`

Do not blind-retry or auto-rebase. Compare base/current revision and epoch. Catch up if retention is contiguous; otherwise resync. Rebuild intent only in the application layer and retain rejected browser intent.

### `xo.resync_required` or R→R+n

Quarantine R+n. Request contiguous R+1. If unavailable or hash differs, watermark-install a full snapshot. Resume only after authoritative revision/hash match. More than three resyncs in 60 seconds for one peer escalates.

### Same revision/different hash or unexpected epoch

Freeze writers and bridges. Preserve both lineages read-only with epoch/revision/hash/recent headers. Require explicit authority selection or application-specific export/merge. Rebootstrap peers and rotate epoch. Never LWW.

### Recursive callback / `ReentrantMutationError`

The parent remains committed. Identify owned subscription and causal IDs; unsubscribe offender; fix convergence/origin idempotence; explicitly replay desired child intent. A second limit breach escalates; do not raise the bound first.

### Formula failure

1. Inspect `FormulaRecord`: ID, dirty status, flight owner, captured dependency version vector, last `DerivedEvent`/error.
2. Cycle: return exact `FormulaCycleError`; never remove an arbitrary edge or serve fresh.
3. Source changed during compute: discard and retry once under the new vector; continued churn returns `FormulaStaleError`.
4. More than one flight for one formula/vector is an invariant failure: disable observed refresh and escalate.
5. Mutation during evaluation returns `FormulaMutationError`; no base mutation occurs.
6. Cross-tree dependency returns `CrossTreeDependencyError` unless a future design defines one shared revision domain.
7. Recover only by trusted local rebind/invalidate/recompute—never deserialize code/cache.

### Capability build/attach collision or partial failure

1. Do not reorder until it “works.” Capture the full prospective `CapabilitySpec` graph and exact collision/cycle/unsatisfied requirement.
2. Verify the previous immutable plan, revision, hooks, subscriptions, threads, sockets, and endpoints are unchanged.
3. If prepare/activation began, run reverse rollback and prove zero owned resources remain.
4. Resolve by changing explicit requirements/conflicts/resource ownership or removing an incompatible capability; never use attach order as policy.
5. Any partial state, leaked resource, emitted event, revision change, or nondeterministic permutation is `InvariantViolation` and release-blocking.

### Capability close/detach failure

Refuse detach while dependents or active work exist unless drain is supported. Drain within a pinned deadline; close in reverse dependency order; report close-failed if resources remain. Do not silently orphan dependents, switch threads to daemon, or claim closed.

### `xo.backpressure` or missing stream terminal

Stop the request. Server stops pulling, closes generator/runs `finally`, then emits one terminal. Retire ID; late chunks drop/count. Missing terminal after deadline closes the connection and cleans all owned requests. Any orphan, bound breach, zero/two terminals, or surfaced late chunk blocks release.

### Close returns but resource remains

Mark close failed. Set stop and shutdown/close the owning descriptor to wake reads; bounded join; capture remaining status; refuse object reuse. Never convert to daemon as a fix.

### Child process duplicates after fork

Reject on PID mismatch before I/O. Child closes inherited descriptors without sending frames, clears ownership, mints a new origin, and explicitly rebuilds process-owned capabilities. If it already wrote, freeze/resync and audit duplicates.

### Browser differs from Python/Redis

Compare namespace/epoch/schema/revision/hash, not clocks. Stop browser writes and preserve rejected intent. Gap uses resync; same revision/different hash uses split-brain playbook. Verify prefix authorization and absence of HTML/eval interpretation before reconnect.

### Clean install wrong, missing, or slow

Discard editable-install evidence. Install sdist and wheel separately; record manifest and `xo.__file__`; cold-import with arbitrary argv and network/thread/process traps; compare API/behavior/JS checksum; block on difference.

### Benchmark suddenly improves

Assume reward hacking until disproven. Compare final snapshot, revisions, event bodies, subscriber order/count, history, codec, formulas, and capability hook trace. Then measure cold and warm p50/p95/p99, allocations/RSS, CPU, and teardown on a pinned environment. Semantic/timer mismatch invalidates the result.

---

## 8. Invariants and escalation thresholds

1. Value slot and children are independent; set/clear never deletes descendants.
2. Every surface uses the same tuple Path and encoder.
3. Accepted transitions are immutable Events; atomic multi-op uses Transaction.
4. Strict visibility follows a known accepted CAS. Unknown durable outcome means `RECOVERY_REQUIRED` and access refusal.
5. One namespace/epoch has one contiguous revision lineage; same revision/different hash is split brain.
6. Revision/epoch/hash establish correctness; bounded ID cache optimizes duplicate handling.
7. Callbacks execute after commit and lock release in FIFO waves; their failure never rolls back state.
8. Delete/replace invalidates old handle generations; stale access raises `StaleNode`.
9. Restore validates/builds off-side and atomically swaps as one revision.
10. Network boundaries never unpickle, eval, import, traverse Python attributes, or accept code.
11. Frames, paths, snapshots, queues, streams, formula work/graph, and concurrency have hard limits and named refusals.
12. Each request ID has one terminal; cleanup precedes it; IDs are not reused per connection.
13. Threads/sockets/streams have an owner, bounded truthful close, and fork reset.
14. Formulas are local-code-only, dirty/lazy, dependency-tracked, single-flight, version-validated; `DerivedEvent` is not a base revision.
15. A root has one immutable validated capability plan at a time; capabilities cannot override core mutation semantics.
16. Build/attach is all-or-nothing; failure leaves root state, revision, plan, subscriptions, and owned resources unchanged.
17. Hook order is explicit and deterministic; semantically ambiguous ordering fails before attach.
18. Detach/close respects dependency order and cannot orphan dependents.
19. Performance claims require semantic equivalence and include cold start, tails, memory, and teardown.
20. Migration completes only when consumers use unified XO and embedded forks cease executable authority.

**Immediate stop/escalation:** same ID/different body; same epoch/revision/different hash; unresolved `CommitOutcomeUnknown`; `InvariantViolation`; more than three resyncs/60s; callback limit twice; any exceeded bound without named refusal; zero/two terminals or cleanup-after-terminal; close/resource leak; PID mismatch reaching I/O; duplicate formula flight or stale publication; capability attach changes state on failure; capability permutation changes semantics without a declared order; two strict state authorities; unknown codec fallback; plaintext non-loopback token auth; compatibility requiring pickle/eval/killport/silent retry; benchmark delta over 20% without semantic/environment proof.

Escalation includes namespace/epoch/revision, event/Transaction/request/formula/capability IDs (not secret payloads), last-good hash, exact error, playbooks tried/results, prospective/active capability plans, and preserved read-only artifacts. **Divergence rule:** if reality contradicts this oracle, stop and revise architecture; never patch around the broken contract.

---

## 9. Destructive test matrix

Use throwaway namespaces, Redis, sockets, processes, browser profiles, and capability resources only.

| ID | Destructive test | Required result |
|---|---|---|
| D1 | Value+child then repeated set/clear. | Descendant/handle survive; one revision per accepted op. |
| D2 | Random four-op Transactions. | Clear/delete/restore remain distinct; failure has zero visibility. |
| D3 | Retain handles across delete/replace/restore. | Old handles raise `StaleNode`; fresh handles work. |
| D4 | Repr/dir/hasattr/debug/snapshot missing paths. | Node/revision/event counts unchanged. |
| D5 | API/internal/JS prototype collision keys. | Item/Path round-trip; no shadow/prototype pollution. |
| D6 | Fuzz dot/slash/bracket/Unicode/empty/NUL/long paths. | All surfaces agree or exact reject. |
| D7 | Self/mutual/deep recursive callbacks. | FIFO waves, no deadlock, bounded child rejection, parent retained. |
| D8 | Slow/raise/self-unsubscribe/add subscriber. | Documented order, isolation, ownership, bounded behavior. |
| D9 | Redis unavailable before send. | `PersistenceError`; no local state/revision/event/history. |
| D10 | Drop every byte boundary of CAS request/reply. | Definite absent/committed or `CommitOutcomeUnknown`+recovery; never double apply. |
| D11 | SIGKILL after CAS before local apply. | Restart catches up exactly once before access. |
| D12 | Inject local apply failure after durable acceptance. | `InvariantViolation`; recovery-only mode; no ordinary access. |
| D13 | Crash after each post-apply phase. | Receipts rebuild or health visibly fails; state never silently rolls back. |
| D14 | Partition/promote two Redis lineages and write same base. | Epoch/hash conflict freezes; both preserved; no LWW. |
| D15 | Exact duplicate and same ID/different body. | Exact duplicate drops; mismatch freezes/escalates. |
| D16 | Deliver R+2/R/duplicate/missing R+1. | Only contiguous apply; gap resyncs; no buffer leak. |
| D17 | Replay below snapshot/ID-cache floor. | Drop/count; canonical hash unchanged. |
| D18 | Colliding-looking namespaces/Unicode/max length. | Perfect isolation or exact rejection. |
| D19 | Crash schema migration each seam; concurrent old writer. | Old or validated new authority only; fence prevents mix. |
| D20 | Remove/change codec; malicious nested tag. | Explicit error/quarantine; no fallback/partial apply. |
| D21 | Two browsers edit same base offline/online in both orders. | One wins; loser `xo.conflict`; no silent loss. |
| D22 | Inject writes at every snapshot/live seam. | Final peer revision/hash matches; every event once. |
| D23 | Huge/truncated/invalid/deep/duplicate-key/NaN frames. | Exact errors, bounded RSS/CPU, zero mutation. |
| D24 | Hidden/import/unregistered path and formula/function payloads. | Stable refusal; no traversal/execution. |
| D25 | Max-rate producer with stopped consumer. | Bound holds; backpressure, cleanup, one terminal. |
| D26 | Cancel/deadline/disconnect generator with observable `finally`. | Cleanup precedes one terminal; no producer remains. |
| D27 | Late chunks, duplicate terminal, unknown/retired request ID. | Drop/count late; violation; no alias. |
| D28 | Close during blocked read/accept/RPC/stream; repeat close. | Idempotent, bounded, truthful, zero owned resources. |
| D29 | Fork with active backend/RPC/capabilities; use parent and child. | Child rejects until rebuild/new origin; parent remains sound. |
| D30 | Cold import under arbitrary argv with side-effect traps. | Within budget; no optional dep/network/thread/process. |
| D31 | Clean sdist/wheel installs and JS asset lookup. | Identical API/behavior/assets/provenance. |
| D32 | Differential replay of observed safe/rejected legacy use. | Intent parity; unsafe mechanics produce documented migration errors. |
| D33 | Real migrated MagicLLight query stream/import provenance. | Same order/cleanup; unified imports only. |
| D34 | Swap migrated endpoint configurations. | `describe` identity mismatch refuses wrong service. |
| D35 | Benchmark candidate omits event/history/codec/formula/capability work. | Semantic gate invalidates before speed comparison. |
| D36 | Formula switches dynamic dependency branch. | Edges replace atomically after successful compute. |
| D37 | Static/dynamic/self formula cycles. | Exact `FormulaCycleError`; no deadlock/fresh result. |
| D38 | 100 concurrent readers of a diamond graph. | One compute/formula/version vector; identical result. |
| D39 | One mutation invalidates 100k edges; no reads; tiny observed queue. | Dirty/coalesced bounded work; no eager storm. |
| D40 | Formula raises, sleeps, or mutates XO while sources churn. | Exact errors; no base mutation; bounded stale behavior. |
| D41 | Restart/peer lacks formula code or has changed fingerprint. | No code/cache deserialization; trusted recompute or explicit stale/error. |
| D42 | Attach duplicate IDs, two Redis authorities, same endpoint, unsatisfied requirements, protocol/codec mismatch. | Build/attach fails before resource publication; root bit-for-bit unchanged. |
| D43 | Permute all orders of history+Redis+server+JS+formula; include declared and undeclared hook ties/cycles. | Valid permutations yield identical event/hash/hook trace; ambiguous/cyclic plans fail deterministically. |
| D44 | Fault every prepare/install/activate seam for each multi-capability combination. | Reverse rollback restores exact prior plan/state/revision/subscriptions; zero leaked resources/events. |
| D45 | Detach provider with dependents; close amid formula compute, Redis receive, RPC stream, browser peer. | Unsafe detach refused; bounded drain; reverse close; truthful failure; no orphan. |
| D46 | Run history+Redis+server+JS+formula together under local, remote, derived, restore, crash, reconnect, and recursive-observer mutations. | One base commit/revision/event per accepted mutation; no echo loops, duplicate history, or derived republish. |
| D47 | Attach a test validator/Pydantic-like capability before/after formula/codec/Redis; validator rejects and crashes. | Candidate Transaction semantics and rejection are order-stable; attach-time incompatibility fails; no partial mutation. |

---

## 10. Release gates

1. **Core:** D1-D8 pass under deterministic and randomized schedules with exact core errors.
2. **Persistence:** D9-D20 pass against real throwaway Redis; ambiguity, epoch, retention, codec, and migration fencing are exercised.
3. **Replica/browser/security:** D21-D24 pass with real sockets and JavaScript/browser; canonical hashes converge and hostile input never mutates.
4. **Streaming:** D25-D27 pass 10/10; memory stays bounded; cleanup-before-single-terminal always holds.
5. **Lifecycle:** D28-D30 pass on every supported process start method/platform; no leaks or import effects.
6. **Packaging:** D31 passes from sdist and wheel in separate clean environments; no editable/source-shadow evidence.
7. **Migration:** D32-D34 pass for every observed consumer; embedded MagicLLight fork ceases executable authority.
8. **Formula:** D36-D41 pass with exact errors, no code/cache serialization, and visible version-vector freshness.
9. **Capability fusion:** D42-D47 pass for pairwise and full-stack combinations. Build/attach failures are side-effect-free; order is deterministic; close/rollback is complete. Future capabilities inherit these gates.
10. **Performance integrity:** semantic checksum precedes timing. Report cold, p50/p95/p99, allocations/RSS, CPU, first/steady sync, teardown, formula dirty/read/diamond, and full capability-stack overhead with raw environment-tagged samples. `ARCHITECTURE.md:211-223` budgets remain targets until measured.
11. **Rollback/divergence:** schema migration, capability attach, consumer cutover, Redis attach, and browser bridge each have a rehearsed throwaway rollback. No unresolved P0 or silent implementation departure.

---

## 11. Unknown knowns and unknown-unknown tripwires

### Unknown knowns surfaced

- A persistence timeout is not proof of non-commit.
- Event-ID dedupe does not establish order.
- Synchronous post-commit dispatch is not callback execution under a reentrant lock.
- Redis CAS alone does not fence two promoted primaries.
- Token-only plaintext non-loopback TCP is not safe.
- MagicLLight migration includes `_inc`, discovery, generators, and import side effects.
- Formula materialization without formula/vector provenance is convincingly stale.
- “Composable” without a capability conflict/order/rollback protocol simply moves MRO bugs into attach order.
- Full-stack composition must be benchmarked; measuring capabilities separately can hide multiplicative hook/serialization costs.
- Warm medians and behaviorally unequal comparisons invite benchmark reward hacking.

### Tripwires

- `(namespace, epoch, revision, canonical snapshot hash)` at every reconciliation boundary;
- counters for duplicate/stale/gap/resync, late chunks/terminal violations, limit refusals, callback errors/depth, formula dirty/compute/discard/error/single-flight joins;
- active immutable capability-plan hash plus hook trace and resource-owner map in diagnostics;
- health states that distinguish healthy, degraded-readonly, resyncing, recovery-required, split-brain, and close-failed;
- optional diagnostics only, preserving hot-path allocation budgets;
- append-only field log: symptom → IDs/revisions/capability plan → response → result → contract/test change;
- periodic canonical-hash reconciliation for long-lived peers.

---

## 12. Implementation ordering

1. Freeze Path, primitive ops, Event, Transaction, stale-handle, callback, and formula contracts.
2. Define and test `CapabilitySpec`, immutable planning, collision/order validation, transactional attach/detach, ownership, and lifecycle with synthetic capabilities.
3. Implement local core, callback waves, atomic restore, History, and FormulaGraph without networking.
4. Add deterministic safe codec and Python↔JavaScript canonical fixtures.
5. Add Redis as a strict persistence/replication capability; retire ambiguity, epoch, resync, and lifecycle risk.
6. Add unary RPC capability, then streaming terminal law.
7. Add browser bridge capability using the identical event/hash/conflict/resync contracts.
8. Run full multi-capability composition gates before consumer migration.
9. Migrate real consumers and remove executable forks after parity.
10. Package clean artifacts and benchmark only after semantic equivalence.

---

## 13. Primitive-strengthening features only

- Read-only `doctor()` reporting namespace/epoch/revision/hash, lifecycle, bounds, formula vector/dirty/errors, active capability-plan hash, and owned resources.
- Offline event/snapshot continuity, hash, codec, formula-provenance, and capability-plan verifier.
- Test-only fault injection at CAS/send/apply/history/notify/snapshot/terminal/formula-publish/capability prepare-activate-rollback seams.
- Owned context-manager handles for subscriptions, formula observations, and temporary capability attachments.

Reject CRDTs, ORM/query language, workflow scheduler, consensus, renderer, code distribution, and generalized UI framework from core.

---

## 14. Resolved integration decisions

1. **Resolved:** v1 refuses non-loopback RPC and WebSocket binds. Remote use requires an explicit external confidential/authenticated terminator; plaintext token-only transport is not accepted.
2. **Resolved as a gate:** every frame, snapshot, callback wave, queue, formula graph/work batch, capability count/hook pipeline, shutdown wait, and replay store has a conservative finite default. Exact defaults are benchmark/adversarial-test outputs recorded before release; implementations may not use an unbounded sentinel.

---

## Final chaser

The most dangerous seam is between “Redis said yes” and “this process knows Redis said yes.” It passes every pleasant demo and later rewrites truth. The second is formula magic tempting an implementer back into `__onchange__`. The third is calling capabilities composable while allowing attach order to become a quieter MRO. Resist all three shortcuts: unknown truth stops the world; invalidation stays cheap; computation stays lazy; capability plans are validated and transactional; freshness and ownership stay explicit. Then XO can remain magical without becoming mysterious.

— XO failure architect, 2026-08-30
