# XO development plan

## Baseline decision

This is a clean re-foundation. The behavior baseline is composite:

- `xo1@101a190`: vision, value/child interaction, subscriptions, Redis history, XO-JS demo;
- `xo2@149b171`: newest Benedict-derived core corrections;
- MagicLight embedded XO through 2024-06-22: configurable service ports and generator streaming;
- AAA and MagicLight: consumer compatibility scenarios.

No legacy source file is copied into the runtime package unless a differential test proves that its exact implementation—not merely its behavior—is required.

## Stage 0 — evidence harness

Deliver:

- isolated legacy environment lock;
- behavior probes for node/value/child/path/hooks/formulas/history/Redis/RPC/JS;
- benchmark protocol with warmup, distributions, allocations/RSS, dependency-graph shapes, and machine metadata;
- expected incompatibility ledger.

Gate:

- every compatibility promise has an executable observation or is explicitly marked proposed;
- legacy failures caused by missing dependencies are reproducibly provisioned rather than bypassed.

## Stage 1 — local semantic core

Deliver:

- `XO`, canonical paths, `MISSING`, mapping and fluent attribute access;
- non-mutating `peek`/membership/snapshot;
- set/delete/batch transaction path;
- immutable events and deterministic subscriptions;
- safe tagged JSON codec and extension registry;
- lazy formulas, automatic read-dependency capture, dirty propagation, cycle detection, cached single-flight recomputation;
- root-scoped capability runtime with declared provisions, requirements, conflicts, ordering, and transactional lifecycle;
- thread-safe lifecycle and explicit close.

Gate:

- core contract suite passes under supported Python versions;
- property/state-machine tests cover arbitrary trees and mutation sequences;
- performance meets absolute budgets and does not materially regress comparable legacy behavior;
- formula gates cover static/dynamic dependencies, chains/diamonds/cycles, exceptions, concurrent readers, stale-compute discard, transitive invalidation cost, and zero recomputation while unobserved;
- capability builder gates cover deterministic order, conflict/cycle rejection, prepare rollback, reverse idempotent close, and zero optional-capability hot-path scan/allocation;
- import graph is standard-library-only and has no background activity.

## Stage 2 — formulas and derived state

Deliver:

- `node.formula(fn)` and explicit dependency introspection;
- context-local automatic dependency capture;
- dirty graph propagation and cached single-flight computation;
- post-commit scheduling for locally or remotely observed formulas;
- materialization policy that persists values but never formula code.

Gate:

- source changes perform no user formula computation for unobserved formulas;
- a diamond graph computes every affected formula at most once per stable source revision;
- cycles report full paths; formula errors remain stable until a dependency changes;
- concurrent reads share one computation and never publish stale output;
- formula functions/closures cannot enter any codec, snapshot, Redis, RPC, or WebSocket payload;
- performance budgets cover clean-cache reads, invalidation per affected edge, and recomputation separately.

## Stage 3 — capability fusion kernel

Deliver:

- frozen `CapabilitySpec` and root-scoped `Capability` lifecycle contracts;
- deterministic dependency/order compiler with singleton-role and conflict validation;
- sparse compiled validator, commit-coordinator, observer, projection, remote-source, and service seams;
- transactional prepare/publish/start and reverse-order close/rollback;
- inspectable named profiles, including a recommended History + Redis + Service + WebSocket profile;
- public conformance kit for future adapters such as Pydantic validation.

Gate:

- input permutation cannot change compiled order or behavior;
- incompatible, duplicate-authority, missing-requirement, and cyclic profiles fail before side effects;
- every injected prepare/start/close failure leaks no thread, socket, subscription, or partial runtime;
- pairwise matrices and the full recommended profile create exactly one semantic commit/history record/invalidation per mutation;
- bare XO pays no optional per-node allocation and no per-mutation registry scan;
- compatibility constructors compile ordinary profiles and contain no alternate implementation.

## Stage 4 — history

Deliver:

- revision DAG, branch-preserving undo/redo/checkout;
- deterministic state reconstruction and compaction policy;
- export/import with integrity checks.

Gate:

- edit-after-undo preserves both futures;
- randomized replay equals materialized state;
- malformed/cyclic/incompatible imports fail without mutating current state.

## Stage 5 — Redis

Deliver:

- RESP client subset, connection pool ownership, strict durable commit;
- atomic revision check/write/publish;
- replication listener, event dedupe, reconnect/catch-up/resnapshot;
- namespace/schema lifecycle and observability.

Gate:

- real disposable Redis scenarios: two/ten processes, conflicts, duplicate/out-of-order delivery, disconnect, restart, crash windows, large snapshots, shutdown;
- no echo loops and eventual snapshot equality;
- strict failure leaves local state unchanged.

## Stage 6 — RPC

Deliver:

- versioned framed codec over Unix and loopback TCP;
- explicit service registry and `public` decorator proxy;
- dynamic client path proxy;
- unary and streaming calls, deadlines, cancellation, typed errors;
- authentication policy and resource bounds.

Gate:

- real subprocess calls and streams;
- cancellation/disconnect releases functions, buffers, tasks, threads, and sockets;
- hostile frame corpus fails closed;
- non-loopback unauthenticated startup/calls are rejected.

## Stage 7 — WebSocket and JavaScript

Deliver:

- RFC 6455 server adapter and state bridge;
- dependency-free JS Proxy client;
- snapshot/catch-up/reconnect/ack/write-policy behavior;
- browser/Bun package artifacts.

Gate:

- real Bun plus real browser scenario;
- bidirectional scalar/tree/delete sync;
- offline mutation policy and reconnect convergence;
- malformed frame, stale revision, permission, and protocol-version scenarios;
- latency budget measured end to end.

## Stage 8 — compatibility and consumers

Deliver:

- measured facades for `xoBenedict`, `FreshRedis`, `xoRedis`, `xoBranch`, `FreshZero`, `FreshClient`;
- migration diagnostics for rejected prototype behaviors;
- AAA and MagicLight scenario adapters;
- one dependency path replacing nested vendored repositories.

Gate:

- observed consumer scenarios pass against the package;
- no consumer imports a vendored XO directory;
- compatibility code delegates to canonical semantics and contains no second state/transport implementation.

## Stage 9 — packaging and release

Deliver:

- Python source/wheel artifacts and JS package artifact;
- CLI (`inspect`, `serve`, `doctor`, `benchmark`);
- API/operation/migration/security documentation;
- CI matrix and reproducible release workflow;
- changelog, provenance, license review, SBOM/checksums.

Gate:

- clean-venv installs from sdist and wheel behave identically;
- JS package works from its packed tarball;
- all behavioral, adversarial, performance, shutdown, and consumer suites pass;
- independent release review finds no unresolved critical/high defect;
- tag contents match verified artifacts.

## Implementation discipline

- Vertical slices remain independently runnable after every merge.
- Capability layers depend on public core contracts only.
- Every network wait has a timeout; every background resource has one owner and an idempotent close.
- No fallback that silently weakens durability, authentication, or consistency.
- No benchmark-specific fast path.
- No docs claim before its executable proof exists.
- Clean cutover only: compatibility facades may preserve source names but no duplicate implementation or deprecated parallel protocol remains.
