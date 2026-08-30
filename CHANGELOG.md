# Changelog

All notable XO changes are recorded here. Versions follow Semantic Versioning.

## 0.3.0 — 2026-08-30

### Added

- A durable release-gating language support map, shared cross-language fixture, and real bidirectional Python↔JavaScript same-namespace conformance test.
- JavaScript clear-value, subtree restore, ordered traversal, containment, default reads, path-scoped subscriptions, and atomic multi-operation transactions.

### Fixed

- Keep canonical root handles usable after authoritative snapshot installation.
- Reject Python integers that cannot cross into JavaScript without precision loss instead of emitting lossy JSON.

## 0.2.3 — 2026-08-30

### Fixed

- Keep JavaScript explicit writes promise-stable when value encoding or socket sends fail, and surface assignment/delete failures through subscriptions instead of unhandled rejections.
- Reject unsafe JavaScript integers before transport so Python/JavaScript state cannot silently lose numeric precision.
- Treat client disconnects during RPC stream termination as normal cleanup instead of leaking worker-thread exceptions.
- Verify every completed check run on a release tag, not only the first GitHub API page, before publishing either package.

## 0.2.2 — 2026-08-30

### Fixed

- Reject non-finite, non-numeric, boolean, and fractional timeout/bound configurations before opening Redis, RPC, or WebSocket resources.
- Retire and cancel RPC stream requests when their client deadline expires instead of leaving a pending request until connection shutdown.
- Reset JavaScript wire message IDs for every reconnected WebSocket session so a valid server sequence beginning at one is accepted.

### Security

- Gate Python and JavaScript registry publication on an explicit existing release tag whose complete Python and JavaScript CI matrix is already successful.


## 0.2.1 — 2026-08-30

### Fixed

- Split canonical M3 Max performance budgets from portable heterogeneous-runner ceilings so CI catches regressions without treating host variance as product regressions.

## 0.2.0 — 2026-08-30

### Added

- Root-scoped `rpc_server()` capability so allow-listed microservices share XO's capability lifecycle and service registry.
- Executable architecture-budget gate covering import, root creation, reads, scalar/path writes, and clean formula reads.
- Full-fusion contract spanning validation, history, strict durability, RPC, WebSocket, and projection observers on one root/revision.

### Changed

- WebSocket writable prefixes now use XO's canonical mutation pipeline by default; custom callbacks remain optional policy hooks.
- `XO.recommended()` and `Profile.hybrid()` accept explicit service transports alongside durability and projections.

## 0.1.0 — 2026-08-30

The first unified XO release.

### Added

- A dependency-free Python state tree where every path can hold both a value and children.
- One immutable event and transaction model for local, restored, remote, and persistent mutations.
- Lazy formulas with read-captured dependencies, caching, dynamic dependency replacement, cycle detection, and single-flight evaluation.
- Deterministic capability composition for validation, history, services, durability, projections, and future extensions.
- Branch-preserving revision history with undo and redo.
- Strict Redis durability and replication over a bounded RESP socket implementation.
- Allow-listed local RPC with deadlines, cancellation, bounded frames, and credit-controlled streaming.
- Scoped WebSocket synchronization and a dependency-free JavaScript Proxy peer.
- Compatibility facades for observed historical XO constructors without retaining a second state engine.
- `xo inspect`, `xo doctor`, `xo benchmark`, and `xo serve` commands.
- Typed Python and JavaScript packages, reproducible build artifacts, a cross-platform CI matrix, and executable verification scripts.

### Changed

- Replaced the historical inheritance ladder and copied Benedict distribution with one canonical semantic core and root-scoped capability runtime.
- Made transport, RPC, browser, and compatibility exports lazy so bare `import xo` stays lightweight.

### Security

- Removed pickle, remote eval, arbitrary attribute traversal, implicit external binds, unbounded network frames, and silent local-only durability fallbacks from supported paths.
