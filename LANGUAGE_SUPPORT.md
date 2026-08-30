# XO language support and parity contract

This file is the release-gating map for every XO language binding. A capability may be marked **full** only when the binding passes the shared fixtures, its own unit contracts, and a live bidirectional test against another full binding in one namespace.

## Status vocabulary

| Status | Meaning | Release consequence |
|---|---|---|
| **full** | Public, typed where the language supports it, and enforced by automated conformance tests | May be advertised as supported |
| **host** | Canonical implementation that owns this capability; peers consume its effects over the protocol | Not a peer-parity defect |
| **protocol** | Available through the shared XO protocol with language-native API shape | Must interoperate with every full binding |
| **planned** | Named future work; no executable implementation or support claim | Must not appear as supported |
| **n/a** | Intentionally belongs outside a language binding or has no meaningful equivalent | Requires a rationale in this map |

## Supported languages

| Language | Tier | Package/runtime | State graph | Read/write parity | Transactions | Snapshots | Subscriptions | Derived projections | Reconnect/catch-up | Live cross-runtime gate |
|---|---|---|---|---|---|---|---|---|---|---|
| Python 3.11–3.14 | **full / host** | `xo-state` | full | full | full | full | full | host | n/a: authoritative in-process root | `tests/test_language_parity.py` |
| JavaScript (Bun/browser) | **full / protocol** | `@fire17/xo-state` | full | full | full | full subtree restore | full, path-scoped | full read-only projection | full | `tests/test_language_parity.py` |

"Full" is semantic parity, not identical syntax. Python remains the current host for validators, normalizers, persistence coordination, formula execution, history, services, RPC serving, and WebSocket serving. JavaScript sees and authors the same canonical state and revisions; host-only executable policy is not serialized or executed remotely.

## Canonical state capability matrix

| Contract | Python | JavaScript | Shared evidence |
|---|---|---|---|
| value and children coexist at one node | full | full | shared snapshot fixture + live peer test |
| virtual path traversal does not mutate | full | full | core tests / JS unit tests |
| attribute/property and explicit path access | full | full | `XO.at`; Proxy + `at` |
| distinguish missing node, missing value, `null`/`None` | full | full | `exists`, `has_value` / `hasValue`, `value` |
| explicit read with default | `get(default)` | `get(default)` | unit contracts |
| set value | full | full | wire `set` |
| clear value while preserving children | full | full | wire `clear` |
| delete subtree | full | full | wire `delete` |
| restore subtree image | full | full | wire `restore` |
| atomic multi-operation commit, one revision | full | full | wire `tx`; bridge atomicity tests |
| optimistic expected-revision conflict | full | full | every remote write carries current revision |
| ordered keys and iteration | full | full | `keys`/iterator contracts |
| child containment | full | full | containment / `has(path)` |
| immutable event revisions and event identity | full | full | event/transaction envelopes |
| path-scoped observation | full | full | core subscription and JS filtering tests |
| snapshot/catch-up reconnect | host | full | WebSocket bridge and JS reconnect tests |
| derived values never enter authored history | host | full read-only | derived envelope tests |
| bytes | full | `Uint8Array` | shared tagged codec fixture |
| tuples | tuple | frozen array | shared tagged codec fixture; semantic fixed sequence |
| safe integers | arbitrary precision locally; protocol must be JS-safe | safe integer | protocol gate prevents silent loss |
| finite floats, strings, booleans, null, lists, objects | full | full | shared codec fixture |
| custom codec extensions | host-only until a tag is implemented by every receiving binding | rejects unknown tags | fail-closed codec law |

## Host capabilities versus binding parity

These capabilities compose around the canonical state graph but are not required to be reimplemented inside every language binding.

| Capability | Python role | JavaScript role | Parity law |
|---|---|---|---|
| validation and normalization | host | receives accepted commits or typed errors | same committed result, never duplicated policy |
| history/undo/redo | host | observes resulting canonical events | history effects must converge like any other commit |
| formulas | host executes | reads derived projection | source code never crosses the boundary |
| Redis durability/replication | host coordinator | transparent peer | durable state is identical after reconnect |
| services and RPC | host/server and client | planned client only if demanded by a real consumer | separate capability, not state-core parity |
| WebSocket transport | host server | protocol client | version/schema/errors shared and fail closed |
| CLI and inspection | host tooling | n/a: use language-native inspection | tooling must not create alternate state semantics |

## Planned language bindings

These are roadmap entries only. They are intentionally not scaffolded because a placeholder binding would create a false support claim.

| Language | Planned native shape | Required transport/codec work | Status |
|---|---|---|---|
| Bash | native command + shell-safe JSON/event stream | process lifecycle, quoting, binary values | planned |
| PowerShell | module/cmdlets + objects/events | cancellation and Windows transport | planned |
| Mojo | typed native client | ecosystem/runtime availability and sockets | planned |
| Rust | crate with async and blocking clients | ownership-safe subscriptions and codec | planned |
| C | small ABI-stable library | memory ownership, callbacks, error structs | planned |
| C++ | RAII wrapper over native core/protocol | exception/no-exception profiles | planned |
| Go | module with contexts/channels | bounded subscription delivery | planned |
| Zig | dependency-light native client | allocator-explicit tree and codec | planned |
| Ruby | gem with method/path proxy | keyword/method collision rules | planned |

## New-binding admission gate

A future binding is not **full** until all gates pass:

1. Implement protocol and schema version negotiation; reject unknown versions and tags.
2. Consume `tests/fixtures/language_parity.json` without a binding-specific fork.
3. Preserve value-plus-children, missing/null, clear/delete, subtree restore, and ordered children.
4. Implement revision-guarded set, clear, delete, restore, and atomic transaction operations.
5. Prove values, bytes, fixed sequences, Unicode, nested lists/objects, and numeric limits without silent coercion.
6. Prove path-scoped subscriptions, duplicate suppression, contiguous revision enforcement, resync, reconnect, and bounded pending work.
7. Run a real bidirectional process test against Python and at least one other full peer in the same live namespace.
8. Add the binding to CI, package/build verification, release workflow required checks, this matrix, and release notes in one change.
9. Exercise failures: unauthorized prefix, stale revision, absent delete, malformed snapshot, unknown codec tag, oversized frame, disconnect during pending write, and partial-invalid transaction.
10. Remove every placeholder/support claim until package installation and the published-channel smoke test pass.

## Maintenance law

Any change to `xo.core`, `xo.codec`, `xo.events`, `xo.wire`, `xo.web`, or a language binding must update the shared fixture or explicitly prove that the capability matrix is unchanged. CI runs the real Python↔JavaScript process test, not only mocked sockets. A release is blocked if this map claims more than the executable gates prove.
