# XO: one living state substrate

## Purpose

XO is the primitive foundation for state that must remain easy locally and become powerful without changing shape.

A user begins with an object:

```python
xo.user.name = "Tami"
```

The same object may later become observable, durable, revisioned, shared across processes, exposed as a service, or synchronized to JavaScript. The path does not change. The mental model does not change. Capabilities attach; the application does not migrate into a framework.

## Recovered intent

The following requirements are recovered from the original repository, its Git history, consumer applications, and verbatim human instructions in `origins.md`:

1. XO stands for an xobject/expando object and is intended as a primitive foundation for advanced applications.
2. A node retains children after it has been assigned or reassigned a value.
3. Value changes run an onchange hook and notify subscribers.
4. Redis-backed XO automatically saves values and synchronizes updates across processes in realtime.
5. XO can use network-accessible Redis for cross-network process sync.
6. `xoBranch` preserves value history and alternate branches rather than overwriting the past.
7. `xoFunctional` explored formulas: an underlying XO mutation ran a function and wrote a derived result (`xo.py:2584-2668`, first committed in `ca092af`). A separate syntax experiment named formula binding `$=` (`idea/ideal.wip.py:12,40,60`). The interaction was valuable even though the implementation was eager and recursion-prone.
8. A Python process can expose functions through object paths and another process can call them through the corresponding path.
9. Cross-process communication should be fast, direct, and support streamed results.
10. Python and JavaScript state can synchronize in realtime so backend changes appear immediately in the frontend and frontend changes return to Python.
11. AAA is an XO consumer for low-level agents whose data can be manipulated externally.
12. MagicLight/AIrouter is an XO consumer demonstrating multi-process routing and a live UI.
13. XO-Svelte is part of xo-benedict, not an independent XO product.
14. The formerly scattered capabilities must become one organized, unified thing without regressing the speed of the working parts.
15. XO's historical inheritance-based variants (`xoRedis`, `xoBranch`, `xoFunctional`, `FreshZero`) must become freely composable capabilities over one bare atomic XO. A recommended hybrid must be able to combine branching history, Redis autosave/realtime sync, RPC service exposure, Python↔JavaScript sync, formulas, and future policies such as Pydantic validation without behavior loss, MRO fragility, or duplicated state authority.
16. Related applications must be mapped, while XO remains the focus.

The original vision also names many aspirational `xo*` domain objects. They remain ecosystem possibilities, not core release requirements unless working evidence exists.

## New design decisions

The following are proposals for the unified implementation, not recovered historical claims:

1. Replace Benedict inheritance with a purpose-built, standard-library-only core.
2. Represent paths canonically as tuples internally and readable dotted/bracket forms externally.
3. Make every mutation one immutable, versioned event consumed by history, persistence, replication, and bridges.
4. Separate fluent creation from non-mutating inspection (`peek`, membership, snapshots).
5. Use a revision DAG for state history rather than storing branch machinery inside each value node.
6. Speak Redis RESP directly so Redis remains optional and cold start stays small.
7. Replace `xoFunctional` with first-class lazy formulas: automatically capture XO reads as dependencies, invalidate cached results on source events, recompute only when read, and recompute after commit when externally observed. Formula code remains local and is never serialized or accepted from a peer.
8. Use framed, versioned tagged JSON for RPC and browser sync; never accept network pickle/dill.
9. Default RPC to Unix sockets or loopback TCP and require authentication when exposed beyond loopback.
10. Provide a dependency-free JavaScript Proxy client over WebSocket.
11. Preserve only observed compatibility names and interactions; reject unsafe accidental behavior.
12. Establish hard cold-start, mutation, formula invalidation/recompute, snapshot, Redis, RPC, and browser latency budgets.
13. Migrate vertical applications to one installed/pinned package only after scenario compatibility passes.

## Experience

### Local state

```python
from xo import XO

state = XO("app")
state.user = "Tami"
state.user.preferences.theme = "dark"

assert state.user.value == "Tami"
assert state.user.preferences.theme.value == "dark"
```

A node is always a node. Assigning `state.user = "Tami"` sets the node's value; it does not replace the node or destroy its descendants.

### Observation

```python
unsubscribe = state.user.subscribe(
    lambda event: print(event.path, event.old, event.new),
    recursive=True,
)

state.user.name = "Tami"
unsubscribe()
```

Subscribers receive one immutable event. Errors in one subscriber do not corrupt the mutation or prevent other subscribers from running.

### Lazy formulas and derived state

```python
state.price = 12
state.quantity = 3
state.total.formula(lambda: state.price.value * state.quantity.value)

assert state.total.value == 36
state.quantity = 4        # marks total dirty; does not recompute yet
assert state.total.value == 48
```

During formula evaluation, XO records every node value read. Source mutations invalidate dependents transitively in $O(\text{affected edges})$ time without executing user code. The next read recomputes once and replaces the dependency set atomically. Subscribed, bridged, or otherwise externally observed formulas recompute after the source transaction commits so peers receive a materialized value without running remote code.


### Capability fusion

XO has one sealed mutation kernel and a root-scoped capability runtime. Behaviors compose through declared requirements, provisions, conflicts, ordering, and lifecycle ownership—not through a growing subclass chain. The build validates the complete profile before side effects, compiles sparse hook pipelines once, prepares capabilities transactionally, and closes them in reverse order. Child nodes inherit the root's capability environment automatically because they are references into the same tree; there is no per-child mixin construction or optional-feature object tax.

`XO()` is the bare object. `XO.compose("app", History(), Redis(...), Service(...), WebSocket(...), Validate(...))` builds any compatible fusion. `XO.recommended(...)` expands to a curated profile using the same pieces. Legacy constructors are thin profile adapters only. New capabilities implement the public SDK and pass lifecycle, ordering, atomicity, serialization, and pairwise/full-profile conformance gates before being called compatible.

The composition model keeps one authority for each semantic role: one state tree, one commit stream, one strict durability coordinator, one history DAG, while allowing multiple independent observers, projections, services, and validators. A collision is rejected at composition time rather than resolved implicitly by inheritance order.

Cycles fail with a dependency path, formula exceptions are cached for the current source version and re-raised until an input changes, concurrent readers share one computation, and a source mutation during computation causes validation/retry rather than publishing a stale result. Formula functions and closures are never serialized, persisted, or accepted over Redis/RPC/WebSocket; only source events and explicitly configured materialized values cross boundaries.

### Persistence and process sync

```python
from xo.backends.redis import RedisBackend

state = XO("app", backend=RedisBackend("redis://localhost:6379/0"))
state.start_sync()
state.status = "ready"       # durable and visible to namespace peers
```

Strict mode commits durably before making a local mutation visible. An unavailable backend causes a clear write failure, not a locally accepted lie. Local-only mode is an explicit policy.

### History

```python
state = XO("draft", history=True)
state.title = "First"
checkpoint = state.revision
state.title = "Second"
state.undo()
state.title = "Alternative"

assert state.history.children(checkpoint)  # both futures remain
```

Undo moves through history. Editing after undo creates another branch. No reachable future is silently deleted.

### Services

```python
from xo.rpc import Service

service = Service()

@service.public.image.generate
def generate(prompt: str, style: str = "realistic"):
    return render(prompt, style)

service.serve("unix:///tmp/image.xo")
```

```python
from xo.rpc import Client

image = Client("unix:///tmp/image.xo")
result = image.image.generate("sunrise", style="vibrant")
```

The decorator and proxy remain expressive. Exposure is allow-listed; transport behavior is explicit; typed errors preserve remote context without exposing internals.

### Streaming

```python
@service.public.chat.stream
def stream(prompt):
    yield from model.stream(prompt)

for chunk in remote.chat.stream("hello"):
    print(chunk, end="")
```

Streams have bounded buffers, cancellation, deadlines, and deterministic cleanup when either side disappears.

### Browser sync

```javascript
import { connectXO } from "@xo/state";

const state = await connectXO("ws://127.0.0.1:8765", { namespace: "app" });
state.user.preferences.theme = "light";
```

The browser speaks the same event semantics. Reconnect resumes from the last acknowledged revision or receives a full snapshot. Browser writes are explicitly enabled and may be path-limited.

## System shape

```text
                         JavaScript Proxy
                               │
                       WebSocket adapter
                               │
local Python ─ events ─ XO state graph ─ history DAG
                               │
                       backend contract
                               │
                      Redis persistence
                               │
                        process replicas

                    Service registry ─ RPC
```

The arrows converge on the semantic core but the modules do not form a dependency cycle. `core` knows no Redis, socket, browser, Rich, Flask, or agent concept.

## Scope by release layer

### Core

- value-plus-children nodes;
- fluent attributes, mapping access, canonical paths;
- values, deletion, snapshots, atomic batches;
- typed immutable events and subscriptions;
- deterministic codec for supported values;
- thread-safe mutation semantics;
- lazy formulas, dependency capture, dirty propagation, and cached single-flight recomputation;
- deterministic capability compiler and transactional root-scoped runtime;
- revision history.

### Redis

- durable snapshots and mutations;
- realtime namespace replication;
- reconnect and catch-up;
- explicit revision conflicts;
- no echo loops;
- bounded network behavior and lifecycle.

### RPC

- Unix and loopback TCP servers;
- expressive path function exposure;
- dynamic client proxy;
- unary and streaming calls;
- deadlines, cancellation, errors, limits, authentication policy.

### Web

- RFC 6455 WebSocket bridge;
- dependency-free JS Proxy;
- snapshots, events, acknowledgements, reconnect;
- explicit path-level browser write policy.

### Compatibility

- `xoBenedict`;
- `FreshRedis` / `xoRedis`;
- `xoBranch`;
- `FreshZero`;
- `FreshClient`;
- migration diagnostics for unsupported unsafe behavior.
- `xoFunctional` migration to local lazy formulas;
- inheritance-based variants migrate to ordinary named composition profiles;

### Tooling

- `xo inspect`;
- `xo serve`;
- `xo doctor`;
- `xo benchmark`;
- deterministic debug rendering and protocol traces behind explicit flags.

Tooling uses the public contracts. It does not receive privileged alternate semantics.

## Non-goals

- copying all of python-benedict;
- remote arbitrary Python object deserialization;
- remote JavaScript evaluation;
- automatic network exposure;
- implicit server startup at import;
- killing another process to claim a port;
- absorbing AAA, MagicLight, AIrouter, or future AI policy into the core;
- rendering application HTML;
- inventing adapters before a demonstrated consumer requires them;
- preserving every prototype operator overload or accidental quirk.

## Performance covenant

Performance is a release contract, not an adjective.

The harness records machine, interpreter, sample count, distributions, payload size, and backend state. It measures legacy behavior in a reproducible legacy environment and the unified candidate in a clean environment. A release fails when a comparable core behavior regresses materially or exceeds its absolute budget without an approved evidence-backed exception.

Initial candidate budgets:

| Operation | Warm median budget |
|---|---:|
| `import xo` | 25 ms |
| root construction | 10 µs |
| existing child lookup | 1 µs |
| local scalar mutation with event | 5 µs |
| five-segment path mutation | 15 µs |
| 10,000-leaf snapshot | 20 ms |
| local durable Redis mutation | 1 ms |
| loopback unary RPC | 1 ms |
| local Python-to-browser update | 10 ms |

Tail latency, allocations, throughput under contention, and shutdown time are recorded alongside medians. A benchmark that omits the work implied by its name is invalid.

## Correctness covenant

Before release, the system must prove:

1. assigning a value never removes descendants;
2. inspection can be performed without creating state;
3. each accepted mutation produces exactly one semantic event and revision;
4. strict persistence failure leaves local state unchanged;
5. replicated events converge without echo;
6. revision conflicts never silently overwrite;
7. undo plus edit preserves both futures;
8. malformed, oversized, unauthenticated, or incompatible frames fail closed;
9. RPC streams release resources on completion, error, cancellation, timeout, and disconnect;
10. browser reconnect reaches the same snapshot as an uninterrupted peer;
11. shutdown leaves no owned thread, socket, task, or subscription alive;
12. wheels and source distributions install and behave identically in clean environments;
13. AAA and MagicLight compatibility scenarios pass before their vendored copies are retired.

## Migration path

1. Freeze and inventory all historical sources; never rewrite provenance.
2. Differentially characterize the old core interactions in an isolated legacy environment.
3. Build and prove the local semantic core.
4. Add history against the same event contract.
5. Add Redis with a real disposable server and crash/conflict tests.
6. Add RPC with real Unix/TCP processes, streaming, cancellation, and hostile frames.
7. Add WebSocket/JS with real Bun/browser processes and reconnect tests.
8. Add measured compatibility facades for AAA and MagicLight scenarios.
9. Replace vendored consumer copies with pinned package dependencies.
10. Package, install from artifacts, benchmark, adversarially review, and release.

## The finished quality

The finished XO should feel smaller than the prototype even though it is more capable. Local use should feel instantaneous. Distributed use should be explicit. Failures should name what failed and what state remains true. Every layer should be independently removable. The same path should remain recognizable from Python memory to Redis to a service call to JavaScript.

The work of art is not the number of features. It is the absence of seams among the features that belong together—and the presence of strong boundaries where they do not.
