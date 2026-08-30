# XO capability taxonomy

This classification separates the product primitive, supported capability layers, developer tooling, compatibility surfaces, consumers, and historical ideas. A name appearing in the legacy vision does not make it a core feature.

## Primary product capabilities

| Capability | Product status | Evidence | Unified destination |
|---|---|---|---|
| value-plus-children expando graph | foundational | `VISION.MD:3-9`, `xo.py` | `xo.core.XO` |
| canonical attribute/item/path access | foundational | `xo.py:1442-1462` | `xo.path`, `XO.node/peek` |
| observable mutation and subscriptions | foundational | `xo.py:1464-1513` | immutable `Event`, `Subscription` |
| snapshots and safe value encoding | foundational | legacy JSON/flatten operations | `xo.codec`, `XO.snapshot` |
| lazy formulas and derived state | foundational | `xo.py:2584-2668`, commit `ca092af`; `$=` formula experiment in `idea/ideal.wip.py:12,40,60` | `xo.formula`, core dependency tracker |
| branching revision history | supported layer | `xoDeque.py`, `VISION.MD:53-59` | `xo.history` |
| Redis autosave and realtime process sync | supported layer | `xo.py:1592-2553` | `xo.backends.redis` |
| low-level function RPC | supported layer | `xoServer.py`, `freshServer.py`, `freshClient.py` | `xo.rpc` |
| bounded streaming RPC | supported layer | later MagicLight embedded variant | `xo.rpc.Stream` |
| Python/JavaScript state sync | supported layer | `JS.py`, `freshSvelt/` | `xo.web`, `js/xo.js` |

## Developer tooling

Tooling observes and operates the product but does not define alternate behavior.

| Historical surface | Classification | Unified treatment |
|---|---|---|
| Rich tree/table renderers | inspection tooling | optional CLI renderer; never imported by core |
| `show`, `whileShow`, `richtree` | inspection tooling | deterministic `xo inspect` and `repr` |
| benchmarks / `xoBench` idea | quality tooling | reproducible benchmark harness and regression gate |
| `xoDecorator` | experimental helper | retain only expressive service exposure through `Service.public` |
| `xoEvents` | exploratory duplicate | delete; folded into core event contract |
| `xoMetric` | proof-of-extension | metrics adapter outside core; not release-critical |
| `xoFunctional` | proof of formula/derived-state intent | replace with first-class lazy formulas, automatic dependency capture, cached recomputation, and cycle-safe invalidation |
| CLI/bookmark/live render experiments | exploratory tooling | no migration unless a concrete workflow requires them |
| Benedict serializers and utilities | inherited unrelated tooling | not part of XO; use standard/third-party serializers explicitly |

## Compatibility surfaces

| Legacy name | Planned mapping |
|---|---|
| `xoBenedict` | compatibility constructor/facade over `XO` |
| `Fresh` | local `XO` with a validator/transform hook, if observed use requires it |
| `FreshRedis` / `xoRedis` | `XO` + `RedisBackend` |
| `xoBranch` | history-enabled `XO` compatibility facade |
| `FreshZero` | `Service` + low-level `Server` |
| `FreshClient` | dynamic `Client` proxy |
| `xoJS` | Python WebSocket bridge plus JS Proxy client |

Compatibility excludes unsafe or accidental behavior: network pickle/dill, remote browser eval, process-killing port takeover, import-time client creation, global roots, silent retry, and mutation-on-inspection.

## Vertical consumers

These validate and consume XO. They do not belong inside its package:

- AAA;
- MagicLight;
- AIrouter/Fusion routing pipeline;
- XO-Svelte demo/application;
- future `xoAtom` or AI objects.

## Historical aspirations

The original vision lists `xoAI`, `xoAider`, `xoJarvis`, `xoEmployee`, `xoStore`, `xoFiles`, `xoAuth`, `xoDB`, messaging hooks, P2P/MQTT/deployment, media, and many other `xo*` concepts. These are a product ecosystem map, not evidence that the core must implement them.

A future adapter or domain object enters XO only when all are true:

1. a concrete consumer exists;
2. it composes through public state/event/backend/service contracts;
3. it does not add policy or heavy dependencies to the core;
4. its lifecycle and failure behavior are testable;
5. maintaining it inside the XO repository is cheaper than maintaining it beside its consumer.

Otherwise it stays an external project or a documented possibility.
