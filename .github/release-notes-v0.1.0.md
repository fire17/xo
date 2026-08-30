## XO 0.1.0 — one state graph, every boundary

XO's first unified release preserves the interaction that made the original experiments exceptional—one path can hold a value and children—while rebuilding every surrounding capability on one bounded contract.

### What ships

- Dependency-free Python state graph with fluent paths, immutable events, transactions, snapshots, and subscriptions.
- Lazy formulas with dynamic read dependencies, cache invalidation, cycle detection, and single-flight evaluation.
- Deterministic capability fusion for validation, history, services, strict durability, and projections.
- Direct RESP Redis durability and replication with compare-and-swap and explicit ambiguous-outcome recovery.
- Allow-listed local RPC with deadlines, cancellation, bounded frames, and credit-controlled streams.
- Scoped WebSocket synchronization plus a dependency-free JavaScript Proxy peer.
- Compatibility facades for observed historical XO constructors, with unsafe prototype behaviors rejected explicitly.
- `xo inspect`, `xo doctor`, `xo benchmark`, and `xo serve` CLI commands.

### Verification

- Python: 84 contracts collected; 83 passed and one disposable-Redis test skipped without `XO_TEST_REDIS_URL`.
- Real Redis: dedicated loopback integration scenario passed.
- JavaScript: 4 tests, 19 assertions, zero failures.
- Runtime matrix: clean package install and end-to-end scenario on Python 3.11, 3.12, 3.13, and 3.14.
- Build: Python wheel/source distribution and JavaScript package dry-pack passed.
- Performance on Apple M3 Max / Python 3.12.3: 0.325 µs existing read, 1.627 µs scalar set, 1.592 µs cached formula read (100,000 operations × 15 rounds, median).

### Lineage

The original 2021 implementation is preserved on the `legacy-2021` branch. Version 0.1.0 is a clean semantic refoundation rather than a bug-for-bug port. See [`origins.md`](https://github.com/fire17/xo/blob/main/origins.md), [`PRODUCT.md`](https://github.com/fire17/xo/blob/main/PRODUCT.md), and [`ARCHITECTURE.md`](https://github.com/fire17/xo/blob/main/ARCHITECTURE.md) for the recovered intent, product boundary, and exact contracts.

### Package channels

The release contains installable Python artifacts. PyPI and JavaScript registry publication workflows are prepared for trusted publishing; those channels become live after their respective registry environments are authorized.
