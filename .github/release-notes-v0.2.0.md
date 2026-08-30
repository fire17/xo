## XO 0.2.0 — the boundaries join the root

XO 0.2.0 closes the last integration gaps around the unified state graph: RPC now participates in capability composition, browser writes enter the same canonical mutation pipeline as Python writes, and the complete profile is exercised as one root with one revision.

### What changed

- Added root-scoped `rpc_server()` composition over the same allow-listed `ServiceRegistry` used by local calls.
- Made writable WebSocket prefixes commit through XO directly by default; explicit callbacks remain policy hooks.
- Extended `XO.recommended()` and `Profile.hybrid()` with explicit service transports and projections.
- Added a full-fusion contract covering validation, branch-preserving history, strict durability, projection observers, RPC, and WebSocket on one root.
- Added executable architecture budgets to CI and the Python runtime matrix.

### Verification

- Python: 87 contracts collected; 86 passed and one disposable-Redis test skipped without `XO_TEST_REDIS_URL`.
- Real Redis: all 11 Redis contracts passed against a dedicated loopback Redis server.
- JavaScript: 4 tests, 19 assertions, zero failures.
- Static gates: Ruff and Python bytecode compilation passed.
- Build: Python wheel/source distribution and JavaScript package dry-pack passed.
- Architecture budgets on Apple M3 Max: 17.58 ms import, 1.95 µs root creation, 0.37 µs existing read, 1.48 µs scalar set, 1.67 µs five-segment set, and 1.55 µs clean formula read; every enforced budget passed.

### Compatibility

The canonical state/event model is unchanged. Version 0.2.0 is a feature release because it adds public composition APIs and changes writable WebSocket prefixes from callback-required to canonical-by-default. Custom write callbacks remain supported for authorization and policy.

The original 2021 implementation remains preserved on [`legacy-2021`](https://github.com/fire17/xo/tree/legacy-2021). See [`ARCHITECTURE.md`](https://github.com/fire17/xo/blob/main/ARCHITECTURE.md) for the exact capability and mutation contracts.

### Package channels

The GitHub release contains installable Python artifacts. PyPI and npm remain intentionally unclaimed until their publisher environments are authorized and installs from those registries are verified.
