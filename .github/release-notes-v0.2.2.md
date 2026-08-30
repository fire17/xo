## XO 0.2.2 — production protocol hardening

XO 0.2.2 closes production-boundary defects found in an adversarial audit of the released v0.2.1 protocol, persistence, lifecycle, and publication paths.

### Fixed

- Reject boolean, fractional, non-finite, zero, and negative resource limits or durations before opening Redis, RPC, or WebSocket resources.
- Cancel and retire expired RPC streams rather than retaining pending requests until connection shutdown.
- Reset JavaScript WebSocket message IDs on reconnect so valid new sessions beginning at message one remain interoperable.
- Use kernel-assigned TCP endpoints in the complete fusion test, removing stale and overlong Unix socket collisions.

### Release integrity

- CI now includes a real Redis 7 integration job.
- Registry workflows require an explicit existing release tag, verify the complete Python 3.11–3.14 Linux/macOS matrix, JavaScript job, and Redis integration check on that tag commit, then publish that exact tag.

### Verified

- Python: 115 passed, 1 intentionally skipped without Redis; real Redis contract: 24 passed.
- JavaScript: 5 passed, 22 assertions.
- Python 3.11, 3.12, 3.13, and 3.14 clean-package verification passed.
- Canonical Apple M3 Max performance gate passed all six budgets.
- Built wheel installed into a clean environment and passed the package smoke scenario.
