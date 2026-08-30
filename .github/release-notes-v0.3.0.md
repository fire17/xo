## XO 0.3.0 — Python ↔ JavaScript semantic parity

XO 0.3.0 makes Python and JavaScript full peers for canonical state operations while preserving explicit host-only boundaries for executable policy such as formulas, validators, history, persistence, services, and transport serving.

### Added

- JavaScript clear-value, subtree restore, ordered traversal, containment, default reads, path-scoped subscriptions, and atomic multi-operation transactions.
- A durable `LANGUAGE_SUPPORT.md` release gate mapping current language capabilities, host-versus-protocol boundaries, and the admission contract for future Bash, PowerShell, Mojo, Rust, C, C++, Go, Zig, and Ruby bindings.
- A shared language fixture consumed by Python and JavaScript.
- A real two-process Python↔JavaScript test proving both runtimes can read and write one live XO namespace in both directions.

### Fixed

- Authoritative snapshot installation keeps the canonical Python root handle live.
- Python integers outside JavaScript's exact safe range fail closed at the language bridge rather than arriving with silent precision loss.
- JavaScript fatal protocol/codec failures retain the original error instead of being overwritten by a generic close event.

### Verified

- Python: 121 passed, 1 intentionally skipped without a disposable Redis URL; strict thread-warning mode passed.
- JavaScript: 9 passed, 53 assertions.
- Live parity: Python and Bun exchanged values, bytes, fixed sequences, set, clear, delete, subtree restore, an atomic four-operation transaction, and subsequent Python writes over one namespace.
- Static gates: Ruff, Python compilation, and Node syntax passed.
- Portable performance: all six budgets passed.
- Python wheel/source distribution and JavaScript dry-pack passed.
