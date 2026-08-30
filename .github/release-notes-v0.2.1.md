## XO 0.2.1 — portable performance gates

XO 0.2.1 corrects the release automation discovered by the first v0.2.0 matrix run. The canonical Apple M3 Max budgets remain unchanged; heterogeneous shared CI runners now use explicit portable ceilings for the two host-sensitive measurements.

### Fixed

- Kept canonical regression ceilings at 25 ms for import and 2 µs for clean formula reads on the reference workstation.
- Added a portable CI profile with 50 ms import and 5 µs clean-formula ceilings, derived from the observed Python 3.11–3.14 Ubuntu/macOS matrix while retaining the same root, lookup, and mutation ceilings.
- Kept local and CI profiles executable through the same benchmark command rather than suppressing or skipping the gate.

### Verification

- Python: 87 contracts collected; 86 passed and one disposable-Redis test skipped without `XO_TEST_REDIS_URL`.
- Real Redis: all 11 Redis contracts passed against a dedicated loopback Redis server.
- JavaScript: 4 tests, 19 assertions, zero failures.
- Canonical M3 Max gate: 18.07 ms import, 1.97 µs root creation, 0.35 µs existing read, 1.46 µs scalar set, 1.83 µs five-segment set, and 1.68 µs clean formula read; zero failures.
- Published-channel install will be verified from the v0.2.1 wheel before this release is called complete.

Version 0.2.1 contains the complete v0.2 capability-fusion release plus this CI portability correction. PyPI and npm remain intentionally unclaimed until their publisher environments and post-publication installs are verified.
