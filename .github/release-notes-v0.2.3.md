## XO 0.2.3 — promise and disconnect hardening

XO 0.2.3 closes production-boundary defects found in a fresh audit of the JavaScript write API, RPC disconnect cleanup, and registry publication gates.

### Fixed

- Keep explicit JavaScript writes promise-stable when value encoding or socket sends fail.
- Surface JavaScript assignment and delete failures through subscription error changes instead of unhandled promise rejections.
- Reject unsafe JavaScript integers before transport so Python/JavaScript state cannot silently lose numeric precision.
- Treat client disconnects during RPC stream termination as normal cleanup instead of leaking worker-thread exceptions.
- Paginate GitHub check runs before package publication so tags with more than 100 checks cannot bypass required-check verification.

### Verified

- Python: 115 passed, 1 intentionally skipped without Redis; strict thread-warning mode passed.
- Real Redis contract: 24 passed.
- JavaScript: 7 passed, 30 assertions.
- Python 3.11, 3.12, 3.13, and 3.14 clean-package verification passed.
- Canonical Apple M3 Max performance gate passed all six budgets.
- Built v0.2.3 wheel installed into a clean environment and passed the package smoke scenario.
- Python wheel/source distribution and JavaScript dry-pack passed.
