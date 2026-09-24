# Changelog

## Unreleased — the single-consumer lane moves here from the reference host (2026-09-24)

### Added

- **`cogno_homeo.lane`** — `LeaderLane`: ONE leader in the deployment drains a shared queue, one
  job at a time, each under its own deadline. Any process may push; exactly one drains — whichever
  holds the lane's leadership lock. Four guarantees, each pinned by a test in
  `tests/unit/test_lane.py`: one leader; one job at a time, oldest first, removed once it ran; a
  heartbeat naming the leader and the running job, with `stalled(...)` as the health predicate (jobs
  waiting under a missing or stale beat — a leader hung without dying); and a deadline per job that
  gives the lane UP — the overrun job is cancelled without being awaited, the host's `on_timeout`
  runs, the lock is released (also when `on_timeout` raises) and the lane waits `lead_retry_s`
  before asking again, so a hung leader cannot keep the lane.
- Three ports — `LeadershipLock` (`lead`/`release`), `JobQueue` (`peek`/`remove`), `Heartbeat`
  (`beat`) — so the durable adapter (a table, a session advisory lock, a beat row) stays the
  host's, like the breaker's `StateStore`. The kernel is still pure code with zero dependencies.
- `Lease` + `InMemoryLaneQueue` — the in-process double (all three ports in one object; handles
  over one lease and one row list compete like processes; `unique=` is the UNIQUE key that makes a
  repeated push of a waiting job one job).
- Moved, not rewritten: the loop, the drain and the heartbeat are the reference host's document
  ingestion lane with the business taken out (what a job is, its deadline's value, what a timeout
  undoes, the log lines) and handed back as callbacks. The one deliberate difference: the release
  after a timeout now also happens when `on_timeout` raises (the host's own undo never raises, so
  nothing it does changes).

## 0.1.1 — 2026-07-31

- No functional changes. First release published via the tag-driven trusted-publishing
  workflow (GitHub Actions OIDC) — validates the token-less release pipeline end to end.

## 0.1.0 — 2026-07-25

First public release on PyPI.

- Domain-agnostic resilience kernel: circuit breaker, retry/backoff, and a
  metrics seam behind a signature-agnostic fallback executor (`resilient_call`).
- Pure code, zero dependencies, zero I/O — the caller owns every actual call.
- Foundation for the fallback chains in `cogno-synapse` (text) and
  `cogno-vox` (audio).
