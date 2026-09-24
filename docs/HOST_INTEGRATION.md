# Host Integration Guide

How to wire `cogno-homeo` into a real application. The kernel **orchestrates**
calls (circuit breaker + retry/backoff + a metrics seam); it never makes one and
holds no I/O. This is the human-facing companion to `examples/host_min.py`.

> TL;DR — you hand `resilient_call` an ordered list of candidates and a one-line
> `attempt` closure; you optionally inject a `CircuitBreaker`, a `RetryPolicy`,
> and a `MetricsSink`. With none of those it degrades to "try each once, fail
> over" — so adopting it changes nothing until you opt in.

---

## 1. The boundary

| Concern | Owner |
| --- | --- |
| Breaker state machine, retry/backoff math, the fallover loop | **kernel** |
| The actual call (HTTP/SDK/DB) wrapped by `attempt` | **host** |
| The breaker **key** (what counts as "the same provider") | **host** |
| Where breaker state lives (in-process vs shared) via `StateStore` | **host** |
| Telemetry destination via `MetricsSink` | **host** |
| Token/cost accounting | **host** (NOT here — the result is opaque to the kernel) |
| The single-consumer lane's loop: one leader, one job at a time, heartbeat, per-job deadline that gives the lane up | **kernel** (`LeaderLane`) |
| What a job IS, how long it may take, what a timeout must undo, where the queue/lock/beat live | **host** (callbacks + the three lane ports) |

---

## 2. `resilient_call`

```python
from cogno_homeo import resilient_call, CircuitBreaker, RetryPolicy

result = await resilient_call(
    candidates,                       # ordered, already filtered for capability
    lambda c: c.do_work(args),        # ONE try against ONE candidate
    key=lambda c: c.name,             # breaker/metrics key (default: c.model / class name)
    is_success=lambda r: bool(r),     # optional: reject an exceptionless-but-empty result
    breaker=CircuitBreaker(),         # optional
    policy=RetryPolicy(max_retries=2),# optional
    metrics=my_sink,                  # optional
)
```

- **First acceptable result wins.** If all candidates fail, the **last exception
  propagates** (you keep the original error type). If nothing was eligible —
  empty list, or every candidate's breaker is open — it raises
  `NoCandidateAvailable`.
- `is_success` lets you treat a returned-but-useless value as a failure (e.g. an
  empty transcription) so the chain fails over instead of returning junk.
- The loop is **signature-agnostic**: text (`b.generate(...)`), audio
  (`t.transcribe(...)`), anything — the per-signature bit is your lambda.

---

## 3. The circuit breaker key (multi-tenant)

The key is an **opaque string you compose** — the kernel never interprets it
(like a `scope`). Pick it by *what the failure is scoped to*:

| Failure | Scope | Key |
| --- | --- | --- |
| Provider outage (5xx/timeout) on a **shared** key | global | `openai:global` |
| Rate-limit / 401 on a **tenant's own** key (BYOK) | per-tenant | `openai:byok:{tenant}` |

Keying BYOK per-tenant prevents one customer's bad key from tripping the breaker
for everyone (noisy-neighbor). For finer control you can run two checks — a
global breaker for infra outages and a per-credential one for auth/quota — and
route `record_failure` by the error class.

---

## 4. Distributed breaker state (`StateStore`)

By default the breaker keeps state **in-process** (`InMemoryStateStore`) — each
worker rediscovers an outage on its own. To share provider health across workers,
implement the `StateStore` port over your store and inject it; the kernel stays
pure:

```python
class RedisStateStore:                      # satisfies StateStore (structural)
    def get(self, key: str) -> BreakerState: ...     # deserialize from Redis
    def set(self, key: str, state: BreakerState) -> None: ...

CircuitBreaker(store=RedisStateStore(redis_client))
```

`BreakerState` is a small dataclass (`status`, `failures`, `opened_at`) — trivial
to (de)serialize.

---

## 5. Metrics (reliability, not billing)

`MetricsSink` receives one `AttemptRecord` per attempt
(`provider`, `ok`, `elapsed_ms`, `error`, `retries`). Plug Prometheus/logs:

```python
class MetricsSink(Protocol):
    def record(self, attempt: AttemptRecord) -> None: ...
```

This is **ops** telemetry. The kernel never sees tokens or cost — the result of
an attempt is opaque to it. Token accounting belongs where the data is produced
(the LLM/audio backends) and is priced by the host.

---

## 6. A single-consumer lane (`LeaderLane`)

For background work that must never run twice AT ONCE because what it spends belongs to the
whole deployment — a model server shared with live traffic, a provider's rate limit, a paid API.
"One job per process" with N workers is N jobs at once; the lane makes it one per deployment:
any process may **push**, exactly one **drains** — whichever holds the lane's leadership lock.

```python
from cogno_homeo import LeaderLane
from cogno_homeo.lane import stalled

lane = LeaderLane(
    queue=store, lock=store, heartbeat=store,  # the three ports — one adapter may be all of them
    work=do_one_job,                           # async: ONE job
    deadline=seconds_for,                      # async: how long THIS job may take (asked first)
    parse=Job.from_row,                        # optional: a row → your job type
    label=lambda job: job.item_id,             # optional: names the running job on the beat
    kinds=runnable_kinds,                      # optional: which kinds may run NOW (None = any)
    on_result=..., on_error=...,               # a job that finished / raised
    on_timeout=mark_interrupted,               # async: undo an overrun job, BEFORE the release
    on_beat_error=...,                         # a beat that failed (a stale lane, not a crash)
    housekeeping=sweep, beat_s=10.0,           # optional: the leader's periodic chores
)
await lane.run(poll_s=2.0, lead_retry_s=30.0, housekeeping_s=300.0)
```

**The four guarantees** (each pinned in `tests/unit/test_lane.py`):

1. **One leader.** Only the holder of the `LeadershipLock` drains. `lead()` takes the lock when
   nobody holds it and CONFIRMS it when this holder already does — a holder whose lock was lost
   behind its back (with its connection, say) must answer `False`, never a remembered `True`.
2. **One job at a time, oldest first,** removed once it RAN — succeeded, raised or timed out. A
   job is tried once; to retry, push it again. `kinds()` holds some kinds back (a budget that ran
   out) without dropping them.
3. **A visible heartbeat.** The leader beats every loop and every `beat_s` during a job, naming
   the job. `stalled(queued=..., beat_age_s=..., stale_after_s=...)` is the predicate for a health
   page: jobs waiting under a missing or stale beat — a leader that hung WITHOUT dying and still
   holds its lock, which a lock alone can never reveal. An empty queue is never stalled.
4. **A deadline per job that gives the lane UP.** The overrun job is cancelled WITHOUT being
   awaited (a driver that ignores the cancel would otherwise hold the lane exactly as long as it
   hangs), `on_timeout` runs, and the lock is released — also when `on_timeout` raises. `run()`
   then waits `lead_retry_s` before asking again, so another process takes over.

`run()` releases the lock on its way out, cancellation included, so the next process does not
wait for this one's connection to time out. A `housekeeping` that raises ends `run()` (and
releases the lock) — it owns its errors.

**The ports are the host's, and so is the durable adapter** — the same boundary as the breaker's
`StateStore` (§4): the kernel stays pure. A Postgres adapter, for instance, is a table of rows
(with a UNIQUE key, so a push that races itself is one job), a **session** advisory lock taken
with `pg_try_advisory_lock` on a connection kept for as long as it leads (the lock dies with the
connection, which is exactly the "a process that dies drops its lock" guarantee — so `lead()`
must check that connection is alive before answering `True`), and a one-row beat table.
`InMemoryLaneQueue` (over a shared `Lease` and a shared `rows` list, handles compete like
processes) is the in-process double, with `unique=` as the UNIQUE key.

The **deadline's value** is the host's too: the kernel cannot know what a job weighs. Keep it
under whatever sweep declares a job's work abandoned, or the sweep will end a job that is still
running.

