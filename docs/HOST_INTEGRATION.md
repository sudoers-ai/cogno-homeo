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
