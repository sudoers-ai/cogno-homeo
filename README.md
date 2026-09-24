# cogno-homeo

**Autonomic resilience kernel for the [Cogno](https://github.com/sudoers-ai/cogno-anima) cognitive pipeline** — circuit breaker, retry/backoff, and a metrics seam behind a signature-agnostic fallback executor.

Named for *homeostasis* — the body's self-regulation that keeps it stable under stress. Where [`cogno-anima`](https://github.com/sudoers-ai/cogno-anima) is the *mind* and [`cogno-synapse`](https://github.com/sudoers-ai/cogno-synapse) is the *nerve* that carries the signal to the models, `cogno-homeo` is the **autonomic layer** that keeps those calls alive when providers fail.

> Status: **alpha** — pure-code kernel + unit suite in place.

## Pure code: zero dependencies, zero I/O

`cogno-homeo` **orchestrates** calls; it never makes one. It knows nothing about LLMs or audio — the result of an attempt is opaque to it. That's exactly why it's shared: both `cogno-synapse` (text/embeddings) and [`cogno-vox`](https://github.com/sudoers-ai/cogno-vox) (STT/TTS) build their fallback chains on the same kernel, instead of each re-implementing the loop.

```
cogno-synapse ─┐
               ├─▶ cogno-homeo   (breaker + retry + metrics + fallback)
cogno-vox ─────┘
```

The specialized edge depends on the generic kernel — never the other way around.

## The pieces

| Piece | What it does |
| --- | --- |
| `CircuitBreaker` | per-key state machine (`closed → open → half-open`); stops hammering a dead provider. State behind the `StateStore` port. |
| `RetryPolicy` | full-jitter exponential backoff (pure math; the sleep happens in the executor). |
| `MetricsSink` (Protocol) | reliability telemetry seam — one `AttemptRecord` per attempt. Host plugs Prometheus/logs; default discards. |
| `resilient_call(...)` | the signature-agnostic executor that composes the three over an ordered candidate list. |
| `LeaderLane` | a single-consumer lane: ONE leader in the deployment drains a shared queue, one job at a time, each under a deadline that gives the lane up when it passes. Lock, queue and heartbeat behind the `LeadershipLock` / `JobQueue` / `Heartbeat` ports. |

## `resilient_call` — one executor, any signature

You pass the candidates and a one-line `attempt` closure; the executor adds breaker + retry + metrics on top. The per-signature bit stays a lambda, so the same kernel serves text and audio:

```python
from cogno_homeo import resilient_call, CircuitBreaker, RetryPolicy

# text (cogno-synapse):
await resilient_call(backends, lambda b: b.generate(system, prompt),
                     breaker=CircuitBreaker(), policy=RetryPolicy(max_retries=2))

# audio (cogno-vox) — same kernel, different attempt + a "non-empty" success rule:
await resilient_call(transcribers, lambda t: t.transcribe(audio, filename),
                     is_success=lambda r: bool(r))
```

First acceptable result wins; if all fail the last exception propagates; if nothing was eligible (empty chain / all breaker-open) it raises `NoCandidateAvailable`. With no `policy`/`breaker`/`metrics`, it degrades to the historical "try each once, fail over" loop — so wrapping an existing chain changes nothing until you opt in.

## Distributed breaker state (host-injected)

The breaker keeps state in-memory per process by default. To share provider health across workers, implement the `StateStore` port over your store (e.g. Redis) and inject it — the kernel stays pure:

```python
class RedisStateStore:                      # satisfies StateStore (structural)
    def get(self, key): ...
    def set(self, key, state): ...

CircuitBreaker(store=RedisStateStore(redis_client))
```

The breaker **key is an opaque string the host composes** — e.g. `openai:global` for a shared key vs `openai:byok:{tenant}` to isolate a tenant's own credential so one bad key can't trip the breaker for everyone. `cogno-homeo` never interprets it.

## `LeaderLane` — one consumer in the deployment

For background work that must never run twice at once because what it spends is the whole
deployment's (a model server, a provider rate limit, a paid API): any process may push a job,
exactly one drains — whichever holds the lane's leadership lock.

```python
from cogno_homeo import LeaderLane

lane = LeaderLane(queue=q, lock=q, heartbeat=q,          # one adapter may be all three ports
                  work=do_one_job,                       # ONE job
                  deadline=seconds_for,                  # how long THIS job may take
                  on_timeout=mark_interrupted)           # undo, before the lock is released
await lane.run()                                         # drains only while it leads
```

The four guarantees: **one leader** (only the lock's holder drains; a process that dies drops
its lock and the next one continues from the queue); **one job at a time, oldest first**,
removed once it ran; **a visible heartbeat** naming the leader and the running job, so a leader
that hung without dying shows as a stale beat over waiting jobs (`cogno_homeo.lane.stalled`);
and **a deadline per job that gives the lane UP** — the overrun job is cancelled without being
awaited, the host undoes, the lock is released and this lane waits before asking again, so a
hung leader cannot keep the lane. The durable adapter (a table, a session advisory lock, a beat
row) is the host's, like the breaker's shared `StateStore`; `InMemoryLaneQueue` + `Lease` are
the in-process double. See [`docs/HOST_INTEGRATION.md`](docs/HOST_INTEGRATION.md) §6.

## Install

```bash
pip install cogno-homeo          # zero third-party runtime deps
pip install -e ".[dev]"          # tests + lint + type-check
```

## The Cogno ecosystem

`cogno-homeo` is one organ of **[Cogno](https://github.com/sudoers-ai)** — a family of
small, composable, Apache-2.0 libraries that together form a complete
conversational-agent platform. Each library owns a single concern and stays
infra-agnostic; a **host** assembles them into a running agent:

![The Cogno ecosystem](docs/assets/cogno-ecosystem.svg)

The open-source libraries are the organs; the **host is the body** that joins
them. Our reference host — `cogno-host`, with its `cogno-ui` dashboard — is the
private product layer, but it holds no special powers: everything it does rides
on the public seams documented in each library's `docs/HOST_INTEGRATION.md`, so
you can assemble a body of your own.

## Test

```bash
pytest tests/unit -q
```
