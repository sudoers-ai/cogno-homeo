"""
Minimal host wiring for cogno-homeo: wrap an ordered list of "providers" in a
resilient executor with a circuit breaker + a metrics sink.

Nothing here touches the network — the providers are in-memory stand-ins, so the
example runs standalone:  python examples/host_min.py

It shows the three things a host supplies:
  1. the `attempt` closure (how to make ONE try against ONE candidate),
  2. the breaker key (an OPAQUE string the host composes — global vs per-tenant),
  3. a MetricsSink (where reliability telemetry goes).
"""

from __future__ import annotations

import asyncio

from cogno_homeo import (
    AttemptRecord,
    CircuitBreaker,
    RetryPolicy,
    resilient_call,
)


class Provider:
    """A stand-in backend. Real ones are LLM/audio/HTTP clients."""

    def __init__(self, name: str, *, healthy: bool) -> None:
        self.model = name
        self.healthy = healthy

    async def call(self, prompt: str) -> str:
        if not self.healthy:
            raise RuntimeError(f"{self.model} is down")
        return f"[{self.model}] {prompt.upper()}"


class PrintSink:
    """Satisfies MetricsSink (structural) — a host plugs Prometheus/logs here."""

    def record(self, attempt: AttemptRecord) -> None:
        status = "ok" if attempt.ok else f"FAIL({attempt.error})"
        print(f"    · {attempt.provider:8} {status:24} {attempt.elapsed_ms:5.1f}ms")


# A host that wants the breaker shared across workers implements StateStore over
# Redis and injects it — the kernel stays pure:
#
#     class RedisStateStore:                  # satisfies StateStore (structural)
#         def get(self, key): ...             # read BreakerState from Redis
#         def set(self, key, state): ...      # write it back
#
#     CircuitBreaker(store=RedisStateStore(redis_client))


async def main() -> None:
    primary = Provider("openai", healthy=False)   # pretend OpenAI is down
    backup = Provider("groq", healthy=True)

    breaker = CircuitBreaker(fail_threshold=2, cooldown_s=30.0)
    metrics = PrintSink()

    for i in range(3):
        print(f"request {i}:")
        out = await resilient_call(
            [primary, backup],
            lambda p: p.call("hello world"),
            # the breaker key is the host's to compose — e.g. "openai:global" for a
            # shared key, or f"openai:byok:{tenant}" to isolate a tenant's own key.
            key=lambda p: p.model,
            breaker=breaker,
            policy=RetryPolicy(max_retries=0),   # one try per provider, then fail over
            metrics=metrics,
        )
        print(f"  -> {out}\n")

    # After 2 failures the breaker OPENS 'openai'; on request 2 it is skipped
    # entirely (no wasted call) and 'groq' answers immediately.


if __name__ == "__main__":
    asyncio.run(main())
