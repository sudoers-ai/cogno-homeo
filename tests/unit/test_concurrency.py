"""
Concurrency tests for the breaker under ``resilient_call``.

asyncio is single-threaded and cooperative, so "races" here mean **coroutine
interleaving at ``await`` points** — the real shape of many concurrent
``resilient_call`` invocations sharing one breaker + store. Each ``attempt`` yields
(``await asyncio.sleep(0)``) so the coroutines interleave between the breaker's
``is_open`` check and its ``record_*`` update; the tests assert the state machine
stays consistent (no lost/duplicated transitions, no corrupted counters).
"""

import asyncio

from cogno_homeo import (
    BreakerStatus,
    CircuitBreaker,
    InMemoryStateStore,
    resilient_call,
)

_KEY = lambda c: c  # noqa: E731 — the candidate string is itself the breaker key


async def _gather_calls(n, candidates, attempt, breaker):
    async def call():
        try:
            return await resilient_call(candidates, attempt, breaker=breaker, key=_KEY)
        except Exception as exc:  # noqa: BLE001 — collect the outcome type
            return type(exc).__name__

    return await asyncio.gather(*[call() for _ in range(n)])


async def test_concurrent_failures_open_breaker_consistently():
    breaker = CircuitBreaker(fail_threshold=5, store=InMemoryStateStore())

    async def attempt(c):
        await asyncio.sleep(0)  # interleave between is_open and record_failure
        raise RuntimeError("boom")

    results = await _gather_calls(30, ["X"], attempt, breaker)

    # every call ended in an expected way (real failure, or skipped once open)
    assert set(results) <= {"RuntimeError", "NoCandidateAvailable"}
    st = breaker.store.get("X")
    assert st.status is BreakerStatus.OPEN
    assert st.failures >= 5            # counter is coherent, not corrupted


async def test_concurrent_successes_keep_breaker_closed():
    breaker = CircuitBreaker(fail_threshold=3, store=InMemoryStateStore())

    async def attempt(c):
        await asyncio.sleep(0)
        return "ok"

    results = await _gather_calls(25, ["K"], attempt, breaker)

    assert all(r == "ok" for r in results)
    st = breaker.store.get("K")
    assert st.status is BreakerStatus.CLOSED and st.failures == 0


async def test_concurrent_independent_keys_do_not_interfere():
    breaker = CircuitBreaker(fail_threshold=3, store=InMemoryStateStore())

    async def attempt(c):
        await asyncio.sleep(0)
        if c.startswith("bad"):
            raise RuntimeError("down")
        return "ok"

    async def call(c):
        try:
            return await resilient_call([c], attempt, breaker=breaker, key=_KEY)
        except Exception:  # noqa: BLE001
            return None

    keys = ["bad1", "good1", "bad2", "good2"] * 12
    await asyncio.gather(*[call(k) for k in keys])

    for good in ("good1", "good2"):
        assert breaker.store.get(good).status is BreakerStatus.CLOSED
        assert breaker.store.get(good).failures == 0
    for bad in ("bad1", "bad2"):
        assert breaker.store.get(bad).status is BreakerStatus.OPEN


async def test_concurrent_failover_to_second_candidate():
    breaker = CircuitBreaker(fail_threshold=3, store=InMemoryStateStore())

    async def attempt(c):
        await asyncio.sleep(0)
        if c == "bad":
            raise RuntimeError("down")
        return f"ok:{c}"

    results = await _gather_calls(20, ["bad", "good"], attempt, breaker)

    # whether 'bad' is open or just failing, every call still reaches 'good'
    assert all(r == "ok:good" for r in results)
    assert breaker.store.get("good").status is BreakerStatus.CLOSED
    assert breaker.store.get("bad").failures >= 1


async def test_half_open_recovers_under_concurrency():
    clock = {"t": 0.0}
    breaker = CircuitBreaker(
        fail_threshold=2, cooldown_s=10.0,
        store=InMemoryStateStore(), now=lambda: clock["t"],
    )
    breaker.record_failure("K")
    breaker.record_failure("K")
    assert breaker.is_open("K") is True

    clock["t"] = 20.0  # past cooldown → next probes are allowed (half-open)

    async def attempt(c):
        await asyncio.sleep(0)
        return "ok"

    results = await _gather_calls(10, ["K"], attempt, breaker)

    assert "ok" in results
    st = breaker.store.get("K")
    assert st.status is BreakerStatus.CLOSED   # a successful probe closed it
    assert st.failures == 0
