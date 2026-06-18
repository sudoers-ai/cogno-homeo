"""Unit tests for resilient_call — the signature-agnostic executor."""

import pytest

from cogno_homeo.breaker import CircuitBreaker
from cogno_homeo.core import NoCandidateAvailable, resilient_call
from cogno_homeo.metrics import AttemptRecord
from cogno_homeo.retry import RetryPolicy


class Backend:
    def __init__(self, model: str, *, fail: bool = False, result: str = "ok"):
        self.model = model
        self.fail = fail
        self.result = result
        self.calls = 0

    async def run(self) -> str:
        self.calls += 1
        if self.fail:
            raise RuntimeError(f"{self.model} down")
        return self.result


class RecordingSink:
    def __init__(self) -> None:
        self.records: list[AttemptRecord] = []

    def record(self, attempt: AttemptRecord) -> None:
        self.records.append(attempt)


async def test_first_success_wins():
    a, b = Backend("a"), Backend("b")
    out = await resilient_call([a, b], lambda c: c.run())
    assert out == "ok"
    assert a.calls == 1 and b.calls == 0          # never reached the second


async def test_failover_to_next():
    a, b = Backend("a", fail=True), Backend("b", result="from-b")
    out = await resilient_call([a, b], lambda c: c.run())
    assert out == "from-b"
    assert a.calls == 1 and b.calls == 1


async def test_all_fail_reraises_last():
    a, b = Backend("a", fail=True), Backend("b", fail=True)
    with pytest.raises(RuntimeError, match="b down"):
        await resilient_call([a, b], lambda c: c.run())


async def test_empty_chain_raises_no_candidate():
    with pytest.raises(NoCandidateAvailable):
        await resilient_call([], lambda c: c.run())


async def test_is_success_rejects_then_falls_over():
    # 'a' returns empty (rejected), 'b' returns content.
    a, b = Backend("a", result=""), Backend("b", result="hi")
    out = await resilient_call(
        [a, b], lambda c: c.run(), is_success=lambda r: bool(r)
    )
    assert out == "hi"
    assert a.calls == 1                            # not retried on the same candidate


async def test_all_reject_without_exception_raises_no_candidate():
    a, b = Backend("a", result=""), Backend("b", result="")
    with pytest.raises(NoCandidateAvailable, match="unacceptable"):
        await resilient_call([a, b], lambda c: c.run(), is_success=lambda r: bool(r))
    assert a.calls == 1 and b.calls == 1


async def test_breaker_open_skips_candidate():
    cb = CircuitBreaker(fail_threshold=1)
    cb.record_failure("a")                         # 'a' is now open
    a, b = Backend("a"), Backend("b", result="from-b")
    out = await resilient_call([a, b], lambda c: c.run(), breaker=cb)
    assert out == "from-b"
    assert a.calls == 0                            # skipped entirely


async def test_breaker_all_open_raises_no_candidate():
    cb = CircuitBreaker(fail_threshold=1)
    cb.record_failure("a")
    a = Backend("a")
    with pytest.raises(NoCandidateAvailable):
        await resilient_call([a], lambda c: c.run(), breaker=cb)


async def test_retry_same_candidate_then_succeeds():
    class Flaky:
        model = "flaky"

        def __init__(self) -> None:
            self.calls = 0

        async def run(self) -> str:
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("transient")
            return "recovered"

    f = Flaky()
    policy = RetryPolicy(max_retries=2, base_ms=0, jitter=False)
    out = await resilient_call([f], lambda c: c.run(), policy=policy)
    assert out == "recovered"
    assert f.calls == 3


async def test_metrics_recorded_per_attempt():
    sink = RecordingSink()
    a, b = Backend("a", fail=True), Backend("b", result="ok")
    await resilient_call([a, b], lambda c: c.run(), metrics=sink)
    assert [(r.provider, r.ok) for r in sink.records] == [("a", False), ("b", True)]
    assert all(r.elapsed_ms >= 0.0 for r in sink.records)


async def test_breaker_records_outcomes():
    cb = CircuitBreaker(fail_threshold=1)
    a, b = Backend("a", fail=True), Backend("b", result="ok")
    await resilient_call([a, b], lambda c: c.run(), breaker=cb)
    assert cb.is_open("a") is True                 # failure opened it
    assert cb.is_open("b") is False                # success kept it closed


async def test_custom_key_used_for_breaker():
    cb = CircuitBreaker(fail_threshold=1)
    cb.record_failure("svc:1")
    a = Backend("a")
    b = Backend("b", result="ok")
    out = await resilient_call(
        [a, b], lambda c: c.run(), key=lambda c: "svc:1" if c is a else "svc:2", breaker=cb
    )
    assert out == "ok"
    assert a.calls == 0                            # 'svc:1' open → a skipped
