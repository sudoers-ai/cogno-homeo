"""Unit tests for the circuit breaker — pure, deterministic (injected clock)."""

from cogno_homeo.breaker import (
    BreakerStatus,
    CircuitBreaker,
    InMemoryStateStore,
)


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def test_closed_by_default():
    cb = CircuitBreaker()
    assert cb.is_open("openai") is False


def test_opens_after_threshold():
    cb = CircuitBreaker(fail_threshold=3)
    for _ in range(2):
        cb.record_failure("openai")
    assert cb.is_open("openai") is False          # still under threshold
    cb.record_failure("openai")                    # 3rd → opens
    assert cb.is_open("openai") is True


def test_success_resets_failures():
    cb = CircuitBreaker(fail_threshold=3)
    cb.record_failure("openai")
    cb.record_failure("openai")
    cb.record_success("openai")
    cb.record_failure("openai")
    cb.record_failure("openai")
    assert cb.is_open("openai") is False           # count was reset by success


def test_half_open_after_cooldown_then_closes_on_success():
    clock = FakeClock()
    cb = CircuitBreaker(fail_threshold=1, cooldown_s=30.0, now=clock)
    cb.record_failure("openai")                    # opens
    assert cb.is_open("openai") is True
    clock.advance(31.0)
    assert cb.is_open("openai") is False           # cooldown elapsed → half-open trial
    store_state = cb.store.get("openai")
    assert store_state.status == BreakerStatus.HALF_OPEN
    cb.record_success("openai")
    assert cb.store.get("openai").status == BreakerStatus.CLOSED


def test_half_open_failure_reopens():
    clock = FakeClock()
    cb = CircuitBreaker(fail_threshold=1, cooldown_s=30.0, now=clock)
    cb.record_failure("openai")                    # opens
    clock.advance(31.0)
    assert cb.is_open("openai") is False           # → half-open
    cb.record_failure("openai")                    # trial failed → reopen
    assert cb.is_open("openai") is True            # still within fresh cooldown


def test_keys_are_independent():
    cb = CircuitBreaker(fail_threshold=1)
    cb.record_failure("openai:global")
    assert cb.is_open("openai:global") is True
    assert cb.is_open("openai:byok:acme") is False


def test_injected_store_is_used():
    store = InMemoryStateStore()
    cb = CircuitBreaker(fail_threshold=1, store=store)
    cb.record_failure("k")
    assert store.get("k").status == BreakerStatus.OPEN
