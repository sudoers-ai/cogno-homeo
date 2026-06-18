"""Unit tests for the retry/backoff policy."""

import pytest

from cogno_homeo.retry import RetryPolicy


def test_default_is_single_attempt():
    p = RetryPolicy()
    assert p.total_attempts == 1


def test_total_attempts_counts_retries():
    assert RetryPolicy(max_retries=2).total_attempts == 3


def test_negative_retries_rejected():
    with pytest.raises(ValueError):
        RetryPolicy(max_retries=-1)


def test_backoff_grows_exponentially_without_jitter():
    p = RetryPolicy(max_retries=3, base_ms=100, factor=2.0, jitter=False)
    assert p.backoff_seconds(0) == pytest.approx(0.1)
    assert p.backoff_seconds(1) == pytest.approx(0.2)
    assert p.backoff_seconds(2) == pytest.approx(0.4)


def test_backoff_capped_at_max():
    p = RetryPolicy(max_retries=10, base_ms=1000, factor=10.0, max_ms=5000, jitter=False)
    assert p.backoff_seconds(5) == pytest.approx(5.0)


def test_jitter_stays_within_bounds():
    p = RetryPolicy(base_ms=1000, factor=2.0, jitter=True)
    for i in range(50):
        ceil = min(1000 * (2.0 ** i), p.max_ms) / 1000.0
        assert 0.0 <= p.backoff_seconds(i) <= ceil
