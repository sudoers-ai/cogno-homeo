"""
cogno_homeo.core — the signature-agnostic resilient executor.

``resilient_call`` turns a "try each candidate in order" loop into one that also
respects a circuit breaker, retries with backoff, and records metrics — without
knowing *what* a candidate is or *what* it returns. The caller supplies:

- ``candidates`` — the ordered list to try (already filtered: e.g. only the
  backends that support a capability),
- ``attempt`` — an async callable that performs ONE try against ONE candidate
  (``lambda b: b.generate(system, prompt)`` for text, ``lambda t: t.transcribe(
  audio, name)`` for audio),
- optionally ``key`` (how to derive the breaker/metrics key from a candidate),
  ``is_success`` (treat an otherwise-exceptionless result as a failure — e.g.
  an empty TTS payload), and the ``breaker``/``policy``/``metrics`` knobs.

First acceptable result wins. If every candidate fails, the last exception
propagates (so callers keep the original error type); if no candidate was even
eligible (empty list / all breaker-open), ``NoCandidateAvailable`` is raised.
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, Optional, Sequence, TypeVar

from cogno_homeo.breaker import CircuitBreaker
from cogno_homeo.metrics import AttemptRecord, MetricsSink, NullMetricsSink
from cogno_homeo.retry import RetryPolicy

T = TypeVar("T")
R = TypeVar("R")


class NoCandidateAvailable(RuntimeError):
    """No candidate could be tried (empty chain, or all skipped by the breaker)."""


def _default_key(candidate: object) -> str:
    return str(getattr(candidate, "model", None) or candidate.__class__.__name__)


async def resilient_call(
    candidates: Sequence[T],
    attempt: Callable[[T], Awaitable[R]],
    *,
    key: Callable[[T], str] = _default_key,
    is_success: Callable[[R], bool] = lambda _r: True,
    breaker: Optional[CircuitBreaker] = None,
    policy: Optional[RetryPolicy] = None,
    metrics: Optional[MetricsSink] = None,
) -> R:
    policy = policy or RetryPolicy()
    sink: MetricsSink = metrics or NullMetricsSink()

    last_exc: Optional[BaseException] = None
    tried_any = False

    for candidate in candidates:
        k = key(candidate)
        if breaker is not None and breaker.is_open(k):
            continue
        tried_any = True

        for retry_index in range(policy.total_attempts):
            if retry_index > 0:
                await asyncio.sleep(policy.backoff_seconds(retry_index - 1))

            t0 = time.perf_counter()
            try:
                result = await attempt(candidate)
            except Exception as exc:  # noqa: BLE001 — failover is the whole point
                last_exc = exc
                _record(sink, k, False, t0, retry_index, str(exc))
                if breaker is not None:
                    breaker.record_failure(k)
                continue

            if not is_success(result):
                # An exceptionless-but-unacceptable result (e.g. empty payload):
                # don't retry the same candidate — move on to the next one.
                _record(sink, k, False, t0, retry_index, "rejected_by_is_success")
                if breaker is not None:
                    breaker.record_failure(k)
                break

            _record(sink, k, True, t0, retry_index, None)
            if breaker is not None:
                breaker.record_success(k)
            return result

    if not tried_any:
        raise NoCandidateAvailable(
            "no candidate was eligible (empty chain or all breaker-open)"
        )
    if last_exc is not None:
        raise last_exc
    # Every tried candidate returned an exceptionless-but-unacceptable result.
    raise NoCandidateAvailable("all candidates returned unacceptable results")


def _record(
    sink: MetricsSink, provider: str, ok: bool, t0: float, retry_index: int, error: Optional[str]
) -> None:
    sink.record(
        AttemptRecord(
            provider=provider,
            ok=ok,
            elapsed_ms=(time.perf_counter() - t0) * 1000.0,
            error=error,
            retries=retry_index,
        )
    )
