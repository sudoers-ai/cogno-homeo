"""
cogno_homeo.retry — a retry/backoff policy (pure math, no sleeping here).

Describes *how many* times to try one candidate and *how long* to wait between
tries. The actual ``await asyncio.sleep`` happens in ``resilient_call`` — this is
just the policy. Full-jitter exponential backoff (``random.uniform(0, delay)``)
to avoid thundering-herd retries.

Default ``max_retries=0`` ⇒ one attempt per candidate, i.e. the historical
"try each backend once, fail over to the next" behaviour, so wrapping an existing
fallback chain in ``resilient_call`` changes nothing unless a policy is supplied.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass
class RetryPolicy:
    max_retries: int = 0          # retries *after* the first try (0 ⇒ single attempt)
    base_ms: float = 200.0
    factor: float = 2.0
    max_ms: float = 10_000.0
    jitter: bool = True

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")

    @property
    def total_attempts(self) -> int:
        return self.max_retries + 1

    def backoff_seconds(self, retry_index: int) -> float:
        """Seconds to sleep before retry ``retry_index`` (0 = first retry)."""
        delay_ms = min(self.base_ms * (self.factor ** retry_index), self.max_ms)
        if self.jitter:
            delay_ms = random.uniform(0.0, delay_ms)
        return delay_ms / 1000.0
