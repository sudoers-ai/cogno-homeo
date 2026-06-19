"""
cogno_homeo.breaker — a per-key circuit breaker.

Stops hammering a candidate that is failing. State is a small machine per opaque
key (the caller decides what the key means — a provider name, ``provider:tenant``,
whatever): ``CLOSED`` (normal) → after ``fail_threshold`` consecutive failures →
``OPEN`` (skip the key for ``cooldown_s``) → ``HALF_OPEN`` (let one trial through)
→ ``CLOSED`` on success / back to ``OPEN`` on failure.

State lives behind the ``StateStore`` port: the default ``InMemoryStateStore`` is
per-process; a host wanting state shared across workers injects its own (e.g. a
Redis-backed store). The breaker itself is pure — no I/O, no clock except an
injectable ``now`` (so tests can advance time deterministically).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional, Protocol, runtime_checkable

# DEBUG-only: the breaker's operational signal is consumed via the MetricsSink in
# core.py; this just traces state transitions during development.
logger = logging.getLogger(__name__)


class BreakerStatus(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class BreakerState:
    """Serializable breaker state for one key."""

    status: BreakerStatus = BreakerStatus.CLOSED
    failures: int = 0
    opened_at: Optional[float] = None


@runtime_checkable
class StateStore(Protocol):
    """Where breaker state is kept. The default is in-memory; a host injects a
    distributed store (e.g. Redis) to share provider health across workers."""

    def get(self, key: str) -> BreakerState: ...
    def set(self, key: str, state: BreakerState) -> None: ...


@dataclass
class InMemoryStateStore:
    """Per-process breaker state. Zero-dependency default."""

    _states: dict[str, BreakerState] = field(default_factory=dict)

    def get(self, key: str) -> BreakerState:
        return self._states.get(key) or BreakerState()

    def set(self, key: str, state: BreakerState) -> None:
        self._states[key] = state


class CircuitBreaker:
    """Per-key open/closed/half-open breaker over a ``StateStore``."""

    def __init__(
        self,
        *,
        fail_threshold: int = 5,
        cooldown_s: float = 30.0,
        store: Optional[StateStore] = None,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        if fail_threshold < 1:
            raise ValueError("fail_threshold must be >= 1")
        self.fail_threshold = fail_threshold
        self.cooldown_s = cooldown_s
        self.store: StateStore = store or InMemoryStateStore()
        self._now = now

    def is_open(self, key: str) -> bool:
        """True if the key should be skipped right now. An ``OPEN`` key past its
        cooldown transitions to ``HALF_OPEN`` and is allowed through (one trial)."""
        st = self.store.get(key)
        if st.status != BreakerStatus.OPEN:
            return False
        if self._now() - (st.opened_at or 0.0) >= self.cooldown_s:
            st.status = BreakerStatus.HALF_OPEN
            self.store.set(key, st)
            logger.debug("event=breaker_transition key=%s from=open to=half_open", key)
            return False
        return True

    def record_success(self, key: str) -> None:
        """A successful call closes the breaker and resets the failure count."""
        prev = self.store.get(key)
        self.store.set(key, BreakerState())
        if prev.status != BreakerStatus.CLOSED:
            logger.debug("event=breaker_transition key=%s from=%s to=closed", key, prev.status.value)

    def record_failure(self, key: str) -> None:
        """A failed call. A failed half-open trial reopens immediately; otherwise
        the breaker opens once ``fail_threshold`` consecutive failures are hit."""
        st = self.store.get(key)
        if st.status == BreakerStatus.HALF_OPEN:
            st.status = BreakerStatus.OPEN
            st.opened_at = self._now()
            st.failures += 1
            logger.debug("event=breaker_transition key=%s from=half_open to=open", key)
        else:
            was_open = st.status == BreakerStatus.OPEN
            st.failures += 1
            if st.failures >= self.fail_threshold:
                st.status = BreakerStatus.OPEN
                st.opened_at = self._now()
                if not was_open:
                    logger.debug(
                        "event=breaker_transition key=%s from=closed to=open fails=%d",
                        key, st.failures,
                    )
        self.store.set(key, st)
