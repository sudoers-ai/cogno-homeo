"""
cogno-homeo — the autonomic resilience kernel of the Cogno stack.

Named for *homeostasis*: self-regulation that keeps the organism stable under
stress. Pure code, zero dependencies, zero I/O — it orchestrates calls (circuit
breaker + retry/backoff + a metrics seam) but never makes one itself. Domain
agnostic: it knows nothing about LLMs or audio, so both cogno-synapse (text) and
cogno-vox (audio) build their fallback chains on the same kernel.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version

try:
    __version__ = _dist_version("cogno-homeo")
except PackageNotFoundError:  # source tree without an installed dist (e.g. vendored checkout)
    __version__ = "0.0.0"


from cogno_homeo.breaker import (
    BreakerState,
    BreakerStatus,
    CircuitBreaker,
    InMemoryStateStore,
    StateStore,
)
from cogno_homeo.core import NoCandidateAvailable, resilient_call
from cogno_homeo.metrics import AttemptRecord, MetricsSink, NullMetricsSink
from cogno_homeo.retry import RetryPolicy

__all__ = [
    "resilient_call",
    "NoCandidateAvailable",
    "CircuitBreaker",
    "BreakerState",
    "BreakerStatus",
    "StateStore",
    "InMemoryStateStore",
    "RetryPolicy",
    "MetricsSink",
    "NullMetricsSink",
    "AttemptRecord",
]
