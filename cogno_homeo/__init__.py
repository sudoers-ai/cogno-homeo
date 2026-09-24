"""
cogno-homeo — the autonomic resilience kernel of the Cogno stack.

Named for *homeostasis*: self-regulation that keeps the organism stable under
stress. Pure code, zero dependencies, zero I/O — it orchestrates calls (circuit
breaker + retry/backoff + a metrics seam) but never makes one itself. Domain
agnostic: it knows nothing about LLMs or audio, so both cogno-synapse (text) and
cogno-vox (audio) build their fallback chains on the same kernel.

``cogno_homeo.lane`` is the same idea for background work: a single-consumer lane
whose ONE leader drains a shared queue, one job at a time, each under a deadline
that gives the lane up when it passes — behind ports for the leadership lock, the
queue and the heartbeat, whose durable adapters are the host's.
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
from cogno_homeo.lane import (
    Heartbeat,
    InMemoryLaneQueue,
    JobQueue,
    LeaderLane,
    LeadershipLock,
    Lease,
)
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
    "LeaderLane",
    "LeadershipLock",
    "JobQueue",
    "Heartbeat",
    "Lease",
    "InMemoryLaneQueue",
]
