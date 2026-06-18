"""
cogno_homeo.metrics — the telemetry seam.

``resilient_call`` records one ``AttemptRecord`` per attempt (provider key,
success, latency, error, retry index). The kernel only *defines* the sink and
calls ``.record(...)``; the host plugs a concrete implementation (Prometheus,
structured logs, …). The default ``NullMetricsSink`` discards.

This is **reliability/ops** telemetry, not usage/billing. The kernel never sees
tokens — the result of an attempt is opaque to it (could be text or audio bytes).
Token accounting lives where the data is produced (the LLM/audio backends) and is
aggregated/priced by the host.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable


@dataclass
class AttemptRecord:
    provider: str
    ok: bool
    elapsed_ms: float
    error: Optional[str] = None
    retries: int = 0


@runtime_checkable
class MetricsSink(Protocol):
    def record(self, attempt: AttemptRecord) -> None: ...


class NullMetricsSink:
    """Discards everything. The zero-config default."""

    def record(self, attempt: AttemptRecord) -> None:  # noqa: D401
        return None
