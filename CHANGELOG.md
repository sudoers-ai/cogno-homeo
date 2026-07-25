# Changelog

## 0.1.0 — 2026-07-25

First public release on PyPI.

- Domain-agnostic resilience kernel: circuit breaker, retry/backoff, and a
  metrics seam behind a signature-agnostic fallback executor (`resilient_call`).
- Pure code, zero dependencies, zero I/O — the caller owns every actual call.
- Foundation for the fallback chains in `cogno-synapse` (text) and
  `cogno-vox` (audio).
