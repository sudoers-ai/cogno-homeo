# Logging — convenção desta lib

Esta biblioteca **emite** logs; o **host configura** (handlers, formato, nível,
contexto de tenant). Regras:

1. Use `logging.getLogger(__name__)` no topo do módulo. Nada de handlers,
   formatters, `basicConfig` ou um `get_logger` próprio.
2. Mensagem = só o fato de domínio, em `key=value`, sempre lazy:
   `logger.info("provider=%s latency_ms=%.0f tokens_in=%d", p, ms, tin)`.
   NÃO coloque tenant_id / timestamp / channel na mensagem — o host injeta
   via contextvars + Filter no root logger (carimbado em todo LogRecord).
3. Níveis:
   - **ERROR**  → nunca aqui; erro fatal vira exceção e propaga (host loga ERROR).
   - **WARNING**→ condição recuperada/tratada (fallback, parse coercion, verify falho).
   - **INFO**   → marco caro e raro; NÃO happy-path por request.
   - **DEBUG**  → trace de fidelidade total (prompt/raw/scores). DEV-ONLY,
                  jamais ligado em produção multi-tenant. Redija secrets (apikey).
4. Controle de nível é por pacote: `logging.getLogger("<pacote>").setLevel(...)`.

O host anexa o handler (TenantFilter + JsonFormatter) ao root logger real;
veja `cogno/core/logging.py` no host como referência.

## Nota específica do cogno-homeo

Estado **operacional** (transições de circuit breaker, failover, retries) é
reportado pelo seam estruturado **`MetricsSink`** — não pelo `logging`. O host
pluga seu backend (Loki/Datadog/Prometheus) no `MetricsSink` e recebe esses
eventos como métricas. O `logging` aqui é **só DEBUG humano** (rastrear uma
tentativa individual durante desenvolvimento); não há WARNING/INFO duplicando o
que o `MetricsSink` já entrega.
