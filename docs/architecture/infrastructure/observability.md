# Observability

## Structured Logging

```python
import structlog

logger = structlog.get_logger()

logger.info(
    "order_submitted",
    client_order_id=str(order.client_order_id),
    instrument_id=order.instrument_id,
    quantity=str(order.quantity),
    side=order.side,
    latency_us=latency_microseconds,
)
```

All log output is JSON, shipped to Loki via Promtail.

## Prometheus Metrics

```python
from prometheus_client import Counter, Histogram, Gauge

signal_to_order_latency = Histogram(
    "signal_to_order_latency_us",
    "Latency from signal generation to order submission (microseconds)",
    buckets=[10, 50, 100, 500, 1000, 5000, 10000],
)

fills_total = Counter("fills_total", "Total fills", ["instrument", "side", "strategy"])
rejected_signals_total = Counter(
    "rejected_signals_total", "Signals rejected by risk", ["strategy", "reason"]
)

open_positions = Gauge("open_positions", "Current open positions count")
portfolio_pnl = Gauge("portfolio_pnl_usd", "Current portfolio PnL in USD")
```

## Grafana Dashboards

| Dashboard            | Key Panels                                          |
|----------------------|-----------------------------------------------------|
| **Trading Overview** | PnL, positions, fill rate, open orders              |
| **System Health**    | Latency histograms, queue depths, reconnects        |
| **Risk Monitor**     | Exposure by instrument, drawdown meter, kill switch |
| **Feed Quality**     | Tick rates, gap counts, staleness                   |
| **Backtest Results** | Sharpe, max DD, win rate, exposure timeline         |

## Latency SLOs

| Stage                         | Target    |
|-------------------------------|-----------|
| Feed receive → Bus publish    | < 100µs   |
| Bus publish → Strategy        | < 50µs    |
| Strategy → Signal emit        | < 1ms     |
| Signal → Risk decision        | < 500µs   |
| Risk decision → Order submit  | < 500µs   |
| Order submit → Exchange ack   | < 10ms    |

## Module Structure

```
monitoring/
├── logger.py           # Structured JSON logger (structlog)
├── metrics.py          # Prometheus metric definitions
├── tracing.py          # OpenTelemetry tracer setup
├── alerting.py         # Alert rule engine and dispatcher
└── health.py           # Health check endpoints
```

## Stack

| Concern     | Technology                            |
|-------------|---------------------------------------|
| Metrics     | **Prometheus** + `prometheus_client`  |
| Dashboards  | **Grafana**                           |
| Logging     | **structlog** → stdout → Loki         |
| Tracing     | **OpenTelemetry** → Jaeger            |
| Alerting    | **Alertmanager** + PagerDuty/Slack    |
