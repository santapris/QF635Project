# Technology Stack

## Core Language

- **Python 3.12+**: asyncio, type hints, match statements
- **C++ 20** (Phase 7+): Feed handler and OMS hot paths

## Messaging / Event Streaming

| Option          | Use Case                          | Notes                    |
|-----------------|-----------------------------------|--------------------------|
| **Kafka**       | Production event bus              | Best durability + replay |
| Redis Streams   | Low-latency pub/sub               | Good Phase 3 option      |
| asyncio.Queue   | Single-process prototype          | Phase 1 only             |
| ZeroMQ          | Ultra-low latency IPC             | C++ bridge               |

**Recommendation**: Start with `asyncio.Queue`, migrate to Kafka at Phase 3.

## Databases

| Need               | Technology                          | Notes                               |
|--------------------|-------------------------------------|-------------------------------------|
| Time-series data   | **TimescaleDB**                     | PostgreSQL-compatible, hypertables  |
| Relational data    | **PostgreSQL 16**                   | Orders, fills, config               |
| Cache / hot state  | **Redis 7**                         | Position cache, session state       |
| Object storage     | **MinIO** (local) / **S3** (cloud)  | Parquet files, backtest results     |

## Serialization

| Use Case          | Format                       | Library             |
|-------------------|------------------------------|---------------------|
| Internal events   | **Pydantic v2**              | Fast validation     |
| Kafka messages    | **Avro** or **MessagePack**  | Compact binary      |
| Config files      | **TOML**                     | `tomllib` (stdlib)  |
| Market data files | **Parquet**                  | `pyarrow`           |

## Observability Stack

| Concern     | Technology                            |
|-------------|---------------------------------------|
| Metrics     | **Prometheus** + `prometheus_client`  |
| Dashboards  | **Grafana**                           |
| Logging     | **structlog** → stdout → Loki         |
| Tracing     | **OpenTelemetry** → Jaeger            |
| Alerting    | **Alertmanager** + PagerDuty/Slack    |

## ML / Research

- **Jupyter** + **pandas** + **numpy** for research
- **Polars** for high-performance data processing
- **vectorbt** or custom backtest engine for rapid strategy evaluation

## Python Dependencies

```toml
[project]
name = "trading"
requires-python = ">=3.12"
dependencies = [
    "pydantic>=2.7",
    "pydantic-settings>=2.3",
    "structlog>=24.0",
    "websockets>=12.0",
    "asyncpg>=0.29",
    "sqlalchemy[asyncio]>=2.0",
    "redis[hiredis]>=5.0",
    "aiokafka>=0.11",
    "pyarrow>=16.0",
    "prometheus-client>=0.20",
    "opentelemetry-sdk>=1.24",
]

[project.optional-dependencies]
dev = ["ruff", "mypy", "pytest", "pytest-asyncio", "pytest-cov", "hypothesis"]
backtest = ["polars>=0.20", "pyarrow>=16.0", "plotly>=5.0"]
```

## Deployment / Orchestration

| Phase | Tool                          |
|-------|-------------------------------|
| 1–2   | Local Python process          |
| 3–5   | Docker Compose                |
| 6–7   | Kubernetes (k3s or EKS/GKE)   |
| 7+    | Kubernetes + Helm + ArgoCD    |
