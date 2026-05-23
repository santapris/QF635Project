# Storage & Persistence

## Storage Decision Matrix

| Data Type              | Storage            | Retention   |
|------------------------|--------------------|-------------|
| Market ticks           | TimescaleDB        | 90 days     |
| OHLCV bars             | TimescaleDB + S3   | Unlimited   |
| Orders / fills         | PostgreSQL         | Unlimited   |
| Event stream           | Kafka              | 7 days      |
| Event archive          | S3/MinIO           | Unlimited   |
| Position snapshots     | Redis + PostgreSQL | Hot: 24h    |
| Backtest results       | S3/MinIO           | Unlimited   |
| Configuration          | PostgreSQL         | Versioned   |
| Audit log              | PostgreSQL + S3    | Never deleted |

## Responsibilities

- Persist all events to the event store (Kafka / PostgreSQL)
- Store time-series market data (TimescaleDB hypertables)
- Cache hot data: order book snapshots, position summaries (Redis)
- Archive cold data: historical bars, backtest results (S3/MinIO)
- Schema migration management (Alembic)

## Module Structure

```
persistence/
├── timescale.py        # TimescaleDB hypertable adapter
├── postgres.py         # Orders, fills, config, audit log
├── redis_cache.py      # Hot state cache
└── s3_store.py         # Parquet archive and backtest results
```

## Technology Choices

| Need               | Technology                          | Notes                               |
|--------------------|-------------------------------------|-------------------------------------|
| Time-series data   | **TimescaleDB**                     | PostgreSQL-compatible, hypertables  |
| Relational data    | **PostgreSQL 16**                   | Orders, fills, config               |
| Cache / hot state  | **Redis 7**                         | Position cache, session state       |
| Object storage     | **MinIO** (local) / **S3** (cloud)  | Parquet files, backtest results     |

## Environment Variables

| Variable       | Required | Description                  |
|----------------|----------|------------------------------|
| `DATABASE_URL` | Yes      | PostgreSQL connection string |
| `REDIS_URL`    | Yes      | Redis connection string      |
| `KAFKA_BOOTSTRAP` | Phase 3+ | Kafka broker addresses    |
