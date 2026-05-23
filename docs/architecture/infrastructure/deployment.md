# Deployment

## Local Development Setup

```bash
# Prerequisites: Python 3.12+, uv, Docker Desktop

# 1. Clone and install
git clone https://github.com/your-org/trading-platform
cd trading-platform
uv sync

# 2. Copy environment config
cp .env.example .env
# Edit .env: add API keys, set ENVIRONMENT=development

# 3. Start infrastructure (Postgres, Redis, Kafka)
docker compose up -d postgres redis kafka zookeeper

# 4. Run migrations
uv run alembic upgrade head

# 5. Run in paper trading mode
uv run python -m trading.runners.live_runner --config config/development.toml

# 6. Run backtests
uv run python scripts/run_backtest.py --strategy momentum --start 2023-01-01 --end 2024-01-01
```

## Docker Compose

```yaml
services:
  postgres:
    image: timescale/timescaledb:latest-pg16
    volumes: [postgres_data:/var/lib/postgresql/data]
    environment: {POSTGRES_DB: trading, POSTGRES_PASSWORD: ${DB_PASSWORD}}

  redis:
    image: redis:7-alpine
    command: redis-server --appendonly yes

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on: [zookeeper]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092

  prometheus:
    image: prom/prometheus:latest
    volumes: [./prometheus.yml:/etc/prometheus/prometheus.yml]

  grafana:
    image: grafana/grafana:latest
    volumes: [./grafana/dashboards:/etc/grafana/dashboards]
    depends_on: [prometheus]

  feed-handler:
    build: {context: .., dockerfile: docker/Dockerfile.trading}
    command: python -m trading.runners.feed_handler_runner
    environment: {EXCHANGE: binance, EVENT_BUS: kafka}
    depends_on: [kafka]

  strategy-engine:
    build: {context: .., dockerfile: docker/Dockerfile.trading}
    command: python -m trading.runners.strategy_runner
    depends_on: [kafka, redis]

  risk-engine:
    build: {context: .., dockerfile: docker/Dockerfile.trading}
    command: python -m trading.runners.risk_runner
    depends_on: [kafka, redis, postgres]

  oms:
    build: {context: .., dockerfile: docker/Dockerfile.trading}
    command: python -m trading.runners.oms_runner
    depends_on: [kafka, postgres]

  position-engine:
    build: {context: .., dockerfile: docker/Dockerfile.trading}
    command: python -m trading.runners.position_runner
    depends_on: [kafka, postgres, redis]

volumes:
  postgres_data:
```

## CI/CD Pipeline (GitHub Actions)

```yaml
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres: {image: timescale/timescaledb:latest-pg16}
      redis: {image: redis:7-alpine}
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: uv sync
      - run: uv run ruff check src/ tests/
      - run: uv run ruff format --check src/ tests/
      - run: uv run mypy src/
      - run: uv run pytest tests/unit/ tests/integration/ -v --cov=trading

  backtest-regression:
    needs: test
    steps:
      - run: uv run pytest tests/system/ -v  # Golden output tests

  build:
    needs: [test, backtest-regression]
    steps:
      - run: docker build -t trading-platform:${{ github.sha }} -f docker/Dockerfile.trading .
      - run: docker push ghcr.io/${{ github.repository }}/trading-platform:${{ github.sha }}

  deploy-staging:
    needs: build
    if: github.ref == 'refs/heads/main'
    steps:
      - run: argocd app sync trading-staging
```

## Production Deployment Evolution

| Phase     | Infrastructure                                  | When to Use                        |
|-----------|-------------------------------------------------|------------------------------------|
| Phase 1–2 | Single VM, process supervisor (supervisord)     | Development, initial live testing  |
| Phase 3–5 | Docker Compose on dedicated VM                  | Small live deployment              |
| Phase 6   | Docker Compose + managed DBs (RDS, ElastiCache) | Production, small scale            |
| Phase 7   | Kubernetes (EKS/GKE) + Helm + ArgoCD            | Multi-strategy, high availability  |

## When Kubernetes Becomes Necessary

Adopt Kubernetes when you need:
- Independent scaling of components (feed handler vs strategy engine)
- Automated pod restart and health checking
- Rolling deployments with zero downtime
- Multiple replicas of stateless components (monitoring, API servers)
- Resource quotas per component
- Multi-region / multi-AZ deployment

**Rule of thumb**: Adopt Kubernetes when running 3+ strategies across 2+ exchanges in production.

## Environment Variables Reference

| Variable              | Required | Description                              |
|-----------------------|----------|------------------------------------------|
| `ENVIRONMENT`         | Yes      | `development` / `staging` / `production` |
| `DATABASE_URL`        | Yes      | PostgreSQL connection string             |
| `REDIS_URL`           | Yes      | Redis connection string                  |
| `KAFKA_BOOTSTRAP`     | Phase 3+ | Kafka broker addresses                   |
| `BINANCE_API_KEY`     | Yes      | Binance API key                          |
| `BINANCE_API_SECRET`  | Yes      | Binance API secret                       |
| `SLACK_WEBHOOK_URL`   | Phase 5+ | Slack alerting webhook                   |
| `KILL_SWITCH_SECRET`  | Phase 5+ | Auth token for manual kill switch        |
