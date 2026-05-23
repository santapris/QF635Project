# Implementation Roadmap

## Phase 1 — Minimal Single-Process Prototype

**Objectives**: Validate core event flow end-to-end on paper trading

**Architecture decisions**
- Single Python process with asyncio
- `asyncio.Queue` as event bus
- No external dependencies (no DB, no Kafka)
- In-memory state only

**Deliverables**
- `core/events.py` — all event dataclasses
- `event_bus/asyncio_bus.py`
- `feed_handler/connectors/binance.py` — WebSocket connection, normalization
- `strategy/examples/momentum.py` — basic signal logic
- `risk/` — minimal position limit rule only
- `oms/engine.py` — order lifecycle (paper only)
- `order_gateways/simulation/` — simulated order gateway
- `runners/live_runner.py` — wires everything together
- Console logging only

**Testing strategy**: Unit tests for each component in isolation using `MemoryBus`

**Accepted technical debt**: No persistence, no reconnect logic, no metrics

---

## Phase 2 — Real Exchange Connectivity

**Objectives**: Connect to a real exchange in paper trading mode

**Architecture decisions**
- Add `order_gateways/binance/` with testnet support
- Implement WebSocket reconnect with backoff
- Add order signing and authentication

**Deliverables**
- `order_gateways/binance/ws_order_gateway.py`
- `feed_handler/connectors/` — production-quality with reconnect
- Integration tests against Binance testnet
- Basic structlog logging

**Testing strategy**: Integration tests replay captured WebSocket sessions

---

## Phase 3 — Persistence + Event Bus

**Objectives**: Survive restarts; introduce Kafka for scalability

**Architecture decisions**
- PostgreSQL for orders/fills (via SQLAlchemy + asyncpg)
- TimescaleDB for tick data
- Kafka replaces asyncio.Queue as event bus
- Redis for position cache

**Deliverables**
- `persistence/postgres.py`, `persistence/timescale.py`, `persistence/redis_cache.py`
- `event_bus/kafka_bus.py`
- Alembic migrations
- State recovery on startup (replay last N events from Kafka)
- Docker Compose with all infrastructure services

**Testing strategy**: Integration tests with Docker Compose spin-up

---

## Phase 4 — Backtesting Engine

**Objectives**: Run strategies on historical data with identical code paths

**Architecture decisions**
- `SimulatedClock` injected in place of `LiveClock`
- `MemoryBus` (synchronous) for maximum backtest speed
- Order book walk slippage model
- Parquet files for historical data

**Deliverables**
- `backtest/` complete module
- `scripts/download_historical.py`
- `runners/backtest_runner.py`
- HTML tearsheet report (PnL, Sharpe, max drawdown, trade list)
- Verified: same strategy code, same results on re-run (determinism test)

**Testing strategy**: "Golden output" tests — backtest results must match stored expected output exactly

---

## Phase 5 — Risk Controls

**Objectives**: Production-grade risk management

**Architecture decisions**
- Full risk rule suite (position, notional, drawdown, concentration, rate-of-loss)
- Kill switch with audit log
- Pre-trade and post-trade risk checks

**Deliverables**
- `risk/rules/` — all rule implementations
- Kill switch API (REST endpoint for manual trigger)
- Risk configuration schema with validation
- Alerting integration (Slack webhook)

**Testing strategy**: Chaos tests — deliberately breach limits and verify system response

---

## Phase 6 — Dashboard / Monitoring

**Objectives**: Full observability stack

**Architecture decisions**
- Prometheus + Grafana + Loki + Alertmanager stack
- All components emit structured logs and metrics
- Pre-built Grafana dashboards as code (JSON provisioning)

**Deliverables**
- `monitoring/` complete module
- Grafana dashboard JSON files in `deploy/grafana/`
- Alertmanager rules for critical conditions
- Runbooks for all alerts in `docs/runbooks/`
- Latency SLO dashboards

**Testing strategy**: Load tests to verify metrics accuracy under high throughput

---

## Phase 7 — Multi-Strategy / Multi-Asset

**Objectives**: Run multiple strategies simultaneously across multiple exchanges

**Architecture decisions**
- Strategy isolation: separate Kafka consumer groups per strategy
- Per-strategy risk limits and capital allocation
- Kubernetes deployment (one pod per component type)
- Portfolio-level risk aggregation across strategies
- Begin C++ migration for feed handler (if latency requires it)

**Deliverables**
- `strategy/registry.py` — multi-strategy lifecycle management
- Portfolio-level risk rules
- Helm chart for Kubernetes deployment
- CI/CD pipeline (GitHub Actions → ArgoCD)
- Performance benchmarks vs. C++ baseline

**Technical debt resolution**: Address all TODO items from Phases 1–6; full type coverage; 80%+ test coverage
