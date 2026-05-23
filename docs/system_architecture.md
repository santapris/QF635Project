# Algorithmic Trading Platform — System Architecture & Implementation Plan

> **Version**: 1.0.0  
> **Status**: Living Document  
> **Language**: Python-primary, C++ migration path included  
> **Paradigm**: Event-Driven, Event-Sourced, Modular Microservices

---

## Table of Contents

1.  [High-Level Architecture](#1-high-level-architecture)
2.  [Core Components](#2-core-components)
3.  [Architecture Principles](#3-architecture-principles)
4.  [Technology Stack Recommendations](#4-technology-stack-recommendations)
5.  [Project Structure](#5-project-structure)
6.  [Internal APIs and Data Contracts](#6-internal-apis-and-data-contracts)
7.  [Concurrency and Runtime Model](#7-concurrency-and-runtime-model)
8.  [Backtesting Design](#8-backtesting-design)
9.  [Risk and Reliability](#9-risk-and-reliability)
10. [Observability](#10-observability)
11. [Agile Implementation Roadmap](#11-agile-implementation-roadmap)
12. [Deployment Strategy](#12-deployment-strategy)
13. [Engineering Best Practices](#13-engineering-best-practices)

---

## 1. High-Level Architecture

### 1.1 Overview

The platform follows a **layered event-driven architecture** where every state transition is triggered by an immutable event. Components never call each other directly — they publish and subscribe to a central event bus. This enables:

- Full deterministic replay of any historical period
- Clean separation of concerns between strategy, risk, and execution
- Horizontal scaling of independent services
- Identical code paths in backtesting and live trading

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL WORLD                                  │
│    Exchange WebSocket APIs    REST APIs    FIX OrderGateways    Data Vendors │
└─────────────────────┬───────────────────────────────────┬───────────────┘
                      │ raw market data                   │ order responses
                      ▼                                   ▲
┌─────────────────────────────────────────────────────────────────────────┐
│                      INGESTION / GATEWAY LAYER                          │
│  ┌──────────────────┐  ┌───────────────────┐  ┌──────────────────────┐  │
│  │   Feed Handler   │  │  Exchange OrderGateway │  │  Broker OrderGateway      │  │
│  │  (normalisation) │  │  (order routing)  │  │  (FIX/REST adapter)  │  │
│  └────────┬─────────┘  └────────┬──────────┘  └──────────┬───────────┘  │
└───────────┼─────────────────────┼────────────────────────┼──────────────┘
            │NormalizedMarketEvent│ OrderEvent/FillEvent   │
            ▼                     ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         EVENT BUS / BROKER                              │
│              (Kafka in production / asyncio.Queue in prototype)         │
│  Topics: market-data | orders | fills | signals | risk | positions      │
└──┬──────────────────┬──────────────────┬──────────────────┬─────────────┘
   │                  │                  │                  │
   ▼                  ▼                  ▼                  ▼
┌──────────┐   ┌───────────────┐   ┌──────────────┐  ┌──────────────────┐
│ Strategy │   │     Risk      │   │  Execution / │  │   Position/PnL   │
│ / Signal │   │  Management   │   │     OMS      │  │     Engine       │
│  Engine  │   │    Engine     │   │              │  │                  │
└────┬─────┘   └──────┬────────┘   └──────┬───────┘  └──────────────────┘
     │ SignalEvent    │ RiskDecision      │ OrderEvent
     └────────────────┴───────────────────┴──────────────────────────────▶
                                                            back to bus
┌─────────────────────────────────────────────────────────────────────────┐
│                     INFRASTRUCTURE LAYER                                │
│  TimescaleDB │ PostgreSQL │ Redis │ S3/MinIO │ Prometheus │ Grafana     │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Event Flow — Live Trading

```
1.  Exchange WebSocket  ──▶  Feed Handler
2.  Feed Handler        ──▶  EVENT BUS (topic: market-data)
3.  Strategy Engine    ◀──   EVENT BUS (subscribe: market-data)
4.  Strategy Engine     ──▶  EVENT BUS (topic: signals)
5.  Risk Engine        ◀──   EVENT BUS (subscribe: signals + positions)
6.  Risk Engine         ──▶  EVENT BUS (topic: risk-decisions)
7.  OMS                ◀──   EVENT BUS (subscribe: risk-decisions)
8.  OMS                 ──▶  Exchange OrderGateway  ──▶  Exchange
9.  Exchange           ──▶  Exchange OrderGateway   ──▶  EVENT BUS (topic: fills)
10. Position Engine    ◀──   EVENT BUS (subscribe: fills)
11. Position Engine     ──▶  EVENT BUS (topic: positions)
12. Risk Engine        ◀──   EVENT BUS (subscribe: positions)  [feedback loop]
```

### 1.3 Event Flow — Backtesting

```
1.  Historical DB / Parquet Files  ──▶  Replay Engine
2.  Replay Engine                  ──▶  simulated EVENT BUS (same interface)
3.  Strategy Engine (unchanged)   ◀──   simulated EVENT BUS
4.  Risk Engine (unchanged)       ◀──   simulated EVENT BUS
5.  Simulated OMS                  ──▶  Simulated Exchange (slippage/latency model)
6.  Simulated Exchange             ──▶  simulated EVENT BUS (fills)
7.  Position Engine (unchanged)   ◀──   simulated EVENT BUS
```

> **Key invariant**: Strategy, Risk, and Position engines are **identical** in live and backtest modes. Only the bus implementation and exchange order_gateway differ.

### 1.4 Concurrency Model

- **Single process (Phase 1)**: `asyncio` event loop, coroutine-per-component
- **Multi-process (Phase 3+)**: Each component is a separate process; Kafka as bus
- **Multi-machine (Phase 7+)**: Kubernetes pods, each component independently scaled
- **C++ migration (Phase 7+)**: Feed handler and OMS hot paths replaced with C++ processes publishing to the same Kafka topics

---

## 2. Core Components

### 2.1 Feed Handler (Market Data Ingestion)

**Responsibilities**  
- Connect to exchange WebSocket and REST endpoints
- Reconnect with exponential backoff on disconnection
- Normalize raw exchange-specific data into a canonical event schema
- Sequence and timestamp all incoming data (exchange time + local receive time)
- Publish `NormalizedMarketEvent` to the event bus
- Maintain an in-memory order book (bid/ask ladder)

**Inputs**: Raw WebSocket frames, REST poll responses  
**Outputs**: `TickEvent`, `TradeEvent`, `OrderBookEvent`, `FundingRateEvent`

**Internal Modules**

```
feed_handler/
├── connector.py        # WebSocket/REST connection lifecycle
├── normalizer.py       # Exchange-specific format → canonical schema
├── order_book.py       # L2/L3 order book reconstruction
├── sequencer.py        # Gap detection, sequence number tracking
└── publisher.py        # Publishes to event bus
```

**Failure Handling**
- WebSocket disconnect → exponential backoff reconnect (1s, 2s, 4s, max 60s)
- Gap in sequence numbers → request snapshot, re-subscribe
- Stale data detection → heartbeat check every N seconds
- Circuit breaker: if reconnect fails > 10 times → raise `FeedUnavailableAlert`

**Scaling**  
- One Feed Handler process per exchange
- For ultra-low latency: replace Python with C++ using Boost.Asio; publish to same Kafka topic

---

### 2.2 Exchange / Broker OrderGateways

**Responsibilities**
- Translate internal `OrderRequest` events into exchange-specific API calls
- Handle authentication (API keys, HMAC signing, OAuth)
- Map exchange order IDs to internal order IDs
- Receive fills and cancellations; publish to event bus
- Rate limit management and throttle queuing

**Inputs**: `OrderRequest`, `CancelRequest`, `AmendRequest`  
**Outputs**: `OrderAcknowledged`, `FillEvent`, `OrderCancelled`, `OrderRejected`

**Supported OrderGateway Types**

| OrderGateway Type    | Protocol        | Latency Profile |
|-----------------|-----------------|-----------------|
| Direct Exchange | WebSocket / FIX | Ultra-low       |
| Prime Broker    | FIX 4.2/4.4     | Low             |
| REST Broker     | HTTPS REST      | Medium          |
| Simulation      | In-process      | Zero            |

**Internal Modules**

```
order_gateways/
├── base.py             # AbstractOrderGateway interface
├── binance/
│   ├── auth.py
│   ├── ws_order_gateway.py
│   └── rest_order_gateway.py
├── interactive_brokers/
│   └── fix_order_gateway.py
└── simulation/
    └── simulated_order_gateway.py
```

**Failure Handling**
- Order rejection → publish `OrderRejected`, notify OMS
- Network timeout → idempotent retry with dedup by client order ID
- Partial fill → track remaining quantity in OMS state

---

### 2.3 Event Bus / Messaging Layer

**Responsibilities**
- Decouple all components via pub/sub
- Guarantee ordered delivery within a topic partition
- Support both in-process (asyncio queue) and out-of-process (Kafka) modes
- Persist all events for replay (Kafka log retention / event store)
- Provide backpressure signaling to publishers

**Interface (AbstractEventBus)**

```python
class AbstractEventBus(Protocol):
    async def publish(self, topic: str, event: BaseEvent) -> None: ...
    async def subscribe(self, topic: str, handler: Callable) -> None: ...
    async def subscribe_many(self, topics: list[str], handler: Callable) -> None: ...
```

**Topic Design**

| Topic           | Producers              | Consumers                        |
|-----------------|------------------------|----------------------------------|
| `market-data`   | Feed Handler           | Strategy, Risk, Position         |
| `signals`       | Strategy Engine        | Risk Engine                      |
| `risk-decisions`| Risk Engine            | OMS                              |
| `orders`        | OMS                    | Exchange OrderGateway, Monitoring     |
| `fills`         | Exchange OrderGateway       | OMS, Position Engine, Monitoring |
| `positions`     | Position Engine        | Risk Engine, Dashboard           |
| `alerts`        | Risk, Feed, OMS        | Monitoring, Dashboard            |

**Implementations**

```
event_bus/
├── base.py             # Protocol / interface definition
├── asyncio_bus.py      # Single-process asyncio.Queue implementation
├── kafka_bus.py        # Production Kafka implementation
└── memory_bus.py       # In-memory bus for unit tests
```

---

### 2.4 Strategy / Signal Engine

**Responsibilities**
- Subscribe to market data events
- Maintain strategy-local state (indicators, positions, parameters)
- Generate `SignalEvent` (buy/sell/close with target size and rationale)
- Support multiple simultaneous strategies with isolated state
- Hot-reload strategy parameters without restart

**Inputs**: `TickEvent`, `TradeEvent`, `OrderBookEvent`, `PositionUpdateEvent`  
**Outputs**: `SignalEvent`

**Internal Modules**

```
strategy/
├── base.py             # AbstractStrategy interface
├── context.py          # Strategy execution context (clock, portfolio view)
├── indicator_lib/      # Technical indicators (EMA, RSI, VWAP, etc.)
│   ├── moving_averages.py
│   ├── momentum.py
│   └── microstructure.py
├── registry.py         # Strategy registration and lifecycle management
└── examples/
    ├── momentum.py
    ├── mean_reversion.py
    └── market_making.py
```

**AbstractStrategy Interface**

```python
class AbstractStrategy(ABC):
    strategy_id: str
    instruments: list[str]

    @abstractmethod
    async def on_tick(self, event: TickEvent) -> list[SignalEvent]: ...

    @abstractmethod
    async def on_fill(self, event: FillEvent) -> None: ...

    @abstractmethod
    async def on_position_update(self, event: PositionUpdateEvent) -> None: ...
```

**Scaling**  
- Each strategy runs in its own asyncio task (single process)
- For CPU-intensive strategies: run in separate process, connected via Kafka
- Strategy parameters stored in Redis for hot-reload

---

### 2.5 Risk Management Engine

**Responsibilities**
- Validate every `SignalEvent` against pre-trade risk rules before order submission
- Track real-time exposure per instrument, sector, and portfolio
- Enforce position limits, drawdown limits, concentration limits
- Trigger kill switch on breach of critical thresholds
- Publish `RiskDecision` (approved/rejected with reason)

**Inputs**: `SignalEvent`, `PositionUpdateEvent`, `FillEvent`  
**Outputs**: `RiskDecision`, `KillSwitchEvent`, `RiskAlertEvent`

**Risk Rules (pluggable)**

```
risk/
├── engine.py           # Orchestrates rule evaluation
├── state.py            # Real-time exposure tracker
├── rules/
│   ├── base.py         # AbstractRiskRule interface
│   ├── position_limits.py
│   ├── notional_limits.py
│   ├── drawdown_limits.py
│   ├── concentration.py
│   ├── rate_of_loss.py
│   └── kill_switch.py
└── models.py           # RiskConfig, ExposureSnapshot
```

**Risk Rule Interface**

```python
class AbstractRiskRule(ABC):
    @abstractmethod
    def evaluate(
        self,
        signal: SignalEvent,
        portfolio_state: PortfolioState,
        config: RiskConfig,
    ) -> RiskRuleResult: ...
    # RiskRuleResult: passed=True/False, reason=str, severity=INFO/WARN/BLOCK/KILL
```

**Kill Switch**  
- Triggered by: drawdown > N%, loss rate > X/min, manual command, system error
- Action: cancel all open orders, block new signals, alert operators, log to audit trail
- Reset requires explicit operator confirmation (never auto-reset in production)

---

### 2.6 Position / PnL Engine

**Responsibilities**
- Maintain real-time inventory per instrument and strategy
- Calculate unrealized and realized PnL using mark-to-market prices
- Track cost basis using configurable accounting method (FIFO, LIFO, weighted average)
- Publish `PositionUpdateEvent` on every fill or price update
- Reconcile against exchange positions periodically

**Inputs**: `FillEvent`, `TickEvent` (for MTM), `PortfolioSnapshotRequest`  
**Outputs**: `PositionUpdateEvent`, `PnLSnapshotEvent`

**Internal Modules**

```
position/
├── engine.py           # Core position tracking logic
├── pnl_calculator.py   # Realized/unrealized PnL computation
├── reconciler.py       # Exchange position reconciliation
├── accounting.py       # FIFO/LIFO/weighted average cost basis
└── models.py           # Position, PnLSnapshot data models
```

---

### 2.7 Execution / Order Management System (OMS)

**Responsibilities**
- Receive approved `RiskDecision` events and convert to exchange orders
- Maintain a lifecycle state machine for every order
- Track partial fills and aggregate fill quantities
- Route orders to the correct exchange order_gateway
- Support order types: market, limit, stop-limit, IOC, FOK, TWAP, VWAP

**Order State Machine**

```
PENDING_NEW
    │
    ▼
ACKNOWLEDGED ───────────────▶ REJECTED
    │
    ├──▶ PARTIALLY_FILLED ──▶ FILLED
    │
    ├──▶ PENDING_CANCEL ──▶ CANCELLED
    │
    └──▶ FILLED
```

**Internal Modules**

```
oms/
├── engine.py           # Order lifecycle management
├── router.py           # Order routing to correct order_gateway
├── state_machine.py    # Order FSM transitions
├── execution_algos/    # TWAP, VWAP, Iceberg, Sniper
│   ├── base.py
│   ├── twap.py
│   └── vwap.py
└── models.py           # Order, Fill data models
```

---

### 2.8 Backtesting / Replay Engine

**Responsibilities**
- Load historical market data from storage (Parquet, TimescaleDB)
- Replay events in chronological order at configurable speed (1x, 10x, max speed)
- Simulate a virtual exchange with configurable slippage, latency, and fill models
- Use identical strategy/risk/position code as live trading
- Produce backtest reports with PnL, Sharpe, drawdown, fill statistics

**Internal Modules**

```
backtest/
├── engine.py           # Main replay loop
├── clock.py            # Simulated clock (controls event timestamps)
├── data_loader.py      # Historical data loading and streaming
├── simulated_exchange.py  # Virtual order book and fill simulation
├── slippage_models.py  # Linear, square-root, market-impact models
├── latency_models.py   # Configurable order-to-fill latency
└── report.py           # Performance metrics and tearsheet
```

**Determinism Guarantees**
- Fixed random seed for all stochastic models
- Snapshots stored every N events for checkpointing
- All parameters logged at run start; full reproducibility from seed + config

---

### 2.9 Monitoring / Logging / Alerting

**Responsibilities**
- Collect structured logs from all components
- Expose Prometheus metrics (latency histograms, counters, gauges)
- Send alerts via PagerDuty/Slack/email on critical events
- Provide OpenTelemetry traces for cross-component request tracking

**Internal Modules**

```
monitoring/
├── logger.py           # Structured JSON logger (structlog)
├── metrics.py          # Prometheus metric definitions
├── tracing.py          # OpenTelemetry tracer setup
├── alerting.py         # Alert rule engine and dispatcher
└── health.py           # Health check endpoints
```

---

### 2.10 Persistence / Storage Layer

**Responsibilities**
- Persist all events to the event store (Kafka / PostgreSQL)
- Store time-series market data (TimescaleDB)
- Cache hot data: order book snapshots, position summaries (Redis)
- Archive cold data: historical bars, backtest results (S3/MinIO)
- Schema migration management (Alembic)

**Storage Decision Matrix**

| Data Type              | Storage           | Retention   |
|------------------------|-------------------|-------------|
| Market ticks           | TimescaleDB       | 90 days     |
| OHLCV bars             | TimescaleDB + S3  | Unlimited   |
| Orders / fills         | PostgreSQL        | Unlimited   |
| Event stream           | Kafka             | 7 days      |
| Event archive          | S3/MinIO          | Unlimited   |
| Position snapshots     | Redis + PostgreSQL| Hot: 24h    |
| Backtest results       | S3/MinIO          | Unlimited   |
| Configuration          | PostgreSQL        | Versioned   |

---

### 2.11 User Interface Dashboard

**Responsibilities**
- Real-time visualization of positions, PnL, open orders
- Strategy performance monitoring
- Risk exposure heatmaps
- System health indicators
- Manual order entry (operator override)
- Kill switch control panel

**Technology**: Grafana for metrics dashboards + lightweight React frontend for trading controls

---

## 3. Architecture Principles

### 3.1 Event Sourcing

Every state change in the system is derived from an **immutable, append-only event log**. No direct database mutations of business state. The current state of any entity (position, order, portfolio) is the fold (reduce) of all events in its stream.

```
current_position = reduce(apply_event, event_stream, initial_state)
```

Benefits:
- Full audit trail for regulatory compliance
- Complete system state reproducible from the event log alone
- Time-travel debugging: replay up to any point in time

### 3.2 Deterministic Replay

Given the same ordered sequence of market events and a fixed configuration, the system must produce **byte-for-byte identical** output on every run. This requires:

- No wall-clock time in strategy logic (use injected `Clock` abstraction)
- No external I/O in strategy (data injected via event bus)
- Fixed random seeds for any stochastic models
- No dependency on system locale, OS, or floating-point mode differences

### 3.3 Loose Coupling

Components communicate **exclusively** through the event bus. No direct function calls across component boundaries in production code. This means:

- Components can be replaced or upgraded independently
- New consumers can be added without modifying producers
- Testing is done by publishing test events, not by calling internal methods

### 3.4 Normalization of Exchange Data

Every exchange speaks a different dialect. The Feed Handler's sole job is to translate all dialects into a single canonical schema before the event enters the bus. No downstream component ever sees raw exchange data.

```
Binance "aggTrade" JSON  ──▶  FeedHandler ──▶  TradeEvent(canonical)
Coinbase "match" JSON    ──▶  FeedHandler ──▶  TradeEvent(canonical)
IB "RTVolume" string     ──▶  FeedHandler ──▶  TradeEvent(canonical)
```

### 3.5 Separation of Strategy / Risk / Execution

These three concerns must never be entangled:
- **Strategy**: decides *what* to trade (signal generation, no risk awareness)
- **Risk**: decides *whether* to trade (pre-trade validation, exposure management)
- **Execution**: decides *how* to trade (order type, routing, timing)

A strategy cannot send an order directly. It emits a signal. The signal passes through risk. Only risk-approved signals become orders.

### 3.6 Stateless vs Stateful Services

| Service        | Stateful? | State Location                             |
|----------------|-----------|--------------------------------------------|
| Feed Handler   | Yes       | In-process order book; rebuilt on restart  |
| Strategy Engine| Yes       | In-process + Redis (for hot reload)        |
| Risk Engine    | Yes       | In-process + Redis                         |
| OMS            | Yes       | In-process + PostgreSQL                    |
| Position Engine| Yes       | In-process + PostgreSQL                    |
| Event Bus      | Yes       | Kafka log                                  |
| Monitoring     | No        | Prometheus pull model                      |

Stateful services must handle restart gracefully by replaying recent events from Kafka or loading their last snapshot from the database.

### 3.7 Reproducibility: Backtest ↔ Live

The codebase enforces a strict rule: strategy logic files live in `strategy/` and are imported by both the live runner and the backtest runner. There is no "backtest version" of a strategy. The only difference is the runtime adapter (live bus vs simulated bus, real order_gateway vs simulated order_gateway).

---

## 4. Technology Stack Recommendations

### Core Language
- **Python 3.12+**: asyncio, type hints, match statements
- **C++ 20** (Phase 7+): Feed handler and OMS hot paths

### Messaging / Event Streaming
| Option        | Use Case                          | Notes                    |
|---------------|-----------------------------------|--------------------------|
| **Kafka**     | Production event bus              | Best durability + replay |
| Redis Streams | Low-latency pub/sub               | Good Phase 3 option      |
| asyncio.Queue | Single-process prototype          | Phase 1 only             |
| ZeroMQ        | Ultra-low latency IPC             | C++ bridge               |

**Recommendation**: Start with `asyncio.Queue`, migrate to Kafka at Phase 3.

### Databases
| Need               | Technology                         | Notes                              |
|--------------------|------------------------------------|------------------------------------|
| Time-series data   | **TimescaleDB**                    | PostgreSQL-compatible, hypertables |
| Relational data    | **PostgreSQL 16**                  | Orders, fills, config              |
| Cache / hot state  | **Redis 7**                        | Position cache, session state      |
| Object storage     | **MinIO** (local) / **S3** (cloud) | Parquet files, backtest results    |

### Serialization
| Use Case         | Format                      | Library            |
|------------------|-----------------------------|--------------------|
| Internal events  | **Pydantic v2**             | Fast validation    |
| Kafka messages   | **Avro** or **MessagePack** | Compact binary     |
| Config files     | **TOML**                    | `tomllib` (stdlib) |
| Market data files| **Parquet**                 | `pyarrow`          |

### Observability Stack
| Concern     | Technology                           |
|-------------|---------------------------------------|
| Metrics     | **Prometheus** + `prometheus_client` |
| Dashboards  | **Grafana**                          |
| Logging     | **structlog** → stdout → Loki        |
| Tracing     | **OpenTelemetry** → Jaeger           |
| Alerting    | **Alertmanager** + PagerDuty/Slack   |

### ML / Research
- **Jupyter** + **pandas** + **numpy** for research
- **Polars** for high-performance data processing
- **vectorbt** or custom backtest engine for rapid strategy evaluation

### Deployment / Orchestration
| Phase | Tool                          |
|-------|-------------------------------|
| 1–2   | Local Python process          |
| 3–5   | Docker Compose                |
| 6–7   | Kubernetes (k3s or EKS/GKE)   |
| 7+    | Kubernetes + Helm + ArgoCD    |

---

## 5. Project Structure

```
trading-platform/
│
├── pyproject.toml              # Single pyproject for monorepo (uv workspaces)
├── uv.lock                     # Lockfile (uv)
├── .env.example                # Example environment variables
├── Makefile                    # Dev shortcuts: make test, make lint, make run
│
├── config/                     # All configuration (TOML, YAML)
│   ├── base.toml               # Default configuration values
│   ├── development.toml        # Dev overrides
│   ├── production.toml         # Production overrides
│   └── strategies/             # Per-strategy parameter files
│       ├── momentum.toml
│       └── market_making.toml
│
├── src/
│   └── trading/                # Main Python package
│       │
│       ├── core/               # Shared primitives; NO business logic
│       │   ├── clock.py        # AbstractClock, LiveClock, SimulatedClock
│       │   ├── events.py       # All event dataclasses (canonical)
│       │   ├── instruments.py  # Instrument, InstrumentSpec
│       │   ├── types.py        # Type aliases (Price, Quantity, OrderId)
│       │   └── exceptions.py
│       │
│       ├── event_bus/          # Event bus abstraction + implementations
│       │   ├── base.py
│       │   ├── asyncio_bus.py
│       │   ├── kafka_bus.py
│       │   └── memory_bus.py
│       │
│       ├── feed_handler/       # Market data ingestion
│       │   ├── base.py
│       │   ├── order_book.py
│       │   ├── normalizer.py
│       │   └── connectors/
│       │       ├── binance.py
│       │       └── coinbase.py
│       │
│       ├── order_gateways/           # Exchange/broker order order_gateways
│       │   ├── base.py
│       │   ├── binance/
│       │   ├── interactive_brokers/
│       │   └── simulation/
│       │
│       ├── strategy/           # Strategy engine
│       │   ├── base.py
│       │   ├── context.py
│       │   ├── registry.py
│       │   ├── indicator_lib/
│       │   └── examples/
│       │
│       ├── risk/               # Risk management
│       │   ├── engine.py
│       │   ├── state.py
│       │   └── rules/
│       │
│       ├── position/           # Position and PnL tracking
│       │   ├── engine.py
│       │   ├── pnl_calculator.py
│       │   └── reconciler.py
│       │
│       ├── oms/                # Order management system
│       │   ├── engine.py
│       │   ├── router.py
│       │   ├── state_machine.py
│       │   └── execution_algos/
│       │
│       ├── backtest/           # Backtesting and replay engine
│       │   ├── engine.py
│       │   ├── clock.py
│       │   ├── data_loader.py
│       │   ├── simulated_exchange.py
│       │   ├── slippage_models.py
│       │   └── report.py
│       │
│       ├── persistence/        # Storage adapters
│       │   ├── timescale.py
│       │   ├── postgres.py
│       │   ├── redis_cache.py
│       │   └── s3_store.py
│       │
│       ├── monitoring/         # Observability
│       │   ├── logger.py
│       │   ├── metrics.py
│       │   ├── tracing.py
│       │   └── alerting.py
│       │
│       └── runners/            # Entry points / orchestration
│           ├── live_runner.py  # Live trading process
│           ├── backtest_runner.py
│           └── data_recorder.py
│
├── tests/
│   ├── unit/                   # Fast, isolated, no I/O
│   │   ├── test_order_book.py
│   │   ├── test_risk_rules.py
│   │   └── test_pnl_calculator.py
│   ├── integration/            # Component interactions, in-memory bus
│   │   ├── test_strategy_risk_flow.py
│   │   └── test_oms_order_gateway_flow.py
│   ├── system/                 # Full backtest runs
│   │   └── test_backtest_e2e.py
│   └── conftest.py             # Shared fixtures
│
├── scripts/                    # Operational scripts
│   ├── download_historical.py
│   ├── run_backtest.py
│   └── healthcheck.py
│
├── notebooks/                  # Research notebooks (never imported by src/)
│   ├── strategy_research.ipynb
│   └── data_exploration.ipynb
│
├── docker/
│   ├── Dockerfile.trading
│   ├── Dockerfile.backtest
│   └── docker-compose.yml
│
├── deploy/
│   ├── k8s/                    # Kubernetes manifests
│   └── helm/                   # Helm chart
│
└── docs/
    ├── architecture.md         # This document
    ├── runbooks/               # Operational runbooks
    └── adr/                    # Architecture Decision Records
```

**Key structural rules:**
- `core/` has zero external dependencies — only stdlib + pydantic
- `strategy/` must never import from `risk/`, `oms/`, or `order_gateways/` directly
- `notebooks/` is never imported by `src/` (enforced by ruff rule)
- Each subdirectory under `src/trading/` can evolve into its own microservice package

---

## 6. Internal APIs and Data Contracts

All events inherit from `BaseEvent` and are immutable pydantic models.

### 6.1 Base Event

```python
from pydantic import BaseModel, Field
from datetime import datetime
from uuid import UUID, uuid4
from enum import Enum

class BaseEvent(BaseModel):
    model_config = {"frozen": True}  # Immutable

    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    timestamp_exchange: datetime       # Exchange-provided timestamp
    timestamp_received: datetime       # Local receive timestamp
    timestamp_processed: datetime | None = None  # Set when consumed
```

### 6.2 Market Events

```python
from decimal import Decimal

class TickEvent(BaseEvent):
    event_type: str = "tick"
    instrument_id: str                 # Normalized: "BTC-USDT"
    bid_price: Decimal
    ask_price: Decimal
    bid_size: Decimal
    ask_size: Decimal
    exchange: str                      # "binance", "coinbase"
    sequence_number: int


class TradeEvent(BaseEvent):
    event_type: str = "trade"
    instrument_id: str
    price: Decimal
    quantity: Decimal
    side: Literal["buy", "sell"]       # Aggressor side
    trade_id: str
    exchange: str


class OrderBookEvent(BaseEvent):
    event_type: str = "order_book"
    instrument_id: str
    exchange: str
    bids: list[tuple[Decimal, Decimal]]  # (price, size), sorted desc
    asks: list[tuple[Decimal, Decimal]]  # (price, size), sorted asc
    is_snapshot: bool                    # True = full snapshot, False = delta
    sequence_number: int
```

### 6.3 Signal Event

```python
class SignalSide(str, Enum):
    BUY = "buy"
    SELL = "sell"
    CLOSE = "close"

class SignalEvent(BaseEvent):
    event_type: str = "signal"
    strategy_id: str
    instrument_id: str
    side: SignalSide
    target_quantity: Decimal           # Desired position change
    target_price: Decimal | None       # None = market order
    confidence: float                  # [0.0, 1.0]
    rationale: str                     # Human-readable signal reason
    metadata: dict = Field(default_factory=dict)  # Strategy-specific extras
```

### 6.4 Risk Decision

```python
class RiskDecisionStatus(str, Enum):
    APPROVED = "approved"
    REJECTED = "rejected"
    MODIFIED = "modified"   # Risk reduced the size

class RiskDecision(BaseEvent):
    event_type: str = "risk_decision"
    signal_id: UUID                    # References SignalEvent.event_id
    strategy_id: str
    instrument_id: str
    status: RiskDecisionStatus
    approved_quantity: Decimal | None  # None if rejected
    rejected_reason: str | None
    risk_rule_results: list[dict]      # All rule evaluations
```

### 6.5 Order Events

```python
class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP_LIMIT = "stop_limit"
    IOC = "ioc"
    FOK = "fok"

class OrderStatus(str, Enum):
    PENDING_NEW = "pending_new"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"

class OrderRequest(BaseEvent):
    event_type: str = "order_request"
    client_order_id: UUID = Field(default_factory=uuid4)
    risk_decision_id: UUID             # References RiskDecision.event_id
    strategy_id: str
    instrument_id: str
    side: Literal["buy", "sell"]
    order_type: OrderType
    quantity: Decimal
    limit_price: Decimal | None
    stop_price: Decimal | None
    exchange: str                      # Target exchange
    time_in_force: str = "GTC"

class OrderAcknowledged(BaseEvent):
    event_type: str = "order_acknowledged"
    client_order_id: UUID
    exchange_order_id: str
    instrument_id: str
    status: OrderStatus = OrderStatus.ACKNOWLEDGED
```

### 6.6 Fill Event

```python
class FillEvent(BaseEvent):
    event_type: str = "fill"
    fill_id: str
    client_order_id: UUID
    exchange_order_id: str
    strategy_id: str
    instrument_id: str
    side: Literal["buy", "sell"]
    fill_price: Decimal
    fill_quantity: Decimal
    remaining_quantity: Decimal
    commission: Decimal
    commission_asset: str              # "USDT", "BTC", etc.
    is_maker: bool
    exchange: str
```

### 6.7 Position Update

```python
class PositionUpdateEvent(BaseEvent):
    event_type: str = "position_update"
    strategy_id: str
    instrument_id: str
    net_quantity: Decimal              # Positive = long, negative = short
    average_entry_price: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    mark_price: Decimal
    notional_value: Decimal
    trigger: Literal["fill", "mark_to_market", "reconciliation"]
```

### 6.8 Portfolio Snapshot

```python
class PortfolioSnapshot(BaseModel):
    timestamp: datetime
    total_equity: Decimal
    total_unrealized_pnl: Decimal
    total_realized_pnl: Decimal
    total_notional: Decimal
    positions: dict[str, PositionUpdateEvent]  # instrument_id → position
    open_orders: dict[UUID, OrderRequest]
    available_capital: Decimal
```

---

## 7. Concurrency and Runtime Model

### 7.1 Single-Process asyncio Architecture (Phase 1–2)

```python
# runners/live_runner.py

async def main():
    bus = AsyncioBus()
    clock = LiveClock()

    # Instantiate all components
    feed_handler = BinanceFeedHandler(bus, clock)
    strategy = MomentumStrategy(bus, clock, config)
    risk_engine = RiskEngine(bus, clock, risk_config)
    position_engine = PositionEngine(bus, clock)
    oms = OrderManagementSystem(bus, clock)
    order_gateway = BinanceOrderGateway(bus, clock, api_key, api_secret)

    # Wire up subscriptions
    await bus.subscribe("market-data", strategy.on_event)
    await bus.subscribe("signals", risk_engine.on_event)
    await bus.subscribe("risk-decisions", oms.on_event)
    await bus.subscribe("fills", position_engine.on_event)
    await bus.subscribe("fills", oms.on_event)
    await bus.subscribe("orders", order_gateway.on_event)
    await bus.subscribe("positions", risk_engine.on_position_update)

    # Start all components as concurrent tasks
    async with asyncio.TaskGroup() as tg:
        tg.create_task(feed_handler.run())
        tg.create_task(strategy.run())
        tg.create_task(risk_engine.run())
        tg.create_task(position_engine.run())
        tg.create_task(oms.run())
        tg.create_task(order_gateway.run())

asyncio.run(main())
```

### 7.2 Task Scheduling

- Each component implements `async def run()` with its own processing loop
- `asyncio.TaskGroup` ensures all tasks are cancelled if any one raises
- Components use `asyncio.Queue` internally for event buffering
- Long-running CPU tasks offloaded to `asyncio.get_event_loop().run_in_executor()` (ThreadPoolExecutor or ProcessPoolExecutor)

### 7.3 WebSocket Ingestion Pattern

```python
async def run(self):
    while not self._shutdown:
        try:
            async with websockets.connect(self.url) as ws:
                await self._subscribe(ws)
                async for message in ws:
                    event = self._normalize(message)
                    await self.bus.publish("market-data", event)
        except (ConnectionClosed, WebSocketException) as e:
            await self._handle_reconnect(e)

async def _handle_reconnect(self, error):
    delay = min(self._backoff * 2 ** self._retry_count, 60)
    self._retry_count += 1
    logger.warning("reconnecting", delay=delay, error=str(error))
    await asyncio.sleep(delay)
```

### 7.4 Backpressure Handling

```python
class AsyncioBus:
    def __init__(self, max_queue_size: int = 10_000):
        self._queues: dict[str, asyncio.Queue] = defaultdict(
            lambda: asyncio.Queue(maxsize=max_queue_size)
        )

    async def publish(self, topic: str, event: BaseEvent):
        try:
            self._queues[topic].put_nowait(event)
        except asyncio.QueueFull:
            metrics.event_bus_dropped_total.labels(topic=topic).inc()
            logger.warning("bus_queue_full", topic=topic, event_type=event.event_type)
            # Drop or block depending on topic criticality
            if topic in CRITICAL_TOPICS:
                await self._queues[topic].put(event)  # Block
            # else: drop silently with metric
```

### 7.5 Multi-Process / Multi-Machine Scaling (Phase 3+)

Replace `AsyncioBus` with `KafkaBus`. Each component becomes its own Docker container / Kubernetes pod. The interface is identical; only the `bus` constructor argument changes.

```python
# Development
bus = AsyncioBus()

# Production
bus = KafkaBus(bootstrap_servers="kafka:9092", group_id="strategy-engine-1")
```

---

## 8. Backtesting Design

### 8.1 Deterministic Replay Loop

```python
class BacktestEngine:
    async def run(self, start: datetime, end: datetime):
        bus = MemoryBus()  # Synchronous, no async overhead
        clock = SimulatedClock(start)

        # Wire identical strategy/risk/position components
        strategy = self.strategy_class(bus, clock, self.config)
        risk = RiskEngine(bus, clock, self.risk_config)
        position = PositionEngine(bus, clock)
        oms = OrderManagementSystem(bus, clock)
        exchange = SimulatedExchange(bus, clock, self.slippage_model)

        # Replay events in strict chronological order
        async for event in self.data_loader.stream(start, end):
            clock.advance_to(event.timestamp_exchange)
            await bus.publish(event.topic, event)
            await bus.flush()  # Process all downstream reactions before next event
```

### 8.2 Simulated Exchange Fill Model

```python
class SimulatedExchange:
    async def on_order(self, order: OrderRequest):
        current_book = self.order_book_state[order.instrument_id]

        if order.order_type == OrderType.MARKET:
            fill_price = self._apply_slippage(
                current_book,
                order.side,
                order.quantity
            )
            await asyncio.sleep(self.latency_model.sample())  # Latency simulation
            await self._emit_fill(order, fill_price, order.quantity)

        elif order.order_type == OrderType.LIMIT:
            self._add_to_pending_book(order)  # Fill when market crosses limit

    def _apply_slippage(self, book, side, qty) -> Decimal:
        # Walk the order book; consume liquidity; apply market impact
        return self.slippage_model.calculate(book, side, qty)
```

### 8.3 Slippage Models

```python
class LinearSlippageModel:
    """Simple: slippage = factor * quantity"""
    def calculate(self, book, side, qty) -> Decimal:
        mid = (book.best_bid + book.best_ask) / 2
        direction = 1 if side == "buy" else -1
        return mid + direction * self.factor * qty

class OrderBookWalkModel:
    """Realistic: walks actual bid/ask ladder, simulates market impact"""
    def calculate(self, book, side, qty) -> Decimal:
        remaining = qty
        total_cost = Decimal(0)
        levels = book.asks if side == "buy" else book.bids
        for price, size in levels:
            consumed = min(remaining, size)
            total_cost += consumed * price
            remaining -= consumed
            if remaining <= 0:
                break
        return total_cost / qty
```

### 8.4 Historical Data Storage

- **Granular tick data**: TimescaleDB hypertable, partitioned by day
- **OHLCV bars** (1m, 5m, 1h, 1d): TimescaleDB + Parquet on S3
- **Order book snapshots**: Parquet on S3 (100ms or 1s snapshots)
- **Download scripts**: `scripts/download_historical.py` fetches and normalizes

---

## 9. Risk and Reliability

### 9.1 Kill Switch

```python
class KillSwitch:
    def __init__(self, bus: AbstractEventBus, oms: OMS):
        self._triggered = False

    async def trigger(self, reason: str, operator: str = "system"):
        if self._triggered:
            return  # Idempotent
        self._triggered = True
        logger.critical("KILL_SWITCH_TRIGGERED", reason=reason, operator=operator)

        # 1. Block all new signals
        await self.bus.publish("system", KillSwitchEvent(reason=reason))

        # 2. Cancel all open orders
        await self.oms.cancel_all_orders()

        # 3. Alert operators
        await self.alerting.send_critical(f"Kill switch: {reason}")

        # 4. Audit log
        await self.audit_logger.log_kill_switch(reason, operator)
```

### 9.2 Pre-Trade Risk Limits

| Limit Type           | Default Value          | Configurable   |
|----------------------|------------------------|----------------|
| Max order notional   | $100,000               | Per strategy   |
| Max position notional| $1,000,000             | Per instrument |
| Max daily loss       | 2% of AUM              | Per strategy   |
| Max drawdown         | 5% of AUM              | Global         |
| Max open orders      | 50                     | Per strategy   |
| Loss rate            | $10,000 / 5 min        | Global         |
| Concentration        | 20% in single asset    | Global         |

### 9.3 Reconnect Logic

All external connections (WebSocket, FIX) implement exponential backoff with jitter:

```python
async def reconnect_with_backoff(connect_fn, max_retries=10):
    for attempt in range(max_retries):
        try:
            return await connect_fn()
        except ConnectionError as e:
            if attempt == max_retries - 1:
                raise
            delay = min(2 ** attempt + random.uniform(0, 1), 60)
            logger.warning("reconnect_attempt", attempt=attempt, delay=delay)
            await asyncio.sleep(delay)
```

### 9.4 Heartbeats and Staleness Detection

```python
class FeedHealthMonitor:
    async def monitor(self):
        while True:
            await asyncio.sleep(self.check_interval_seconds)
            for instrument_id, last_tick_time in self.last_tick_times.items():
                age = datetime.utcnow() - last_tick_time
                if age > self.stale_threshold:
                    await self.alerting.send_warning(
                        f"Stale feed: {instrument_id}, last tick {age.seconds}s ago"
                    )
```

### 9.5 Audit Logging

Every order, fill, risk decision, and system event is written to an append-only audit log with:
- UTC timestamp
- Component ID
- Event type
- Full event payload (JSON)
- Operator identity (for manual actions)

Stored in: PostgreSQL `audit_log` table + S3 archive (never deleted).

---

## 10. Observability

### 10.1 Structured Logging

```python
import structlog

logger = structlog.get_logger()

# Usage
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

### 10.2 Prometheus Metrics

```python
from prometheus_client import Counter, Histogram, Gauge

# Latency metrics
signal_to_order_latency = Histogram(
    "signal_to_order_latency_us",
    "Latency from signal generation to order submission (microseconds)",
    buckets=[10, 50, 100, 500, 1000, 5000, 10000],
)

# Counters
fills_total = Counter("fills_total", "Total fills", ["instrument", "side", "strategy"])
rejected_signals_total = Counter(
    "rejected_signals_total", "Signals rejected by risk", ["strategy", "reason"]
)

# Gauges
open_positions = Gauge("open_positions", "Current open positions count")
portfolio_pnl = Gauge("portfolio_pnl_usd", "Current portfolio PnL in USD")
```

### 10.3 Key Dashboards (Grafana)

| Dashboard              | Key Panels                                         |
|------------------------|----------------------------------------------------|
| **Trading Overview**   | PnL, positions, fill rate, open orders             |
| **System Health**      | Latency histograms, queue depths, reconnects       |
| **Risk Monitor**       | Exposure by instrument, drawdown meter, kill switch|
| **Feed Quality**       | Tick rates, gap counts, staleness                  |
| **Backtest Results**   | Sharpe, max DD, win rate, exposure timeline        |

### 10.4 Latency Monitoring

Track end-to-end latency at each stage:

```
Feed receive → Bus publish:        target < 100µs
Bus publish → Strategy onEvent:    target < 50µs
Strategy → Signal emit:            target < 1ms
Signal → Risk decision:            target < 500µs
Risk decision → Order submit:      target < 500µs
Order submit → Exchange ack:       target < 10ms (network dependent)
```

---

## 11. Agile Implementation Roadmap

### Phase 1: Minimal Single-Process Prototype

**Objectives**: Validate core event flow end-to-end on paper trading

**Architecture Decisions**:
- Single Python process with asyncio
- `asyncio.Queue` as event bus
- No external dependencies (no DB, no Kafka)
- In-memory state only

**Deliverables**:
- `core/events.py` — all event dataclasses
- `event_bus/asyncio_bus.py`
- `feed_handler/connectors/binance.py` — WebSocket connection, normalization
- `strategy/examples/momentum.py` — basic signal logic
- `risk/` — minimal position limit rule only
- `oms/engine.py` — order lifecycle (paper only)
- `order_gateways/simulation/` — simulated order_gateway
- `runners/live_runner.py` — wires everything together
- Console logging only

**Testing Strategy**: Unit tests for each component in isolation using `MemoryBus`

**Technical Debt**: No persistence, no reconnect logic, no metrics — acceptable

---

### Phase 2: Real Exchange Connectivity

**Objectives**: Connect to a real exchange in paper trading mode

**Architecture Decisions**:
- Add `order_gateways/binance/` with testnet support
- Implement WebSocket reconnect with backoff
- Add order signing and authentication

**Deliverables**:
- `order_gateways/binance/ws_order_gateway.py`
- `feed_handler/connectors/` — production-quality with reconnect
- Integration tests against Binance testnet
- Basic structlog logging

**Testing Strategy**: Integration tests replay captured WebSocket sessions

---

### Phase 3: Persistence + Event Bus

**Objectives**: Survive restarts; introduce Kafka for scalability

**Architecture Decisions**:
- PostgreSQL for orders/fills (via SQLAlchemy + asyncpg)
- TimescaleDB for tick data
- Kafka replaces asyncio.Queue as event bus
- Redis for position cache

**Deliverables**:
- `persistence/postgres.py`, `persistence/timescale.py`, `persistence/redis_cache.py`
- `event_bus/kafka_bus.py`
- Alembic migrations
- State recovery on startup (replay last N events from Kafka)
- Docker Compose with all infrastructure services

**Testing Strategy**: Integration tests with Docker Compose spin-up

---

### Phase 4: Backtesting Engine

**Objectives**: Run strategies on historical data with identical code paths

**Architecture Decisions**:
- `SimulatedClock` injected in place of `LiveClock`
- `MemoryBus` (synchronous) for maximum backtest speed
- Order book walk slippage model
- Parquet files for historical data

**Deliverables**:
- `backtest/` complete module
- `scripts/download_historical.py`
- `runners/backtest_runner.py`
- HTML tearsheet report (PnL, Sharpe, max drawdown, trade list)
- Verified: same strategy code, same results on re-run (determinism test)

**Testing Strategy**: "Golden output" tests — backtest results must match stored expected output exactly

---

### Phase 5: Risk Controls

**Objectives**: Production-grade risk management

**Architecture Decisions**:
- Full risk rule suite (position, notional, drawdown, concentration, rate-of-loss)
- Kill switch with audit log
- Pre-trade and post-trade risk checks

**Deliverables**:
- `risk/rules/` — all rule implementations
- Kill switch API (REST endpoint for manual trigger)
- Risk configuration schema with validation
- Alerting integration (Slack webhook)

**Testing Strategy**: Chaos tests — deliberately breach limits and verify system response

---

### Phase 6: Dashboard / Monitoring

**Objectives**: Full observability stack

**Architecture Decisions**:
- Prometheus + Grafana + Loki + Alertmanager stack
- All components emit structured logs and metrics
- Pre-built Grafana dashboards as code (JSON provisioning)

**Deliverables**:
- `monitoring/` complete module
- Grafana dashboard JSON files in `deploy/grafana/`
- Alertmanager rules for critical conditions
- Runbooks for all alerts in `docs/runbooks/`
- Latency SLO dashboards

**Testing Strategy**: Load tests to verify metrics accuracy under high throughput

---

### Phase 7: Multi-Strategy / Multi-Asset

**Objectives**: Run multiple strategies simultaneously across multiple exchanges

**Architecture Decisions**:
- Strategy isolation: separate Kafka consumer groups per strategy
- Per-strategy risk limits and capital allocation
- Kubernetes deployment (one pod per component type)
- Portfolio-level risk aggregation across strategies
- Begin C++ migration for feed handler (if latency requires it)

**Deliverables**:
- `strategy/registry.py` — multi-strategy lifecycle management
- Portfolio-level risk rules
- Helm chart for Kubernetes deployment
- CI/CD pipeline (GitHub Actions → ArgoCD)
- Performance benchmarks vs. C++ baseline

**Technical Debt Resolution**: Address all TODO items from Phases 1–6; full type coverage; 80%+ test coverage

---

## 12. Deployment Strategy

### 12.1 Local Development Setup

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

### 12.2 Docker Compose Architecture

```yaml
# docker/docker-compose.yml
services:

  # Infrastructure
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

  # Observability
  prometheus:
    image: prom/prometheus:latest
    volumes: [./prometheus.yml:/etc/prometheus/prometheus.yml]

  grafana:
    image: grafana/grafana:latest
    volumes: [./grafana/dashboards:/etc/grafana/dashboards]
    depends_on: [prometheus]

  # Application services
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

### 12.3 CI/CD Pipeline (GitHub Actions)

```yaml
# .github/workflows/ci.yml
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres: {image: timescale/timescaledb:latest-pg16, ...}
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
    runs-on: ubuntu-latest
    steps:
      - run: uv run pytest tests/system/ -v  # Golden output tests

  build:
    needs: [test, backtest-regression]
    runs-on: ubuntu-latest
    steps:
      - run: docker build -t trading-platform:${{ github.sha }} -f docker/Dockerfile.trading .
      - run: docker push ghcr.io/${{ github.repository }}/trading-platform:${{ github.sha }}

  deploy-staging:
    needs: build
    if: github.ref == 'refs/heads/main'
    steps:
      - run: argocd app sync trading-staging
```

### 12.4 Production Deployment Evolution

| Phase     | Infrastructure                                  | When to Use                        |
|-----------|-------------------------------------------------|------------------------------------|
| Phase 1–2 | Single VM, process supervisor (supervisord)     | Development, initial live testing  |
| Phase 3–5 | Docker Compose on dedicated VM                  | Small live deployment              |
| Phase 6   | Docker Compose + managed DBs (RDS, ElastiCache) | Production, small scale            |
| Phase 7   | Kubernetes (EKS/GKE) + Helm + ArgoCD            | Multi-strategy, high availability  |

### 12.5 When Kubernetes Becomes Useful

Kubernetes overhead is only justified when you need:
- Independent scaling of components (feed handler vs strategy engine)
- Automated pod restart and health checking
- Rolling deployments with zero downtime
- Multiple replicas of stateless components (monitoring, API servers)
- Resource quotas per component
- Multi-region / multi-AZ deployment

**Rule of thumb**: Adopt Kubernetes when running 3+ strategies across 2+ exchanges in production.

---

## 13. Engineering Best Practices

### 13.1 Typing Strategy

- **All source code**: strict type hints; no `Any` unless unavoidable
- **mypy configuration** (`pyproject.toml`):

```toml
[tool.mypy]
strict = true
python_version = "3.12"
ignore_missing_imports = false
disallow_untyped_defs = true
disallow_any_explicit = true
warn_return_any = true
```

- Use `typing.Protocol` for duck-typed interfaces (not ABCs)
- Use `NewType` for domain primitives: `OrderId = NewType("OrderId", UUID)`
- Use `TypeAlias` for complex types: `Price: TypeAlias = Decimal`

### 13.2 Linting and Formatting

```toml
# pyproject.toml
[tool.ruff]
target-version = "py312"
line-length = 100
select = [
    "E", "W",   # pycodestyle
    "F",        # pyflakes
    "I",        # isort
    "B",        # flake8-bugbear
    "UP",       # pyupgrade
    "SIM",      # flake8-simplify
    "TCH",      # flake8-type-checking
    "RUF",      # ruff-specific rules
]
ignore = ["E501"]  # line length handled by formatter

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
```

### 13.3 Testing Hierarchy

```
tests/
├── unit/           # No I/O, no bus, pure function tests
│                   # Target: 200ms total, 90%+ coverage of core/
│
├── integration/    # Uses MemoryBus; wires real components together
│                   # Uses Docker-provided Postgres/Redis/Kafka
│                   # Target: < 30s total
│
└── system/         # Full backtest runs; golden output comparison
                    # Target: < 5 min total; run on every main branch push
```

**Fixtures philosophy**:
- Use `pytest-asyncio` for all async tests
- Factory functions over fixtures for event objects
- Shared `conftest.py` provides: `memory_bus`, `simulated_clock`, `mock_order_gateway`

### 13.4 Configuration Management

```toml
# config/base.toml — defaults only; no secrets
[trading]
environment = "development"
log_level = "INFO"

[risk]
max_position_notional_usd = 100_000
max_daily_loss_pct = 0.02
drawdown_kill_switch_pct = 0.05

[feed_handler]
reconnect_max_attempts = 10
stale_feed_threshold_seconds = 30
```

```python
# core/config.py
from pydantic_settings import BaseSettings, TomlConfigSettingsSource

class TradingConfig(BaseSettings):
    model_config = SettingsConfigDict(toml_file="config/base.toml")

    environment: str
    log_level: str
    risk: RiskConfig
    feed_handler: FeedHandlerConfig

    @classmethod
    def settings_customise_sources(cls, settings_cls, **kwargs):
        return (
            TomlConfigSettingsSource(settings_cls, "config/base.toml"),
            TomlConfigSettingsSource(settings_cls, f"config/{ENVIRONMENT}.toml"),
            kwargs["env_settings_source"],  # Env vars override all
        )
```

### 13.5 Secrets Handling

- **Never** commit secrets to git (enforced by `git-secrets` pre-commit hook)
- Local development: `.env` file (gitignored)
- Staging/Production: environment variables injected by CI/CD or Kubernetes Secrets
- Advanced: HashiCorp Vault or AWS Secrets Manager for rotation

```bash
# .env (gitignored)
BINANCE_API_KEY=xxx
BINANCE_API_SECRET=xxx
DATABASE_URL=postgresql+asyncpg://user:password@localhost/trading
REDIS_URL=redis://localhost:6379/0
```

### 13.6 Dependency Management

- **Tool**: `uv` (fast, lockfile-based, workspace support)
- **Policy**: pin all transitive dependencies in `uv.lock`
- **Security**: `uv audit` in CI to detect vulnerabilities
- **Updates**: Dependabot weekly PRs; human review before merge

```toml
# pyproject.toml
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

### 13.7 Reproducibility Checklist

Before any backtest result is considered valid:
- [ ] Configuration file committed and tagged
- [ ] `uv.lock` committed (exact dependency versions)
- [ ] Random seed set in all stochastic models
- [ ] Historical data snapshot referenced by content hash
- [ ] Git commit SHA recorded in backtest report
- [ ] Golden output test passes in CI

### 13.8 Architecture Decision Records (ADRs)

All significant architectural choices must be documented in `docs/adr/`:

```markdown
# ADR-001: Use Kafka as Production Event Bus

## Status: Accepted

## Context
We need an event bus that supports persistence for replay and can scale
across processes.

## Decision
Use Apache Kafka in production. Use asyncio.Queue in Phase 1 prototype.

## Consequences
- Positive: event replay, horizontal scaling, durability
- Negative: operational complexity, requires Zookeeper/KRaft
- Migration: AbstractEventBus interface allows transparent swap
```

---

## Appendix: Quick Reference

### Critical Path Latency Budget (Live Trading)

```
Exchange → Feed Handler:          ~1–5ms   (network)
Feed Handler → Event Bus:         <100µs
Event Bus → Strategy:             <50µs
Strategy computation:             <1ms
Strategy → Risk Engine:           <50µs
Risk evaluation:                  <500µs
Risk → OMS:                       <50µs
OMS → Exchange OrderGateway:           <100µs
Exchange OrderGateway → Exchange:      ~1–5ms   (network)
──────────────────────────────────────────
Total round-trip (excl. network): ~3ms
Total end-to-end (incl. network): ~10–20ms
```

### System State Recovery Sequence (on restart)

```
1. Connect to Kafka; seek to last committed offset
2. Load position snapshots from Redis (last 24h)
3. If Redis miss: replay fills from PostgreSQL
4. Load open orders from PostgreSQL
5. Reconcile positions with exchange REST API
6. Resume market data subscription
7. Mark system as READY; begin processing signals
```

### Environment Variables Reference

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

---

*This document should be treated as a living specification. Update it via PR with each significant architectural change. Archive superseded decisions in `docs/adr/`.*
