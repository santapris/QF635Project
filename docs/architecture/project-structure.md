# Project Structure

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
│       ├── order_gateways/     # Exchange/broker order gateways
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
    ├── architecture/           # This documentation set
    ├── runbooks/               # Operational runbooks
    └── adr/                    # Architecture Decision Records
```

## Structural Rules

- `core/` has zero external dependencies — only stdlib + pydantic
- `strategy/` must never import from `risk/`, `oms/`, or `order_gateways/` directly
- `notebooks/` is never imported by `src/` (enforced by ruff rule)
- Each subdirectory under `src/trading/` can evolve into its own microservice package

## Live Runner Wiring

```python
# runners/live_runner.py

async def main():
    bus = AsyncioBus()
    clock = LiveClock()

    feed_handler = BinanceFeedHandler(bus, clock)
    strategy = MomentumStrategy(bus, clock, config)
    risk_engine = RiskEngine(bus, clock, risk_config)
    position_engine = PositionEngine(bus, clock)
    oms = OrderManagementSystem(bus, clock)
    order_gateway = BinanceOrderGateway(bus, clock, api_key, api_secret)

    await bus.subscribe("market-data", strategy.on_event)
    await bus.subscribe("signals", risk_engine.on_event)
    await bus.subscribe("risk-decisions", oms.on_event)
    await bus.subscribe("fills", position_engine.on_event)
    await bus.subscribe("fills", oms.on_event)
    await bus.subscribe("orders", order_gateway.on_event)
    await bus.subscribe("positions", risk_engine.on_position_update)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(feed_handler.run())
        tg.create_task(strategy.run())
        tg.create_task(risk_engine.run())
        tg.create_task(position_engine.run())
        tg.create_task(oms.run())
        tg.create_task(order_gateway.run())

asyncio.run(main())
```
