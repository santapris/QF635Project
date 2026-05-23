# Trading Platform

An event-driven algorithmic trading platform. Same code paths for backtest,
paper trading, and (with real order_gateway adapters added) live trading.

## Status

- Core domain model (Decimal money, NewType IDs, frozen Pydantic events)
- Event bus (in-memory, asyncio, Kafka)
- Feed handler with order book reconstruction and reconnect logic
- Strategy framework with momentum / mean-reversion / market-making examples
- Risk engine with clamping rules, throttle, and latched kill switch
- Position engine with WAVG / FIFO / LIFO accounting
- OMS with state machine, signal/decision join, and TWAP/VWAP execution
- Simulation order_gateway (paper trading) and backtest order_gateway (time-jumping)
- Binance Spot & Futures order_gateway with REST/WebSocket, depth book management, and testnet connectivity
- Backtest engine with deterministic simulated clock, Sharpe/Sortino/drawdown metrics

Deferred: additional exchange adapters, Kafka end-to-end test infrastructure, dashboards.

## Layout

```
src/trading/
  core/          types, events, clock, instruments, exceptions, positions
  event_bus/     in-memory, asyncio, Kafka pub-sub
  feed_handler/  market data ingestion + order book + connectors + normalizers
  strategy/      strategy framework, registry, indicator_lib, examples
  risk/          pre-trade risk engine + rules + kill switch
  position/      position tracking + WAVG/FIFO/LIFO accounting + pnl
  oms/           OMS, state machine, execution algos (TWAP/VWAP)
  order_gateways/      simulation + Binance adapter + rate limiter + venue registry
  backtest/      replay engine + scheduling order_gateway + metrics + report
  config/        Pydantic schema + TOML loader + builder + settings
  runners/       CLI entry points + staged pipeline (stage1-4)
  health.py      component health checks + backpressure gauge
  logging.py     structlog configuration
  monitoring/    (deferred)
  persistence/   (deferred)
tests/
  unit/          per-module tests
  integration/   cross-module end-to-end tests (pipeline + binance wiring)
configs/         example TOML configs + sample data
```

## Quick start

```bash
make install-dev
make test
make backtest     # runs configs/backtest_example.toml
```

```bash
python 3.11 -m venv .venv && source .venv/bin/activate (.venv/Scripts/activate if Windows)
python -m pip install -U pip wheel
python -m pip install uv 
uv pip install -e ".[binance]"
```

To run paper trading against the simulation order_gateway:

```bash
make run-paper    # Ctrl-C to stop
```

To run and validate binance_testnet:
```bash
cp .env.example .env   # then fill in BINANCE_API_KEY and BINANCE_API_SECRET
python -m trading.runners.run_binance_testnet
```

## Architecture

Components communicate through one event bus. Strategies emit signals; the
risk engine approves or clamps them into decisions; the OMS turns approved
decisions into orders; the order_gateway turns orders into venue calls; venue
responses flow back as acks/fills; the position engine aggregates fills
into positions and PnL; the risk engine consumes positions to enforce
limits. No component knows about any other directly — only through events.

Determinism: strategies, risk, OMS, position, and order_gateway logic depend
only on injected `Clock`. In production the clock is wall-clock; in
backtests it is a `SimulatedClock` advanced by the replay engine. The
same code produces identical results in both environments.

See `docs/architecture` for the full design documents.
