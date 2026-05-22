# Trading Platform

An event-driven algorithmic trading platform. Same code paths for backtest,
paper trading, and (with real gateway adapters added) live trading.

## Status

Production-shaped foundation:

- Core domain model (Decimal money, NewType IDs, frozen Pydantic events)
- Event bus (in-memory, asyncio, Kafka)
- Feed handler with order book reconstruction and reconnect logic
- Strategy framework with momentum / mean-reversion / market-making examples
- Risk engine with clamping rules, throttle, and latched kill switch
- Position engine with WAVG / FIFO / LIFO accounting
- OMS with state machine, signal/decision join, and TWAP/VWAP execution
- Simulation gateway (paper trading) and backtest gateway (time-jumping)
- Backtest engine with deterministic simulated clock, Sharpe/Sortino/drawdown metrics

Deferred: real exchange adapters, Kafka end-to-end test infrastructure, dashboards.

## Layout

```
src/trading/
  core/          types, events, clock, instruments, exceptions, positions
  event_bus/     in-memory, asyncio, Kafka pub-sub
  feed_handler/  market data ingestion + order book
  strategy/      strategy framework, registry, indicators, examples
  risk/          pre-trade risk engine + rules + kill switch
  position/      position tracking + WAVG/FIFO/LIFO accounting
  oms/           OMS, state machine, execution algos (TWAP/VWAP)
  gateways/      simulation gateway + rate limiter + venue registry
  backtest/      replay engine + scheduling gateway + metrics + report
  config/        Pydantic schema + TOML loader + builder
  runners/       CLI entry points
tests/
  unit/          per-module tests
  integration/   cross-module end-to-end tests
configs/         example TOML configs + sample data
```

## Quick start

```bash
make install-dev
make test
make backtest     # runs configs/backtest_example.toml
```

To run paper trading against the simulation gateway:

```bash
make run-paper    # Ctrl-C to stop
```

## Architecture

Components communicate through one event bus. Strategies emit signals; the
risk engine approves or clamps them into decisions; the OMS turns approved
decisions into orders; the gateway turns orders into venue calls; venue
responses flow back as acks/fills; the position engine aggregates fills
into positions and PnL; the risk engine consumes positions to enforce
limits. No component knows about any other directly — only through events.

Determinism: strategies, risk, OMS, position, and gateway logic depend
only on injected `Clock`. In production the clock is wall-clock; in
backtests it is a `SimulatedClock` advanced by the replay engine. The
same code produces identical results in both environments.

See `trading_system_architecture.md` for the full design document.
