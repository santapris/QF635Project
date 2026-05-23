# Backtest Engine

**Package**: `trading.backtest`

## Responsibilities

- Load historical market data from storage (Parquet, TimescaleDB)
- Replay events in strict chronological order via a simulated clock
- Simulate a virtual exchange with configurable slippage, latency, and fill models
- Use identical strategy/risk/position/OMS code as live trading
- Produce performance reports (PnL, Sharpe, drawdown, fill statistics)

## Module Structure

```
backtest/
├── engine.py            # Main replay loop; injects SimulatedClock + MemoryBus
├── data_source.py       # Historical data loading and streaming (Parquet / DB)
├── order_gateway.py     # Simulated order gateway (fill model, latency model)
├── metrics.py           # Backtest performance metric calculations
└── report.py            # HTML / JSON tearsheet generation
```

## Key Design

```python
async def run(self, start: datetime, end: datetime):
    bus = MemoryBus()           # Synchronous — no async overhead
    clock = SimulatedClock(start)

    # Identical components as live trading
    strategy = self.strategy_class(bus, clock, self.config)
    risk = RiskEngine(bus, clock, self.risk_config)
    position = PositionEngine(bus, clock)
    oms = OrderManagementSystem(bus, clock)
    exchange = SimulatedOrderGateway(bus, clock, self.slippage_model)

    async for event in self.data_source.stream(start, end):
        clock.advance_to(event.timestamp_exchange)
        await bus.publish(event.topic, event)
        await bus.flush()   # All downstream reactions complete before next event
```

`flush()` after each event enforces strict causality — a strategy cannot react to a price event that hasn't been published yet.

## Simulated Order Gateway

- **Market orders**: fills immediately at `slippage_model.calculate(book, side, qty)`
- **Limit orders**: added to a pending book; filled when market crosses the limit price
- **Latency**: `await asyncio.sleep(latency_model.sample())` before emitting fill

## Slippage Models

| Model              | Description                                              |
|--------------------|----------------------------------------------------------|
| `LinearSlippage`   | `mid ± factor × qty` — simple, fast                     |
| `OrderBookWalk`    | Walks actual bid/ask ladder, simulates liquidity impact  |

## Determinism Guarantees

- Fixed random seed for all stochastic models
- `SimulatedClock` never reads wall-clock time
- All parameters and the git SHA are recorded in the report header
- Golden-output CI tests: re-run must produce byte-identical results

See [backtesting.md](../backtesting.md) for the full reproducibility checklist.
