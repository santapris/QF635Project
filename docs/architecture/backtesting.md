# Backtesting & Replay Engine

## Responsibilities

- Load historical market data from storage (Parquet, TimescaleDB)
- Replay events in chronological order at configurable speed (1x, 10x, max speed)
- Simulate a virtual exchange with configurable slippage, latency, and fill models
- Use identical strategy/risk/position code as live trading
- Produce backtest reports with PnL, Sharpe, drawdown, fill statistics

## Module Structure

```
backtest/
├── engine.py              # Main replay loop
├── clock.py               # Simulated clock (controls event timestamps)
├── data_loader.py         # Historical data loading and streaming
├── simulated_exchange.py  # Virtual order book and fill simulation
├── slippage_models.py     # Linear, square-root, market-impact models
├── latency_models.py      # Configurable order-to-fill latency
└── report.py              # Performance metrics and tearsheet
```

## Deterministic Replay Loop

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

## Simulated Exchange Fill Model

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
        return self.slippage_model.calculate(book, side, qty)
```

## Slippage Models

```python
class LinearSlippageModel:
    """slippage = factor * quantity"""
    def calculate(self, book, side, qty) -> Decimal:
        mid = (book.best_bid + book.best_ask) / 2
        direction = 1 if side == "buy" else -1
        return mid + direction * self.factor * qty

class OrderBookWalkModel:
    """Walks actual bid/ask ladder, simulates market impact"""
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

## Historical Data Storage

| Data Type            | Storage                    | Notes |
|----------------------|----------------------------|-------|
| Granular tick data   | TimescaleDB hypertable     | Partitioned by day |
| OHLCV bars           | TimescaleDB + Parquet/S3   | 1m, 5m, 1h, 1d |
| Order book snapshots | Parquet on S3              | 100ms or 1s snapshots |

Download and normalize via `scripts/download_historical.py`.

## Determinism Guarantees

- Fixed random seed for all stochastic models
- Snapshots stored every N events for checkpointing
- All parameters logged at run start; full reproducibility from seed + config

### Reproducibility Checklist

Before any backtest result is considered valid:

- [ ] Configuration file committed and tagged
- [ ] `uv.lock` committed (exact dependency versions)
- [ ] Random seed set in all stochastic models
- [ ] Historical data snapshot referenced by content hash
- [ ] Git commit SHA recorded in backtest report
- [ ] Golden output test passes in CI
