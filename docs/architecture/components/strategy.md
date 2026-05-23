# Strategy Engine

**Package**: `trading.strategy`

## Responsibilities

- Subscribe to market data events
- Maintain strategy-local state (indicators, positions, parameters)
- Generate `SignalEvent` (buy/sell/close with target size and rationale)
- Support multiple simultaneous strategies with isolated state
- Hot-reload strategy parameters without restart

**Inputs**: `TickEvent`, `TradeEvent`, `OrderBookEvent`, `PositionUpdateEvent`  
**Outputs**: `SignalEvent`

## Module Structure

```
strategy/
├── base.py              # AbstractStrategy interface
├── context.py           # Strategy execution context (clock, portfolio view)
├── registry.py          # Strategy registration and lifecycle management
├── indicator_lib/
│   ├── moving_averages.py   # EMA, SMA, VWAP
│   ├── momentum.py          # RSI, rate-of-change
│   └── microstructure.py    # Order book imbalance, microprice, VPIN
└── examples/
    ├── momentum.py
    ├── mean_reversion.py
    └── market_making.py
```

## AbstractStrategy Interface

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

## Design Rules

- Strategy must never import from `risk/`, `oms/`, or `order_gateways/`
- Strategy emits signals only — it cannot submit orders directly
- All time references must use the injected `Clock`, never `datetime.now()`
- Strategy parameters stored in Redis for hot-reload without restart

## Scaling

Each strategy runs in its own asyncio task within a single process. For CPU-intensive strategies, run in a separate process connected via Kafka with its own consumer group.
