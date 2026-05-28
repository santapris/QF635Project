# Strategy Engine

**Package**: `trading.strategy`

## Responsibilities

- Subscribe to market data events
- Maintain strategy-local state (indicators, positions, parameters)
- Generate `SignalEvent`s expressing **desired order state** — a tuple of
  `OrderLeg`s (the orders the strategy wants resting right now)
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

## Signal Output

A strategy returns `SignalEvent`s whose `legs` express its complete desired
order state (see [Data Contracts](../data-contracts.md)). Each leg carries an
`ExecutionIntent` (`PASSIVE` / `NORMAL` / `URGENT`) — a *stance* about urgency
and price, **not** an algorithm. The OMS router decides whether a leg places as
one order or slices; the strategy stays ignorant of execution mechanics.

Re-emitting the same desired legs is safe: the OMS reconciles against open
orders, so unchanged quotes keep their queue position and only genuine changes
churn orders. Strategies that want their orders *sliced* across re-signals must
keep a stable `leg_id` per leg so the OMS resumes the execution algo rather
than restarting it.

## Design Rules

- Strategy must never import from `risk/`, `oms/`, or `order_gateways/`
- Strategy emits signals only — it cannot submit orders directly
- Strategy declares intent, never an execution algorithm
- All time references must use the injected `Clock`, never `datetime.now()`
- Strategy parameters stored in Redis for hot-reload without restart

## Scaling

Each strategy runs in its own asyncio task within a single process. For CPU-intensive strategies, run in a separate process connected via Kafka with its own consumer group.
