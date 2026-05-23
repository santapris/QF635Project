# Data Contracts

All events inherit from `BaseEvent` and are immutable Pydantic models. No component ever sees raw exchange data — the Feed Handler normalizes everything before it enters the bus.

## Base Event

```python
from pydantic import BaseModel, Field
from datetime import datetime
from uuid import UUID, uuid4

class BaseEvent(BaseModel):
    model_config = {"frozen": True}  # Immutable

    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    timestamp_exchange: datetime       # Exchange-provided timestamp
    timestamp_received: datetime       # Local receive timestamp
    timestamp_processed: datetime | None = None  # Set when consumed
```

## Market Events

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

## Signal Event

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
    metadata: dict = Field(default_factory=dict)
```

## Risk Decision

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

## Order Events

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

## Fill Event

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

## Position Update

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

## Portfolio Snapshot

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

## Topic Map

| Topic           | Producers              | Consumers                        |
|-----------------|------------------------|----------------------------------|
| `market-data`   | Feed Handler           | Strategy, Risk, Position         |
| `signals`       | Strategy Engine        | Risk Engine                      |
| `risk-decisions`| Risk Engine            | OMS                              |
| `orders`        | OMS                    | Exchange OrderGateway, Monitoring |
| `fills`         | Exchange OrderGateway  | OMS, Position Engine, Monitoring |
| `positions`     | Position Engine        | Risk Engine, Dashboard           |
| `alerts`        | Risk, Feed, OMS        | Monitoring, Dashboard            |
| `system`        | Kill Switch, Admin     | All components                   |
