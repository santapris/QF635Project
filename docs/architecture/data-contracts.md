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

A `SignalEvent` is a strategy's **complete desired order state** for one
instrument — not a single order. It carries a tuple of `OrderLeg`s: the full
set of orders the strategy wants resting on the exchange right now. The OMS
treats this as final state and reconciles open orders against it (place
missing, cancel withdrawn, leave unchanged, cancel-replace on price/size
change). An empty `legs` tuple means "cancel everything for this instrument."

```python
class ExecutionIntent(str, Enum):
    PASSIVE = "passive"   # patient, maker-preferred; queue position matters
    NORMAL  = "normal"    # router decides: single clip or slice by size
    URGENT  = "urgent"    # cross the spread, fill now

class OrderLeg(BaseModel):
    leg_id: str                        # stable identity within the signal
    side: Side
    quantity: Quantity
    price: Price | None = None         # None = market order
    order_type: OrderType = OrderType.MARKET
    time_in_force: TimeInForce = TimeInForce.IOC
    intent: ExecutionIntent = ExecutionIntent.PASSIVE

class SignalEvent(BaseEvent):
    event_type: Literal["signal"] = "signal"
    strategy_id: StrategyId
    instrument: Instrument
    legs: tuple[OrderLeg, ...] = ()     # complete desired order state
    atomic: bool = False               # True = all-or-nothing across legs
    rationale: str = ""
    metadata: dict[str, str] = Field(default_factory=dict)
```

**`leg_id`** is a stable per-leg identity. It lets ladder strategies place
multiple legs on the same side, lets risk map approvals back to specific
legs, and lets the OMS resume a sliced leg's execution algo across re-signals.

**`atomic`** controls partial-rejection handling in risk:
- `False` (default, market-making semantics): legs are independent. Risk
  drops failing legs and approves the survivors.
- `True` (pairs/hedged semantics): any leg rejection rejects the whole signal.
  No partial placement.

## Risk Decision

Risk evaluates each leg independently and reports the per-leg verdict.

```python
class ApprovedLeg(BaseModel):
    leg_id: str
    side: Side
    approved_quantity: Quantity        # may be clamped below requested

class RejectedLeg(BaseModel):
    leg_id: str
    side: Side
    rule_name: str
    reason: str
    severity: Severity

class RiskDecision(BaseEvent):
    event_type: Literal["risk_decision"] = "risk_decision"
    signal_event_id: EventId           # References SignalEvent.event_id
    strategy_id: StrategyId
    approved: bool
    severity: Severity = Severity.INFO
    rule_name: str | None = None       # rule that produced a rejection
    reason: str = ""
    approved_legs: tuple[ApprovedLeg, ...] = ()
    rejected_legs: tuple[RejectedLeg, ...] = ()
```

Semantics:
- `approved=True`, all legs in `approved_legs`: every leg passed.
- `approved=True` with non-empty `rejected_legs`: partial approval (only when
  `signal.atomic=False`) — the OMS places the approved legs.
- `approved=False`: nothing placed; `rejected_legs` enumerates why.

A `KILL`-severity rejection engages the kill switch and short-circuits the
remaining legs of that signal.

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

### Execution Routed (audit)

When a leg's `intent` is `NORMAL`, whether it slices depends on the router's
view of size and venue rules — so the decision isn't visible from the signal
alone. The OMS publishes an audit record of what it chose, on the `orders`
topic.

```python
class ExecutionRoutedEvent(BaseEvent):
    event_type: Literal["execution_routed"] = "execution_routed"
    strategy_id: StrategyId
    instrument: Instrument
    leg_id: str
    side: Side
    intent: ExecutionIntent
    quantity: Quantity
    algo: str                          # "immediate", "TWAPAlgo", ...
    reason: str                        # why the router chose this
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

## Open Orders Snapshot

The OMS is the single writer of order state; risk and the dashboard are
read-only consumers. Whenever the open-order set changes (place, ack, reject,
cancel, fill) the OMS publishes a full snapshot on the `open-orders` topic.
**Snapshot semantics** — replace wholesale; anything absent is no longer open,
and a dropped snapshot self-heals on the next one.

```python
class WorkingExposure(BaseModel):
    strategy_id: StrategyId
    instrument: Instrument
    working_buy: Quantity              # Σ leaves on open BUY orders
    working_sell: Quantity             # Σ leaves on open SELL orders
    open_order_count: int

class OpenOrderDetail(BaseModel):
    order_id: str
    client_order_id: str
    strategy_id: StrategyId
    instrument: Instrument
    side: Side
    order_type: OrderType
    quantity: Quantity
    leaves_quantity: Quantity
    price: Price | None = None
    status: OrderStatus
    created_at_ns: Timestamp

class OpenOrdersSnapshotEvent(BaseEvent):
    event_type: Literal["open_orders_snapshot"] = "open_orders_snapshot"
    exposures: tuple[WorkingExposure, ...] = ()  # per-side aggregate (risk)
    orders: tuple[OpenOrderDetail, ...] = ()     # per-order detail (dashboard)
```

Buy and sell are kept **separate, not netted** — a flat position with a
working buy and a working sell is not flat exposure; either fill moves you off
zero. Risk consumes `exposures` so `MaxPositionRule` checks *effective*
exposure (confirmed position + working orders), closing a double-approve hole.

## Venue Position Snapshot

Exchange-reported net position per instrument — ground truth, directly
comparable to the exchange UI. Published by the venue gateway's state
reconciler on a poll of the position endpoint, on the `venue-positions` topic.
Distinct from the per-strategy books the Position Engine derives from fills:
the venue knows only the net across everything (all strategies plus external /
manual trading), so this is published verbatim and never folded into the
fill-driven books.

```python
class VenuePosition(BaseModel):
    instrument: Instrument
    net_quantity: Quantity             # signed: +long, -short
    entry_price: Price
    mark_price: Price
    unrealized_pnl: Price

class VenuePositionSnapshotEvent(BaseEvent):
    event_type: Literal["venue_position_snapshot"] = "venue_position_snapshot"
    positions: tuple[VenuePosition, ...] = ()
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

| Topic             | Producers              | Consumers                          |
|-------------------|------------------------|------------------------------------|
| `market-data`     | Feed Handler           | Strategy, Risk, Position, OMS¹     |
| `signals`         | Strategy Engine        | Risk Engine, OMS²                  |
| `risk-decisions`  | Risk Engine            | OMS                                |
| `orders`          | OMS                    | Order Gateways, Dashboard          |
| `open-orders`     | OMS                    | Risk Engine, Dashboard             |
| `fills`           | Order Gateways         | OMS, Position Engine, Risk, Dashboard |
| `positions`       | Position Engine        | Risk Engine, Dashboard             |
| `venue-positions` | State reconciler       | Dashboard                          |
| `account`         | Balance reconciler     | Dashboard                          |
| `alerts`          | Risk, Feed, OMS        | Monitoring, Dashboard              |
| `system`          | Kill Switch, Admin     | All components                     |

¹ The OMS subscribes to `market-data` only to cache the latest mark per
  instrument for the execution router's sizing — it does **not** drive
  execution algos from ticks (that's a timer; see [OMS](components/oms.md)).
² The OMS caches each `SignalEvent` so it can rebuild the approved signal when
  the matching `RiskDecision` arrives.

`ExecutionRoutedEvent` rides the `orders` topic (it is OMS-produced
order-lifecycle metadata). `OpenOrdersSnapshotEvent`, `VenuePositionSnapshotEvent`,
and `AccountSnapshotEvent` are *state-of-the-world* snapshots: the dashboard
serves them via polled REST endpoints rather than the WebSocket event stream
(see [Dashboard](dashboard.md)).
