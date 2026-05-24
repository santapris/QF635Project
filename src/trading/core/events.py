"""Canonical event schema.

Every message that crosses the event bus is one of these types. They are:

- **Immutable** (Pydantic ``frozen=True``) — events represent facts that
  happened, not state to be mutated.
- **Self-describing** — ``event_type`` is a discriminator that lets us
  dispatch without isinstance chains and serialize into Kafka with type
  info preserved.
- **Stamped** with two timestamps:

    - ``ts_event``: when the underlying real-world thing occurred (exchange
      timestamp on market data, fill time on a fill, etc.). This is what
      strategies sort on.
    - ``ts_ingest``: when our system saw it. The gap between these two is
      our latency to the venue and is reported to monitoring.

All event ids are UUIDs generated at construction time. All money fields
are ``Decimal``. JSON serialization preserves Decimal precision via
``model_dump(mode="json")``.
"""

from __future__ import annotations

from typing import Annotated, Literal, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from .instruments import Instrument
from .types import (
    ClientOrderId,
    EventId,
    ExchangeOrderId,
    FillId,
    OrderId,
    OrderStatus,
    OrderType,
    Price,
    Quantity,
    Severity,
    Side,
    StrategyId,
    Symbol,
    TimeInForce,
    Timestamp,
)


# --- Base ------------------------------------------------------------------


class BaseEvent(BaseModel):
    """Common fields and config for every event type."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        # Decimals serialize as strings to preserve precision; consumers
        # round-trip through Pydantic, not raw json.loads.
        ser_json_inf_nan="strings",
    )

    event_id: EventId = Field(default_factory=lambda: EventId(uuid4()))
    ts_event: Timestamp
    ts_ingest: Timestamp
    source: str = Field(..., description="Component that produced this event.")


# --- Market data -----------------------------------------------------------


class TickEvent(BaseEvent):
    """Top-of-book snapshot. Published when bid or ask changes."""

    event_type: Literal["tick"] = "tick"
    instrument: Instrument
    bid_price: Price
    bid_size: Quantity
    ask_price: Price
    ask_size: Quantity

    @property
    def mid(self) -> Price:
        return (self.bid_price + self.ask_price) / 2

    @property
    def spread(self) -> Price:
        return self.ask_price - self.bid_price


class TradeEvent(BaseEvent):
    """A trade printed on the public tape."""

    event_type: Literal["trade"] = "trade"
    instrument: Instrument
    price: Price
    quantity: Quantity
    aggressor_side: Side | None = Field(
        default=None,
        description="Side of the aggressor (taker), if the venue reports it.",
    )
    venue_trade_id: str | None = None


class OrderBookLevel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    price: Price
    quantity: Quantity


class OrderBookEvent(BaseEvent):
    """L2 snapshot or delta. ``is_snapshot`` distinguishes them."""

    event_type: Literal["order_book"] = "order_book"
    instrument: Instrument
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    sequence: int
    is_snapshot: bool = False


class FundingRateEvent(BaseEvent):
    event_type: Literal["funding_rate"] = "funding_rate"
    instrument: Instrument
    rate: Price
    next_funding_time: Timestamp


# --- Strategy --------------------------------------------------------------


class SignalEvent(BaseEvent):
    """A strategy's intent to trade. Not yet an order — risk hasn't seen it."""

    event_type: Literal["signal"] = "signal"
    strategy_id: StrategyId
    instrument: Instrument
    side: Side
    target_quantity: Quantity = Field(..., description="Absolute quantity to trade.")
    suggested_price: Price | None = Field(
        default=None,
        description="If provided, hint to the OMS for limit pricing.",
    )
    order_type: OrderType = OrderType.MARKET
    time_in_force: TimeInForce = TimeInForce.IOC
    rationale: str = Field(default="", description="Human-readable reason for audit.")
    metadata: dict[str, str] = Field(default_factory=dict)


# --- Risk ------------------------------------------------------------------


class RiskDecision(BaseEvent):
    """Risk engine's verdict on a signal. Approved decisions become orders."""

    event_type: Literal["risk_decision"] = "risk_decision"
    signal_event_id: EventId
    strategy_id: StrategyId
    approved: bool
    severity: Severity = Severity.INFO
    rule_name: str | None = Field(
        default=None,
        description="Rule that produced the verdict (only set on rejection).",
    )
    reason: str = ""
    # Risk may modify the size downward (e.g. clamp to remaining headroom).
    approved_quantity: Quantity | None = None


class RiskAlertEvent(BaseEvent):
    event_type: Literal["risk_alert"] = "risk_alert"
    rule_name: str
    severity: Severity
    message: str
    metadata: dict[str, str] = Field(default_factory=dict)


class KillSwitchEvent(BaseEvent):
    """Latched. Once this fires, no order may flow until manually reset."""

    event_type: Literal["kill_switch"] = "kill_switch"
    triggered_by: str = Field(..., description="Rule or operator that fired it.")
    reason: str
    metadata: dict[str, str] = Field(default_factory=dict)


# --- OMS / orders ----------------------------------------------------------


class OrderRequest(BaseEvent):
    """OMS instructing the order_gateway to place an order."""

    event_type: Literal["order_request"] = "order_request"
    order_id: OrderId
    client_order_id: ClientOrderId
    strategy_id: StrategyId
    instrument: Instrument
    side: Side
    order_type: OrderType
    quantity: Quantity
    price: Price | None = Field(default=None, description="Required for LIMIT orders.")
    stop_price: Price | None = None
    time_in_force: TimeInForce = TimeInForce.GTC


class CancelRequest(BaseEvent):
    event_type: Literal["cancel_request"] = "cancel_request"
    order_id: OrderId
    client_order_id: ClientOrderId
    instrument: Instrument


class AmendRequest(BaseEvent):
    event_type: Literal["amend_request"] = "amend_request"
    order_id: OrderId
    client_order_id: ClientOrderId
    instrument: Instrument
    new_quantity: Quantity | None = None
    new_price: Price | None = None


# --- OrderGateway responses -----------------------------------------------------


class OrderAcknowledged(BaseEvent):
    event_type: Literal["order_acknowledged"] = "order_acknowledged"
    order_id: OrderId
    client_order_id: ClientOrderId
    exchange_order_id: ExchangeOrderId


class OrderRejected(BaseEvent):
    event_type: Literal["order_rejected"] = "order_rejected"
    order_id: OrderId
    client_order_id: ClientOrderId
    reason: str
    venue_error_code: str | None = None


class OrderCancelled(BaseEvent):
    event_type: Literal["order_cancelled"] = "order_cancelled"
    order_id: OrderId
    client_order_id: ClientOrderId
    reason: str = ""


class FillEvent(BaseEvent):
    """A partial or complete execution. Multiple fills per order are possible."""

    event_type: Literal["fill"] = "fill"
    fill_id: FillId = Field(default_factory=lambda: FillId(uuid4()))
    order_id: OrderId
    client_order_id: ClientOrderId
    exchange_order_id: ExchangeOrderId | None
    strategy_id: StrategyId
    instrument: Instrument
    side: Side
    fill_price: Price
    fill_quantity: Quantity
    cumulative_quantity: Quantity
    leaves_quantity: Quantity
    fee: Price = Field(default=Price(0), description="Always positive; in fee_currency.")
    fee_currency: str = ""
    is_maker: bool | None = None
    venue_trade_id: str | None = None

    @property
    def is_complete(self) -> bool:
        return self.leaves_quantity == 0


# --- Position / PnL --------------------------------------------------------


class PositionUpdateEvent(BaseEvent):
    """Published whenever a position changes (fill) or is marked-to-market."""

    event_type: Literal["position_update"] = "position_update"
    strategy_id: StrategyId
    instrument: Instrument
    quantity: Quantity = Field(..., description="Signed: +long / -short.")
    average_entry_price: Price
    realized_pnl: Price
    unrealized_pnl: Price
    mark_price: Price


class PnLSnapshotEvent(BaseEvent):
    """Periodic portfolio-level PnL snapshot for monitoring & reporting."""

    event_type: Literal["pnl_snapshot"] = "pnl_snapshot"
    strategy_id: StrategyId | None = Field(
        default=None,
        description="None means portfolio-wide aggregate.",
    )
    realized_pnl: Price
    unrealized_pnl: Price
    total_pnl: Price
    gross_exposure: Price
    net_exposure: Price


class AccountBalance(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    asset: str
    free: Price
    locked: Price


class AccountSnapshotEvent(BaseEvent):
    """Exchange-reported account state (wallet balances)."""

    event_type: Literal["account_snapshot"] = "account_snapshot"
    balances: tuple[AccountBalance, ...]


# --- Discriminated union ---------------------------------------------------
# Use this when receiving events from a serialization layer (Kafka, etc.)
# Pydantic will dispatch on ``event_type`` and instantiate the right class.

Event = Annotated[
    Union[
        TickEvent,
        TradeEvent,
        OrderBookEvent,
        FundingRateEvent,
        SignalEvent,
        RiskDecision,
        RiskAlertEvent,
        KillSwitchEvent,
        OrderRequest,
        CancelRequest,
        AmendRequest,
        OrderAcknowledged,
        OrderRejected,
        OrderCancelled,
        FillEvent,
        PositionUpdateEvent,
        PnLSnapshotEvent,
        AccountSnapshotEvent,
    ],
    Field(discriminator="event_type"),
]


__all__ = [
    "AccountBalance",
    "AccountSnapshotEvent",
    "AmendRequest",
    "BaseEvent",
    "CancelRequest",
    "Event",
    "FillEvent",
    "FundingRateEvent",
    "KillSwitchEvent",
    "OrderAcknowledged",
    "OrderBookEvent",
    "OrderBookLevel",
    "OrderCancelled",
    "OrderRejected",
    "OrderRequest",
    "PnLSnapshotEvent",
    "PositionUpdateEvent",
    "RiskAlertEvent",
    "RiskDecision",
    "SignalEvent",
    "TickEvent",
    "TradeEvent",
]
