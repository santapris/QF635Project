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
    ExecutionIntent,
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


class OrderLeg(BaseModel):
    """One order within a :class:`SignalEvent`.

    A leg is the atomic unit of a strategy's desired state: one side, one
    price, one size, and the order parameters the OMS should use when placing
    it. Strategies with a single order pass one leg; market-makers pass two
    (bid + ask); ladder/spread strategies may pass more — including multiple
    legs on the same side at different price levels, which is why each leg
    carries a stable ``leg_id`` rather than being keyed by side.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    leg_id: str = Field(
        default_factory=lambda: uuid4().hex[:12],
        description=(
            "Stable identifier for this leg within its signal. Used by risk "
            "and OMS to address legs unambiguously when multiple legs may "
            "share a side (e.g. a price ladder)."
        ),
    )
    side: Side
    quantity: Quantity
    price: Price | None = Field(
        default=None,
        description="Limit price. None for market orders.",
    )
    order_type: OrderType = OrderType.MARKET
    time_in_force: TimeInForce = TimeInForce.IOC
    intent: ExecutionIntent = Field(
        default=ExecutionIntent.PASSIVE,
        description=(
            "Strategy's execution stance. The OMS router maps this to a "
            "concrete algo. PASSIVE (default) always places in-place with no "
            "slicing — reproduces market-making behaviour."
        ),
    )


class SignalEvent(BaseEvent):
    """A strategy's complete desired order state for one instrument.

    ``legs`` is the full set of orders the strategy wants resting on the
    exchange for this instrument right now. The OMS treats this as final
    state — it reconciles open orders against the legs tuple and:

    - Places legs that have no matching open order.
    - Cancels open orders that have no matching leg (strategy withdrew them).
    - Leaves open orders whose leg is unchanged (preserves queue position).
    - Cancel-replaces open orders whose price or size changed.

    An empty ``legs`` tuple means "cancel everything for this instrument."

    ``atomic`` controls how risk handles partial rejection:

    - ``atomic=False`` (default, market-making semantics): legs are
      independent. Risk evaluates each leg and drops the ones that fail;
      surviving legs flow through. Use for quote sets where each side
      stands alone.
    - ``atomic=True`` (pairs/hedged semantics): the legs must succeed or
      fail together. If any leg fails risk, the whole signal is rejected
      and no orders are placed. Use when one leg without the other leaves
      a worse position than not trading at all.
    """

    event_type: Literal["signal"] = "signal"
    strategy_id: StrategyId
    instrument: Instrument
    legs: tuple[OrderLeg, ...] = Field(
        default=(),
        description="Desired orders. Empty means withdraw all open orders for this instrument.",
    )
    atomic: bool = Field(
        default=False,
        description=(
            "If True, any leg rejection rejects the whole signal. Use for "
            "pairs/hedged trades where partial fills are worse than none."
        ),
    )
    rationale: str = Field(default="", description="Human-readable reason for audit.")
    metadata: dict[str, str] = Field(default_factory=dict)


# --- Risk ------------------------------------------------------------------


class ApprovedLeg(BaseModel):
    """Risk engine's verdict on one approved leg of a signal."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    leg_id: str
    """Matches the originating ``OrderLeg.leg_id``."""
    side: Side
    approved_quantity: Quantity
    """Quantity approved by risk — may be less than requested if clamped."""


class RejectedLeg(BaseModel):
    """Risk engine's verdict on a rejected leg of a signal.

    Preserved on every ``RiskDecision`` so downstream consumers (audit,
    dashboards) can attribute partial-approval cases to specific rules.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    leg_id: str
    side: Side
    rule_name: str
    reason: str
    severity: Severity = Severity.INFO


class RiskDecision(BaseEvent):
    """Risk engine's verdict on a signal. Approved decisions become orders.

    Semantics:

    - ``approved=True`` and ``len(approved_legs) == len(signal.legs)``:
      every leg passed.
    - ``approved=True`` and ``len(rejected_legs) > 0``: partial approval
      (only possible when ``signal.atomic=False``). The OMS places the
      approved legs; rejected_legs is informational.
    - ``approved=False``: nothing is placed. ``rejected_legs`` enumerates
      which legs failed and why.
    """

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
    # Legs that passed risk. Empty when approved=False.
    approved_legs: tuple[ApprovedLeg, ...] = Field(default=())
    # Legs that failed risk. Non-empty on outright rejection and on
    # partial approval; preserved for audit/observability.
    rejected_legs: tuple[RejectedLeg, ...] = Field(default=())


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


class ExecutionRoutedEvent(BaseEvent):
    """Audit record of the OMS router's execution decision for one leg.

    Because a leg's intent (e.g. ``NORMAL``) maps to a concrete algo only
    after the router weighs market state and venue rules, the decision is
    not visible from the signal alone. This event makes it observable: it
    says which algo (if any) the router chose for a leg, and why.
    """

    event_type: Literal["execution_routed"] = "execution_routed"
    strategy_id: StrategyId
    instrument: Instrument
    leg_id: str
    side: Side
    intent: ExecutionIntent
    quantity: Quantity
    algo: str = Field(
        description="Algo class chosen, or 'immediate' when placed as a single order.",
    )
    reason: str = Field(default="", description="Why the router chose this algo.")


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


class OrderAmended(BaseEvent):
    event_type: Literal["order_amended"] = "order_amended"
    order_id: OrderId
    client_order_id: ClientOrderId
    new_price: Price | None = None
    new_quantity: Quantity | None = None
    new_exchange_order_id: ExchangeOrderId | None = None


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


class VenuePosition(BaseModel):
    """Exchange-reported net position for one instrument — ground truth.

    Distinct from the per-strategy books the PositionEngine derives from
    fills. The venue knows only the net across everything (all our
    strategies, plus any external/manual trading), so this is the number
    directly comparable to what the exchange UI shows.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument: Instrument
    net_quantity: Quantity
    """Signed: +long, -short."""
    entry_price: Price
    mark_price: Price
    unrealized_pnl: Price


class VenuePositionSnapshotEvent(BaseEvent):
    """Exchange-reported net positions. Published by the venue gateway's
    state reconciler on a poll of the position endpoint. Snapshot semantics:
    replace wholesale; an instrument absent here is flat on the venue."""

    event_type: Literal["venue_position_snapshot"] = "venue_position_snapshot"
    positions: tuple[VenuePosition, ...] = ()


class WorkingExposure(BaseModel):
    """Open-order (not yet filled) exposure for one (strategy, instrument).

    Buy and sell are kept *separate* rather than netted: a flat position
    with a working buy and a working sell is not flat exposure — either
    fill moves you off zero. Consumers (risk) need the worst case per
    side, which netting would hide.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    strategy_id: StrategyId
    instrument: Instrument
    working_buy: Quantity
    """Sum of leaves_quantity across open BUY orders."""
    working_sell: Quantity
    """Sum of leaves_quantity across open SELL orders."""
    open_order_count: int


class OpenOrderDetail(BaseModel):
    """One currently-resting (non-terminal) order, for display/audit.

    Distinct from :class:`WorkingExposure`, which is the per-side aggregate
    risk reasons about. This is the individual-order view an operator sees.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

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
    """The OMS's working-order state-of-the-world.

    Published by the OMS whenever the open-order set changes (place, ack,
    reject, cancel, fill). Snapshot semantics — replace wholesale; anything
    absent from this snapshot is no longer open. A dropped snapshot
    self-heals on the next one. Mirrors AccountSnapshotEvent / PnLSnapshotEvent.

    Carries two views of the same state:
    - ``exposures``: per-(strategy, instrument) aggregate, side-separated.
      What the risk engine consumes.
    - ``orders``: every individual resting order. What the dashboard shows.
    """

    event_type: Literal["open_orders_snapshot"] = "open_orders_snapshot"
    exposures: tuple[WorkingExposure, ...] = ()
    orders: tuple[OpenOrderDetail, ...] = ()


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
        ExecutionRoutedEvent,
        CancelRequest,
        AmendRequest,
        OrderAcknowledged,
        OrderRejected,
        OrderCancelled,
        OrderAmended,
        FillEvent,
        PositionUpdateEvent,
        PnLSnapshotEvent,
        AccountSnapshotEvent,
        OpenOrdersSnapshotEvent,
        VenuePositionSnapshotEvent,
    ],
    Field(discriminator="event_type"),
]


__all__ = [
    "AccountBalance",
    "AccountSnapshotEvent",
    "AmendRequest",
    "ApprovedLeg",
    "BaseEvent",
    "CancelRequest",
    "Event",
    "ExecutionRoutedEvent",
    "FillEvent",
    "FundingRateEvent",
    "KillSwitchEvent",
    "OrderAcknowledged",
    "OrderAmended",
    "OrderBookEvent",
    "OrderBookLevel",
    "OrderCancelled",
    "OrderRejected",
    "OpenOrderDetail",
    "OpenOrdersSnapshotEvent",
    "OrderLeg",
    "OrderRequest",
    "RejectedLeg",
    "PnLSnapshotEvent",
    "PositionUpdateEvent",
    "WorkingExposure",
    "RiskAlertEvent",
    "RiskDecision",
    "SignalEvent",
    "TickEvent",
    "TradeEvent",
    "VenuePosition",
    "VenuePositionSnapshotEvent",
]
