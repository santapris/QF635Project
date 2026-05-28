"""Order — the OMS's internal, mutable view of one outstanding order.

Distinct from :class:`OrderRequest` (the *intent* event sent to the
order_gateway) and :class:`FillEvent` (the *result* event from the order_gateway).
The Order object lives in the OMS, gets mutated as order_gateway responses
flow in, and is never published on the bus — it's internal state.

Every mutation goes through :meth:`transition_to` so the state machine
catches illegal transitions early.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from ..core.events import FillEvent
from ..core.instruments import Instrument
from ..core.types import (
    ClientOrderId,
    ExchangeOrderId,
    OrderId,
    OrderStatus,
    OrderType,
    Price,
    Quantity,
    Side,
    StrategyId,
    TimeInForce,
    Timestamp,
)
from .state_machine import is_terminal, validate_transition


@dataclass(slots=True)
class Order:
    """Mutable order tracked by the OMS."""

    order_id: OrderId
    client_order_id: ClientOrderId
    strategy_id: StrategyId
    instrument: Instrument
    side: Side
    order_type: OrderType
    quantity: Quantity
    price: Price | None
    time_in_force: TimeInForce
    created_at_ns: Timestamp

    # State that mutates as the order progresses
    status: OrderStatus = OrderStatus.PENDING_NEW
    exchange_order_id: ExchangeOrderId | None = None
    cumulative_filled: Quantity = Quantity(Decimal(0))
    average_fill_price: Price = Price(Decimal(0))
    last_update_ns: Timestamp = 0
    reject_reason: str | None = None

    # Linkage for child orders spawned by an execution algorithm. Holds the
    # ``OrderLeg.leg_id`` of the leg whose algo emitted this child. None means
    # the order was placed directly (PASSIVE / single-clip routing) and is a
    # plain resting order, not a slice.
    parent_leg_id: str | None = None

    # Fill ids we've already applied, so a duplicate fill from the order_gateway
    # is detected rather than double-counted.
    _applied_fills: set[str] = field(default_factory=set)

    # --- Derived ----------------------------------------------------------

    @property
    def leaves_quantity(self) -> Quantity:
        return Quantity(self.quantity - self.cumulative_filled)

    @property
    def is_terminal(self) -> bool:
        return is_terminal(self.status)

    @property
    def is_child(self) -> bool:
        return self.parent_leg_id is not None

    # --- Mutators ---------------------------------------------------------

    def transition_to(self, new_status: OrderStatus, *, at_ns: Timestamp) -> None:
        """Set status, validating against the state machine."""
        validate_transition(self.status, new_status)
        self.status = new_status
        self.last_update_ns = at_ns

    def apply_fill(self, fill: FillEvent) -> bool:
        """Apply a fill. Returns True if this fill was applied (False if dup).

        Updates cumulative_filled and average_fill_price, then transitions
        to PARTIALLY_FILLED or FILLED. Does *not* publish — the caller
        owns publishing decisions.
        """
        fill_key = str(fill.fill_id)
        if fill_key in self._applied_fills:
            return False
        self._applied_fills.add(fill_key)

        # Update VWAP of fills.
        prior_qty = self.cumulative_filled
        new_qty = Quantity(prior_qty + fill.fill_quantity)
        if new_qty > 0:
            total_cost = (
                self.average_fill_price * prior_qty
                + fill.fill_price * fill.fill_quantity
            )
            self.average_fill_price = Price(total_cost / new_qty)
        self.cumulative_filled = new_qty

        # Decide new status.
        new_status = (
            OrderStatus.FILLED
            if self.leaves_quantity == 0
            else OrderStatus.PARTIALLY_FILLED
        )
        self.transition_to(new_status, at_ns=fill.ts_event)
        return True


__all__ = ["Order"]
