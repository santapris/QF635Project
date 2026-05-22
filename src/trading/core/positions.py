"""Position domain type.

A :class:`Position` is the current state of holding (or shorting) one
instrument under one strategy. It is *derived* state — the position
engine reduces a stream of fills into positions — but the type itself
is a primitive shared by anyone who reasons about exposure: strategy
context, risk engine, dashboards.

Frozen and immutable. Each fill produces a new :class:`Position`; the
old one is the snapshot at the previous moment.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from .instruments import Instrument
from .types import Price, Quantity, StrategyId


class Position(BaseModel): # Inherits from Pydantic's BaseModel, giving automatic validation, serialization, and type coercion on construction
    """Inventory in one instrument under one strategy."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    strategy_id: StrategyId
    instrument: Instrument
    quantity: Quantity = Field(..., description="Signed: +long, -short, 0 flat.") # ... (Python's Ellipsis) is Pydantic's way of saying "this field is required — no default."
    average_entry_price: Price = Field(
        default=Price(0),
        description="Average cost basis of the open position. Zero when flat.",
    )
    realized_pnl: Price = Field(default=Price(0))
    unrealized_pnl: Price = Field(default=Price(0))
    mark_price: Price = Field(default=Price(0), description="Last mark used for MTM.")

    @property
    def is_flat(self) -> bool:
        return self.quantity == 0

    @property
    def is_long(self) -> bool:
        return self.quantity > 0

    @property
    def is_short(self) -> bool:
        return self.quantity < 0

    @property
    def total_pnl(self) -> Price:
        return self.realized_pnl + self.unrealized_pnl


__all__ = ["Position"]
