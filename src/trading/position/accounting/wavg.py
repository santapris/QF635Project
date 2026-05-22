"""Weighted-average-cost book.

A single running average of cost basis. Cheap, simple, and what most
crypto exchanges report for "average entry price." Realized PnL when
reducing is ``(fill_price - avg_cost) * fill_qty``, signed by the side
of the current position.

Cases:

- Flat → fill opens the position; avg = fill price.
- Same side → extends; avg is volume-weighted over old and new.
- Opposite side, |fill| ≤ |position| → reduces; cost basis unchanged
  on the remaining inventory.
- Opposite side, |fill| > |position| → fully closes the old position,
  realizes PnL on its full size, then opens a new one in the opposite
  direction at fill price.
"""

from __future__ import annotations

from decimal import Decimal

from ...core.events import FillEvent
from ...core.types import Price, Quantity, Side
from .base import AccountingBook


class WAVGBook(AccountingBook):
    """Single weighted-average cost basis."""

    __slots__ = ("_quantity", "_avg_cost", "_realized_pnl")

    def __init__(self) -> None:
        self._quantity: Quantity = Quantity(Decimal(0))
        self._avg_cost: Price = Price(Decimal(0))
        self._realized_pnl: Price = Price(Decimal(0))

    @property
    def quantity(self) -> Quantity:
        return self._quantity

    @property
    def average_entry_price(self) -> Price:
        return self._avg_cost

    @property
    def realized_pnl(self) -> Price:
        return self._realized_pnl

    def unrealized_pnl(self, mark_price: Price) -> Price:
        if self._quantity == 0:
            return Price(Decimal(0))
        return Price((mark_price - self._avg_cost) * self._quantity)

    def apply_fill(self, fill: FillEvent) -> None:
        # Fees come straight off realized regardless of what the fill does.
        self._realized_pnl = Price(self._realized_pnl - fill.fee)

        signed_fill = fill.fill_quantity * Decimal(fill.side.sign)
        new_qty = self._quantity + signed_fill

        # --- Case 1: opening from flat -------------------------------------
        if self._quantity == 0:
            self._quantity = Quantity(signed_fill)
            self._avg_cost = fill.fill_price
            return

        # --- Case 2: same side — extending --------------------------------
        same_side = (self._quantity > 0) == (signed_fill > 0)
        if same_side:
            total_cost = (
                self._avg_cost * abs(self._quantity)
                + fill.fill_price * fill.fill_quantity
            )
            self._quantity = Quantity(new_qty)
            self._avg_cost = Price(total_cost / abs(new_qty))
            return

        # --- Case 3 & 4: opposite side ------------------------------------
        # position_sign = +1 if currently long, -1 if short.
        position_sign = 1 if self._quantity > 0 else -1

        if abs(signed_fill) <= abs(self._quantity):
            # Partial or exact close, no flip.
            close_qty = fill.fill_quantity
            pnl = Decimal(position_sign) * (fill.fill_price - self._avg_cost) * close_qty
            self._realized_pnl = Price(self._realized_pnl + pnl)
            self._quantity = Quantity(new_qty)
            if new_qty == 0:
                self._avg_cost = Price(Decimal(0))
            # else: avg cost unchanged on remaining position.
            return

        # Flip: close everything, then open fresh on the opposite side.
        closed_qty = abs(self._quantity)
        pnl = Decimal(position_sign) * (fill.fill_price - self._avg_cost) * closed_qty
        self._realized_pnl = Price(self._realized_pnl + pnl)
        remaining = fill.fill_quantity - closed_qty
        self._quantity = Quantity(Decimal(fill.side.sign) * remaining)
        self._avg_cost = fill.fill_price


__all__ = ["WAVGBook"]
