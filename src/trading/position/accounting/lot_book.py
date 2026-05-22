"""FIFO/LIFO common implementation.

FIFO and LIFO differ only in *which lot to consume first* on a reducing
fill. Everything else — extension, flipping, fee handling, the
average-entry-price computation — is identical. Subclasses override
:meth:`_pop_lot` and :meth:`_peek_lot`.

Lots all carry positive quantities. The book's ``_side_sign`` tracks
whether the position is long (+1), short (-1), or flat (0). A single
book never holds mixed-side lots; a flip closes all lots first, then
opens new ones on the opposite side.
"""

from __future__ import annotations

from collections import deque
from decimal import Decimal

from ...core.events import FillEvent
from ...core.types import Price, Quantity
from ..lots import Lot
from .base import AccountingBook


class _LotBook(AccountingBook):
    """Shared logic for FIFO/LIFO. Subclasses pick the consumption order."""

    __slots__ = ("_lots", "_side_sign", "_realized_pnl")

    def __init__(self) -> None:
        self._lots: deque[Lot] = deque()
        self._side_sign: int = 0  # +1 long, -1 short, 0 flat
        self._realized_pnl: Price = Price(Decimal(0))

    # --- Subclass hooks ---------------------------------------------------

    def _peek_lot(self) -> Lot:
        """Return the lot that will be consumed next. Implemented by subclass."""
        raise NotImplementedError

    def _pop_lot(self) -> Lot:
        """Remove and return the lot that consumes next. Implemented by subclass."""
        raise NotImplementedError

    # --- AccountingBook protocol ------------------------------------------

    @property
    def quantity(self) -> Quantity:
        total = sum((lot.quantity for lot in self._lots), Decimal(0))
        return Quantity(Decimal(self._side_sign) * total)

    @property
    def average_entry_price(self) -> Price:
        if not self._lots:
            return Price(Decimal(0))
        total_qty = Decimal(0)
        total_cost = Decimal(0)
        for lot in self._lots:
            total_qty += lot.quantity
            total_cost += lot.quantity * lot.price
        return Price(total_cost / total_qty)

    @property
    def realized_pnl(self) -> Price:
        return self._realized_pnl

    def unrealized_pnl(self, mark_price: Price) -> Price:
        if self._side_sign == 0:
            return Price(Decimal(0))
        # Sum (mark - lot_price) * qty across lots, signed by side.
        # Equivalent to (mark - avg_cost) * total_qty but loops once.
        total = Decimal(0)
        for lot in self._lots:
            total += (mark_price - lot.price) * lot.quantity
        return Price(Decimal(self._side_sign) * total)

    def apply_fill(self, fill: FillEvent) -> None:
        self._realized_pnl = Price(self._realized_pnl - fill.fee)

        fill_sign = fill.side.sign
        remaining = fill.fill_quantity

        # --- Case 1: flat — opening a new position ------------------------
        if self._side_sign == 0:
            self._side_sign = fill_sign
            self._lots.append(Lot(quantity=remaining, price=fill.fill_price))
            return

        # --- Case 2: same side — extending --------------------------------
        if self._side_sign == fill_sign:
            self._lots.append(Lot(quantity=remaining, price=fill.fill_price))
            return

        # --- Case 3/4: opposite side — close lots, then possibly flip -----
        # Realized PnL per closed unit:
        #   long lot closed by sell: pnl_unit = sell_price - lot_price
        #   short lot closed by buy: pnl_unit = lot_price - buy_price
        # Generically: pnl_unit = self._side_sign * (fill_price - lot_price)
        while remaining > 0 and self._lots:
            lot = self._peek_lot()
            close_qty = min(lot.quantity, remaining)
            pnl_unit = Decimal(self._side_sign) * (fill.fill_price - lot.price)
            self._realized_pnl = Price(self._realized_pnl + pnl_unit * close_qty)
            lot.quantity = Quantity(lot.quantity - close_qty)
            remaining -= close_qty
            if lot.quantity == 0:
                self._pop_lot()

        if not self._lots:
            self._side_sign = 0
            if remaining > 0:
                # Flipped: open a new lot on the opposite side.
                self._side_sign = fill_sign
                self._lots.append(Lot(quantity=remaining, price=fill.fill_price))


class FIFOBook(_LotBook):
    """Consume oldest lots first."""

    def _peek_lot(self) -> Lot:
        return self._lots[0]

    def _pop_lot(self) -> Lot:
        return self._lots.popleft()


class LIFOBook(_LotBook):
    """Consume newest lots first."""

    def _peek_lot(self) -> Lot:
        return self._lots[-1]

    def _pop_lot(self) -> Lot:
        return self._lots.pop()


__all__ = ["FIFOBook", "LIFOBook"]
