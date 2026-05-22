"""Accounting book abstraction.

A *book* is the per-(strategy, instrument) ledger that decides how to
compute realized PnL when a position is reduced. Three implementations:

- :class:`WAVGBook` — weighted average cost; one running number.
- :class:`FIFOBook` — oldest-first lot consumption.
- :class:`LIFOBook` — newest-first lot consumption.

All three expose the same external state: signed quantity, average
entry price of the remaining open position, and cumulative realized
PnL. The choice of method shifts when PnL is recognised (realized vs
unrealized) but the total economic outcome is the same — what
differs is the *path* the numbers take through the lifetime of the
position.

Fees: every fill carries a positive fee in ``fill.fee``. The book
subtracts fees from realized PnL at the moment of the fill. (We do not
fold fees into cost basis; doing so muddles the "what was my entry
price" reporting that strategies and dashboards expect.)
"""

from __future__ import annotations

from decimal import Decimal
from typing import Protocol, runtime_checkable

from ...core.events import FillEvent
from ...core.types import Price, Quantity


@runtime_checkable
class AccountingBook(Protocol):
    """Per-(strategy, instrument) PnL ledger."""

    def apply_fill(self, fill: FillEvent) -> None:
        """Mutate the book to reflect a new fill."""

    @property
    def quantity(self) -> Quantity:
        """Signed: +long, -short, 0 flat."""

    @property
    def average_entry_price(self) -> Price:
        """Weighted-average entry price of the remaining open position.

        Zero when the book is flat. For FIFO/LIFO this is the volume-
        weighted average of the open lots, not the avg of the lots that
        were closed.
        """

    @property
    def realized_pnl(self) -> Price:
        """Cumulative realized PnL, net of fees."""

    def unrealized_pnl(self, mark_price: Price) -> Price:
        """Unrealized PnL at ``mark_price`` for the current open position.

        Defined as ``sign(quantity) * (mark - avg_entry) * |quantity|``.
        Returns zero when flat.
        """


__all__ = ["AccountingBook"]
