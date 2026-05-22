"""Time-Weighted Average Price.

Slices a parent quantity into ``num_slices`` equal pieces evenly spaced
over ``duration_seconds``. The first slice fires immediately; the last
fires near the end of the window.

Implementation notes:

- Slice size and interval are computed once at construction. The
  algorithm does not adapt to fills or market conditions — that's
  intentional. TWAP's point is predictable scheduling; if you want
  adaptivity, use VWAP or write a custom algo.
- The remainder of integer division (e.g. 100 / 3 = 33 with 1 left
  over) is added to the final slice.
- Quantity rounding happens at the OMS layer via ``instrument.round_quantity``;
  the algo emits raw decimals.
"""

from __future__ import annotations

from decimal import Decimal

from ...core.exceptions import ConfigError
from ...core.types import OrderType, Price, Quantity, TimeInForce, Timestamp
from .base import ChildOrderSpec, ExecutionAlgo


_NS_PER_SECOND = 1_000_000_000


class TWAPAlgo(ExecutionAlgo):
    """Equal-size slices, equal time spacing."""

    __slots__ = (
        "_quantity", "_num_slices", "_interval_ns", "_slice_qty",
        "_remainder", "_order_type", "_time_in_force", "_price",
        "_slices_sent", "_start_ns", "_cancelled",
    )

    def __init__(
        self,
        *,
        quantity: Quantity,
        duration_seconds: float,
        num_slices: int,
        start_ns: Timestamp,
        order_type: OrderType = OrderType.MARKET,
        time_in_force: TimeInForce = TimeInForce.IOC,
        price: Price | None = None,
    ) -> None:
        if num_slices <= 0:
            raise ConfigError("num_slices must be positive")
        if duration_seconds <= 0:
            raise ConfigError("duration_seconds must be positive")
        if quantity <= 0:
            raise ConfigError("quantity must be positive")

        self._quantity = quantity
        self._num_slices = num_slices
        self._interval_ns = int(duration_seconds * _NS_PER_SECOND) // num_slices
        self._order_type = order_type
        self._time_in_force = time_in_force
        self._price = price

        # Slice math: even split, with any remainder going to the last slice.
        self._slice_qty = Quantity(quantity / Decimal(num_slices))
        sent_total = self._slice_qty * (num_slices - 1)
        self._remainder = Quantity(quantity - sent_total)

        self._slices_sent = 0
        self._start_ns = start_ns
        self._cancelled = False

    def next_slice(self, now_ns: Timestamp) -> ChildOrderSpec | None:
        if self._cancelled or self._slices_sent >= self._num_slices:
            return None
        # When should the next slice fire?
        target_ns = self._start_ns + self._slices_sent * self._interval_ns
        if now_ns < target_ns:
            return None

        is_last = self._slices_sent == self._num_slices - 1
        qty = self._remainder if is_last else self._slice_qty
        self._slices_sent += 1
        return ChildOrderSpec(
            quantity=qty,
            order_type=self._order_type,
            time_in_force=self._time_in_force,
            price=self._price,
        )

    def is_done(self) -> bool:
        return self._cancelled or self._slices_sent >= self._num_slices

    def cancel(self) -> None:
        self._cancelled = True


__all__ = ["TWAPAlgo"]
