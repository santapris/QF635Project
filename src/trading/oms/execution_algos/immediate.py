"""Immediate execution: one child order equal to the full parent size.

Used for MARKET orders and any signal without a non-trivial execution
algo. The "algo" is really just a uniform interface around the
default path.
"""

from __future__ import annotations

from ...core.types import OrderType, Price, Quantity, TimeInForce, Timestamp
from .base import ChildOrderSpec, ExecutionAlgo


class ImmediateAlgo(ExecutionAlgo):
    """Single-shot. Emits one slice of size ``quantity``, then is done."""

    __slots__ = ("_quantity", "_order_type", "_time_in_force", "_price", "_done")

    def __init__(
        self,
        *,
        quantity: Quantity,
        order_type: OrderType,
        time_in_force: TimeInForce,
        price: Price | None = None,
    ) -> None:
        self._quantity = quantity
        self._order_type = order_type
        self._time_in_force = time_in_force
        self._price = price
        self._done = False

    def next_slice(self, now_ns: Timestamp) -> ChildOrderSpec | None:
        if self._done:
            return None
        self._done = True
        return ChildOrderSpec(
            quantity=self._quantity,
            order_type=self._order_type,
            time_in_force=self._time_in_force,
            price=self._price,
        )

    def is_done(self) -> bool:
        return self._done

    def cancel(self) -> None:
        self._done = True


__all__ = ["ImmediateAlgo"]
