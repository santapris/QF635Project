"""MaxOrderSize: cap the size of a single order.

Distinct from :class:`MaxPositionRule`: this caps a single click, not
the cumulative position. Useful as a fat-finger guard — a strategy bug
that asks for 1000x the intended size is caught here.

Default policy: clamp. Rejecting a 1000x-bug to a single tick-sized
order is safer than passing it through, and also safer than rejecting
entirely (which would let the strategy retry on the next signal).
"""

from __future__ import annotations

from ...core.events import OrderLeg, SignalEvent
from ...core.types import Quantity
from ..base import AbstractRiskRule, RuleResult
from ..state import RiskState


class MaxOrderSizeRule(AbstractRiskRule):
    """Bound the quantity of a single order."""

    def __init__(self, *, max_quantity: Quantity) -> None:
        if max_quantity <= 0:
            raise ValueError("max_quantity must be positive")
        self._max_quantity = max_quantity

    @property
    def name(self) -> str:
        return "max_order_size"

    def evaluate(self, signal: SignalEvent, leg: OrderLeg, state: RiskState) -> RuleResult:
        if leg.quantity <= self._max_quantity:
            return RuleResult.approve(self.name)
        return RuleResult.approve(
            self.name, 
            clamp_to=self._max_quantity,
            reason=(
                f"clamped {leg.quantity} => {self._max_quantity} "
                f"(max_order_size limit)"
            ),
        )


__all__ = ["MaxOrderSizeRule"]
