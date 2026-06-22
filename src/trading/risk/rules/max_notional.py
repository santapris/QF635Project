"""MaxNotional: cap order notional in quote currency.

Complements :class:`MaxOrderSizeRule`. A size cap of 1 BTC means
something very different when BTC is $10k vs $100k; a notional cap of
$50k is invariant. Both rules can coexist; the engine takes the tightest
constraint.

Requires a reference price. We use ``signal.suggested_price`` if set
(limit orders include it); otherwise we cannot evaluate and approve
conservatively. Strategies that want notional enforced on market orders
should set ``suggested_price`` to the latest mid.
"""

from __future__ import annotations

from decimal import Decimal

from ...core.events import OrderLeg, SignalEvent
from ...core.types import Price, Quantity
from ..base import AbstractRiskRule, RuleResult
from ..state import RiskState


class MaxNotionalRule(AbstractRiskRule):
    """Bound order notional (price * quantity) in quote currency."""

    def __init__(self, *, max_notional: Price) -> None:
        if max_notional <= 0:
            raise ValueError("max_notional must be positive")
        self._max_notional = max_notional

    @property
    def name(self) -> str:
        return "max_notional"

    def evaluate(self, signal: SignalEvent, leg: OrderLeg, state: RiskState) -> RuleResult:
        price = leg.price
        if price is None or price <= 0:
            # Can't enforce without a price. Approve and let other rules
            # (max_order_size, max_position) catch egregious sizes.
            return RuleResult.approve(self.name)

        notional = price * leg.quantity
        if notional <= self._max_notional:
            return RuleResult.approve(self.name)

        # Clamp by reducing quantity to fit the cap.
        clamped_qty = Quantity(self._max_notional / price)
        if clamped_qty <= 0:
            return RuleResult.reject(
                self.name,
                reason=(
                    f"notional {notional} > cap {self._max_notional} "
                    f"and clamped quantity would be zero"
                ),
            )
        return RuleResult.approve(
            self.name, 
            clamp_to=clamped_qty, 
            reason=(
                f"clamped {leg.quantity} -> {clamped_qty:.6g} "
                f"(notional {notional:.2f} > cap {self._max_notional})"
            ),
            )


__all__ = ["MaxNotionalRule"]
