"""MaxPosition: cap absolute inventory per (strategy, instrument).

This rule prefers to *clamp* rather than reject. If a strategy asks for
size 0.5 but only 0.3 of long headroom remains, the rule approves with
``approved_quantity=0.3``. Only when headroom is zero (or the signal
would extend an already-capped position) does the rule reject.

Clamping is the right default for a position cap. Rejecting outright
turns a strategy that would have placed *some* business into one that
placed none, which is rarely what an operator wants.

Headroom is computed against *effective* exposure — confirmed position
plus working (unfilled) orders on the same side — not confirmed position
alone. Without this, a strategy could get N orders each individually
approved that together blow past the cap if they all fill, because each
evaluation only saw the confirmed fills at that instant. Working-order
exposure comes from the OMS via OpenOrdersSnapshotEvent (see RiskState).
"""

from __future__ import annotations

from decimal import Decimal

from ...core.events import OrderLeg, SignalEvent
from ...core.types import Quantity, Side
from ..base import AbstractRiskRule, RuleResult
from ..state import RiskState


class MaxPositionRule(AbstractRiskRule):
    """Bound absolute long/short position for one strategy."""

    def __init__(
        self,
        *,
        max_long: Quantity,
        max_short: Quantity,
    ) -> None:
        if max_long < 0:
            raise ValueError("max_long must be non-negative")
        if max_short < 0:
            raise ValueError("max_short must be non-negative (use positive number)")
        self._max_long = max_long
        # Stored as a positive number; converted to a -ve floor at evaluate time.
        self._max_short = max_short

    @property
    def name(self) -> str:
        return "max_position"

    def evaluate(self, signal: SignalEvent, leg: OrderLeg, state: RiskState) -> RuleResult:
        current = state.get_position(signal.strategy_id, signal.instrument)
        working_buy, working_sell = state.get_working(
            signal.strategy_id, signal.instrument
        )

        if leg.side is Side.BUY:
            # Worst case: confirmed long + all working buys + this leg, all
            # filled. Ceiling is +max_long.
            headroom = self._max_long - (current + working_buy)
        else:
            # Worst case: confirmed (signed) - all working sells - this leg.
            # Floor is -max_short, so sellable = (current - working_sell) + max_short.
            headroom = (current - working_sell) + self._max_short

        if headroom <= 0:
            return RuleResult.reject(
                self.name,
                reason=(
                    f"position cap reached: current={current}, "
                    f"working_buy={working_buy}, working_sell={working_sell}, "
                    f"max_long={self._max_long}, max_short={-self._max_short}"
                ),
            )
        if headroom < leg.quantity:
            return RuleResult.approve(self.name, clamp_to=Quantity(headroom))
        return RuleResult.approve(self.name)


__all__ = ["MaxPositionRule"]
