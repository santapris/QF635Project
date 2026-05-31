"""MaxPosition: cap absolute inventory per (strategy, instrument).

This rule prefers to *clamp* rather than reject. If a strategy asks for
size 0.5 but only 0.3 of long headroom remains, the rule approves with
``approved_quantity=0.3``. Only when headroom is zero (or the signal
would extend an already-capped position) does the rule reject.

Clamping is the right default for a position cap. Rejecting outright
turns a strategy that would have placed *some* business into one that
placed none, which is rarely what an operator wants.

Signal-as-snapshot semantics (the authoritative statement of this contract
lives on :class:`SignalEvent`; this is the MaxPosition-specific consequence).
A signal is a strategy's complete desired resting state, not an increment on
top of what is already working. So the cap is checked against confirmed
position plus the same-side legs *in this signal*, NOT plus already-working
orders — counting working orders would double-count a re-quote against the
order it replaces. Same-side legs within one signal are summed against each
other, so a multi-leg ladder is still bounded as a whole.

This relies on signals being authoritative snapshots. A strategy that instead
emits independent incremental signals (each meant to coexist) is not bounded by
this rule alone.
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

        # Sum the *other* same-side legs in this signal. The signal is the
        # strategy's full desired resting state, so siblings on the same side
        # share the cap — but this leg's own quantity is what `headroom` is
        # compared against / clamped to, so it must not be counted here.
        # Working orders are deliberately ignored (see module docstring): the
        # OMS reconciles them to the signal, so they are not additional.
        siblings = sum(
            (o.quantity for o in signal.legs if o is not leg and o.side is leg.side),
            Quantity(Decimal(0)),
        )

        if leg.side is Side.BUY:
            # Ceiling is +max_long: confirmed long + sibling buys + this leg.
            headroom = self._max_long - (current + siblings)
        else:
            # Floor is -max_short: confirmed (signed) - sibling sells - this leg.
            headroom = (current - siblings) + self._max_short

        if headroom <= 0:
            return RuleResult.reject(
                self.name,
                reason=(
                    f"position cap reached: current={current}, "
                    f"same_side_siblings={siblings}, "
                    f"max_long={self._max_long}, max_short={-self._max_short}"
                ),
            )
        if headroom < leg.quantity:
            return RuleResult.approve(self.name, clamp_to=Quantity(headroom))
        return RuleResult.approve(self.name)


__all__ = ["MaxPositionRule"]
