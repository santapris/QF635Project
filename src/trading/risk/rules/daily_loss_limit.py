"""DailyLossLimit: stop trading once today's realized loss exceeds a floor.

The rule emits ``Severity.KILL`` rather than ``BLOCK`` when it trips,
which the engine recognises as a kill-switch trigger. Once the kill
switch is on, *no* signal flows from anyone until an operator resets,
which is the right behaviour for a daily-loss breach: a strategy in
that state needs human attention before it resumes.
"""

from __future__ import annotations

from decimal import Decimal

from ...core.events import SignalEvent
from ...core.types import Price, Severity
from ..base import AbstractRiskRule, RuleResult
from ..state import RiskState


class DailyLossLimitRule(AbstractRiskRule):
    """Trigger kill on daily realized loss > limit. Limit is expressed as a positive number."""

    def __init__(self, *, max_loss: Price) -> None:
        if max_loss <= 0:
            raise ValueError("max_loss must be positive (it's the absolute loss cap)")
        # Stored as a -ve threshold for direct comparison against signed PnL.
        self._floor: Price = -max_loss

    @property
    def name(self) -> str:
        return "daily_loss_limit"

    def evaluate(self, signal: SignalEvent, state: RiskState) -> RuleResult:
        pnl = state.get_realized_pnl_today(signal.strategy_id)
        if pnl <= self._floor:
            return RuleResult.reject(
                self.name,
                reason=(
                    f"daily realized PnL {pnl} ≤ floor {self._floor}; "
                    f"engaging kill switch"
                ),
                severity=Severity.KILL,
            )
        return RuleResult.approve(self.name)


__all__ = ["DailyLossLimitRule"]
