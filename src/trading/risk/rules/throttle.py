"""Throttle: cap signal rate per strategy.

A misbehaving strategy that fires hundreds of signals per second is
both a risk (bad logic in flight) and an expense (exchange rate limits,
fees). The throttle rule rejects when the strategy's signal count in a
sliding window exceeds the configured cap.

Rejects, doesn't clamp. Throttling means "you're moving too fast; slow
down." Approving a fraction of the next signal would still be too fast.
"""

from __future__ import annotations

from ...core.events import SignalEvent
from ...core.types import Severity
from ..base import AbstractRiskRule, RuleResult
from ..state import RiskState


class ThrottleRule(AbstractRiskRule):
    """Bound signals-per-window per strategy. Rejects when the cap is exceeded."""

    def __init__(self, *, max_signals: int, window_seconds: float = 60.0) -> None:
        if max_signals <= 0:
            raise ValueError("max_signals must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._max_signals = max_signals
        self._window_seconds = window_seconds

    @property
    def name(self) -> str:
        return "throttle"

    def evaluate(self, signal: SignalEvent, state: RiskState) -> RuleResult:
        # The state's recent-signal window includes the *current* signal —
        # the engine records it before running rules. So the comparison is
        # "are we at or above the cap *including this one*".
        count = state.signals_in_window(
            signal.strategy_id, window_seconds=self._window_seconds
        )
        if count > self._max_signals:
            return RuleResult.reject(
                self.name,
                reason=(
                    f"throttle exceeded: {count} signals in last "
                    f"{self._window_seconds}s (cap={self._max_signals})"
                ),
                severity=Severity.WARN,
            )
        return RuleResult.approve(self.name)


__all__ = ["ThrottleRule"]
