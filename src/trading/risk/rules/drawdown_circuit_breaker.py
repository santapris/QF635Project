"""DrawdownCircuitBreakerRule: multi-severity halt based on session drawdown.

Complements DailyLossLimitRule (which only tracks realized PnL and escalates
directly to KILL) by:

- Including unrealized PnL in the equity calculation — a large open losing
  position is visible before it is closed.
- A two-tier escalation: WARN at a soft threshold (signal blocked, alert
  emitted), KILL at a hard threshold (kill switch engaged).
- Drawdown measured from the session-peak total equity (realized + unrealized),
  so intraday recoveries reset the effective baseline.

State is maintained in RiskState._session_peak_equity, updated on every
PositionUpdateEvent — no additional subscriptions required.
"""

from __future__ import annotations

from ...core.events import OrderLeg, SignalEvent
from ...core.types import Severity
from ..base import AbstractRiskRule, RuleResult
from ..state import RiskState


class DrawdownCircuitBreakerRule(AbstractRiskRule):
    """Halt quoting when session drawdown exceeds configurable thresholds.

    Parameters
    ----------
    warn_pct:
        Drawdown fraction (0.10 = 10%) at which signals are blocked with
        Severity.WARN and a risk alert is emitted. Trading is paused until
        equity recovers above this threshold.
    kill_pct:
        Drawdown fraction at which the kill switch is engaged (Severity.KILL).
        Must be >= warn_pct.
    """

    def __init__(self, *, warn_pct: float = 0.10, kill_pct: float = 0.20) -> None:
        if not 0.0 < warn_pct <= 1.0:
            raise ValueError(f"warn_pct must be in (0, 1], got {warn_pct}")
        if not warn_pct <= kill_pct <= 1.0:
            raise ValueError(f"kill_pct must be >= warn_pct and <= 1, got {kill_pct}")
        self._warn_pct = warn_pct
        self._kill_pct = kill_pct

    @property
    def name(self) -> str:
        return "drawdown_circuit_breaker"

    def evaluate(self, signal: SignalEvent, leg: OrderLeg, state: RiskState) -> RuleResult:
        dd = state.get_drawdown_pct(signal.strategy_id)
        if dd is None:
            return RuleResult.approve(self.name)  # cold start — no position data yet

        if dd >= self._kill_pct:
            return RuleResult.reject(
                self.name,
                reason=(
                    f"session drawdown {dd:.1%} >= kill threshold {self._kill_pct:.1%}; "
                    "engaging kill switch"
                ),
                severity=Severity.KILL,
            )

        if dd >= self._warn_pct:
            return RuleResult.reject(
                self.name,
                reason=(
                    f"session drawdown {dd:.1%} >= warn threshold {self._warn_pct:.1%}; "
                    "blocking until equity recovers"
                ),
                severity=Severity.WARN,
            )

        return RuleResult.approve(self.name)


__all__ = ["DrawdownCircuitBreakerRule"]
