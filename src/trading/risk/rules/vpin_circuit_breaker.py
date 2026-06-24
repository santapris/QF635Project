"""VPINCircuitBreaker: halt quoting when order flow toxicity is sustained. 

Measure order flow toxicity using VPIN (Volume-Synchronized Probability of Informed Trading) and halt quoting when the VPIN exceeds a threshold for a sustained period of time.
Rule listens to cached VPIN values updated from MicrostructureSnapshotEvent. 
Engages kill switch after threwshold is breached for at least a sustained number of ticks, preventing a single noisy spike from halting the strat. 

2 layers: 
- strat: VPIN > threshold => widen spread 3x (quotes are still live, but less aggressive)
- risk (this rule): VPIN > threshold for sustained period => halt quoting (quotes are dead)
"""

from __future__ import annotations

from collections import defaultdict, deque

from ...core.events import MicrostructureSnapshotEvent, OrderLeg, SignalEvent
from ...core.types import Severity
from trading.risk.base import AbstractRiskRule, RuleResult
from ..state import RiskState


class VPINCircuitBreakerRule(AbstractRiskRule):
    """VPINCircuitBreaker: halt quoting when order flow toxicity is sustained. 
    
    Parameters
    ----------
    threshold: 
    - VPIN of 0.7  as high toxicity threshold (Ref Easley et al. 2012) Put as 0.8 for a more conservative approach.
    sustained_ticks:
    - No of ticks VPIN must be above threshold to trigger kill switch.
    """

    def __init__(self, *, threshold: float = 0.8, sustained_ticks: int = 5) -> None:
        if not 0.0 <= threshold <= 1.0:
            raise ValueError(f"threshold must be between 0 and 1, got {threshold}")
        if sustained_ticks <= 0:
            raise ValueError(f"sustained_ticks must be a positive integer, got {sustained_ticks}")
        
        self._threshold = threshold
        self._sustained_ticks = sustained_ticks

    @property
    def name(self) -> str:
        return "vpin_circuit_breaker"
    
    def evaluate(self, signal: SignalEvent, leg: OrderLeg, state: RiskState) -> RuleResult:
        """Evaluate the VPIN circuit breaker rule.
        
        Parameters
        ----------
        signal : SignalEvent
            The signal event to evaluate.
        leg : OrderLeg
            The order leg associated with the signal.
        state : RiskState
            The current risk state of the strategy.

        Returns
        -------
        RuleResult
            The result of the rule evaluation, indicating whether to halt quoting or not.
        """
        instrument_id = signal.instrument.instrument_id
        vpin_value = state.get_vpin(signal.strategy_id, instrument_id)  # Assuming this method retrieves the latest VPIN value
        
        if vpin_value is None:
            # VPIN not yet warmed up, let strategy continue quoting
            return RuleResult.approve(self.name, reason="No VPIN value available. Quoting continues.")
        
        ticks_above = state.get_vpin_breach_ticks(
            signal.strategy_id, instrument_id, threshold=self._threshold
        )

        if vpin_value >= self._threshold and ticks_above >= self._sustained_ticks:
            return RuleResult.reject(
                self.name, 
                reason=(
                    f"VPIN value {vpin_value:.2f} has exceeded threshold {self._threshold} for "
                    f"{ticks_above} ticks, which is above the sustained threshold of {self._sustained_ticks}. "
                    "Halting quoting to defend against sustained order flow toxicity."
                ), 
                severity=Severity.KILL
            )
        
        if vpin_value >= self._threshold:
            return RuleResult.reject(
                self.name, 
                reason=(
                    f"VPIN value {vpin_value:.2f} has exceeded threshold {self._threshold} for "
                    f"{ticks_above} ticks, which is below the sustained threshold of {self._sustained_ticks}. "
                    "Blocking quptes until sustained breach or reset."
                ),
                severity=Severity.BLOCK
            )
        
        return RuleResult.approve(self.name, "VPIN below threshold. Quoting continues.")


__all__ = ["VPINCircuitBreakerRule"]