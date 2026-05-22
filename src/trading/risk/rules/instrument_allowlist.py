"""InstrumentAllowlist: restrict a strategy to a configured set of instruments.

Defence against a strategy bug (or config error) that has it submitting
signals on an instrument it was never authorised for. Rejecting hard
because there's no sensible clamp — wrong instrument is wrong.
"""

from __future__ import annotations

from collections.abc import Iterable

from ...core.events import SignalEvent
from ..base import AbstractRiskRule, RuleResult
from ..state import RiskState


class InstrumentAllowlistRule(AbstractRiskRule):
    """Reject signals on instruments outside the configured allow-list."""

    def __init__(self, *, allowed_instrument_ids: Iterable[str]) -> None:
        self._allowed = frozenset(allowed_instrument_ids)
        if not self._allowed:
            raise ValueError("allowed_instrument_ids must be non-empty")

    @property
    def name(self) -> str:
        return "instrument_allowlist"

    def evaluate(self, signal: SignalEvent, state: RiskState) -> RuleResult:
        iid = signal.instrument.instrument_id
        if iid in self._allowed:
            return RuleResult.approve(self.name)
        return RuleResult.reject(
            self.name,
            reason=f"instrument {iid} not in allow-list",
        )


__all__ = ["InstrumentAllowlistRule"]
