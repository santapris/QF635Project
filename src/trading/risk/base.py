"""Risk rule abstraction.

A rule is a pure function ``(signal, state) -> RuleResult``. It must
not mutate state, must not perform I/O, and must return quickly — it's
on the hot path for every signal that flows.

The engine composes rule results:

- if *any* rule returns ``approved=False``, the signal is rejected
- among approving rules that suggest a clamped quantity, the engine
  takes the minimum (the tightest constraint wins)
- ``INFO`` / ``WARN`` severities annotate but don't reject

Rules carry their parameters in their constructor. Construction time
is the right place for validation; the hot path stays branch-light.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from ..core.events import SignalEvent
from ..core.types import Quantity, Severity


@dataclass(frozen=True, slots=True)
class RuleResult:
    """One rule's verdict on one signal."""

    rule_name: str
    approved: bool
    severity: Severity = Severity.INFO
    reason: str = ""
    approved_quantity: Quantity | None = None
    """If set on an approving result, suggests the signal be clamped to this size.

    The engine takes ``min(approved_quantity)`` across all approving rules
    that set this; the original requested quantity is the implicit ceiling.
    """

    @classmethod
    def approve(cls, rule_name: str, *, clamp_to: Quantity | None = None) -> "RuleResult":
        return cls(
            rule_name=rule_name,
            approved=True,
            severity=Severity.INFO,
            approved_quantity=clamp_to,
        )

    @classmethod
    def reject(
        cls,
        rule_name: str,
        reason: str,
        *,
        severity: Severity = Severity.BLOCK,
    ) -> "RuleResult":
        return cls(
            rule_name=rule_name,
            approved=False,
            severity=severity,
            reason=reason,
        )


class AbstractRiskRule(ABC):
    """Pre-trade risk rule."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Short, stable name used in events and logs. e.g. 'max_position'."""

    @abstractmethod
    def evaluate(self, signal: SignalEvent, state: "RiskState") -> RuleResult:  # noqa: F821
        """Decide whether ``signal`` is acceptable. Must be pure and fast."""


__all__ = ["AbstractRiskRule", "RuleResult"]
