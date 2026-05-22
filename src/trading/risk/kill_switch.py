"""Kill switch.

The kill switch is the system's final escalation. Once latched on, the
risk engine rejects every signal until an operator manually resets it.
Operators trip it manually; rules trip it programmatically when they
hit a severity-KILL condition (daily loss limit, recurring rejects,
external alert).

The switch carries the reason and the rule/source that tripped it, so
the resulting alert is actionable.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..core.clock import Clock
from ..core.types import Timestamp


@dataclass(frozen=True, slots=True)
class KillSwitchState:
    engaged: bool
    triggered_by: str = ""
    reason: str = ""
    triggered_at_ns: Timestamp = 0


class KillSwitch:
    """Latched system-wide stop. Thread-safe for reads; writes via engine only."""

    __slots__ = ("_clock", "_state")

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._state: KillSwitchState = KillSwitchState(engaged=False)

    @property
    def engaged(self) -> bool:
        return self._state.engaged

    @property
    def state(self) -> KillSwitchState:
        return self._state

    def engage(self, *, triggered_by: str, reason: str) -> KillSwitchState:
        """Latch on. Idempotent — the first reason wins.

        Returning the state lets the engine emit a single KillSwitchEvent
        with exactly the fields that latched.
        """
        if self._state.engaged:
            return self._state
        self._state = KillSwitchState(
            engaged=True,
            triggered_by=triggered_by,
            reason=reason,
            triggered_at_ns=self._clock.now_ns(),
        )
        return self._state

    def reset(self) -> None:
        """Operator-only. Clears the latch."""
        self._state = KillSwitchState(engaged=False)


__all__ = ["KillSwitch", "KillSwitchState"]
