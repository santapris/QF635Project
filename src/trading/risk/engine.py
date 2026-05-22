"""Risk engine.

Subscribes to:

- ``signals``  → evaluates rules, publishes RiskDecision
- ``fills``    → updates positions in state
- ``positions``→ updates positions + realized PnL in state

Publishes to:

- ``risk-decisions`` for every signal it sees (approval or rejection)
- ``alerts`` whenever a rule emits ``WARN`` or above
- ``alerts`` (KillSwitchEvent) when the kill switch trips

Composition of rule results:

1. Kill switch latched → reject immediately (no rules run).
2. Run all rules. If *any* returns ``approved=False``:
     - if severity is ``KILL``: engage kill switch, then reject this signal.
     - otherwise: reject this signal with the first failing rule's reason.
3. If all approved: ``approved_quantity = min(non-None clamps, requested_qty)``.

The engine processes signals serially per coroutine — concurrent
signal handling would race on state updates. The bus's per-subscriber
queue gives us that serialisation for free.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from decimal import Decimal

from ..core.clock import Clock
from ..core.events import (
    BaseEvent,
    FillEvent,
    KillSwitchEvent,
    PositionUpdateEvent,
    RiskAlertEvent,
    RiskDecision,
    SignalEvent,
)
from ..core.exceptions import BackpressureError
from ..core.types import Quantity, Severity, StrategyId
from ..event_bus.base import AbstractEventBus, Topic
from .base import AbstractRiskRule, RuleResult
from .kill_switch import KillSwitch
from .state import RiskState

_log = logging.getLogger(__name__)


class RiskEngine:
    """Pre-trade risk evaluation and state-of-the-world tracking."""

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        clock: Clock,
        state: RiskState | None = None,
        kill_switch: KillSwitch | None = None,
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._state = state or RiskState(clock=clock)
        self._kill_switch = kill_switch or KillSwitch(clock)
        self._rules: dict[StrategyId, list[AbstractRiskRule]] = {}
        # Rules applied to every strategy. Useful for global guards like
        # instrument allow-lists shared across many strategies.
        self._global_rules: list[AbstractRiskRule] = []
        self._started = False
        self._dropped_events: int = 0

    # --- Configuration ----------------------------------------------------

    def register_rules(
        self, strategy_id: StrategyId, rules: Iterable[AbstractRiskRule]
    ) -> None:
        """Attach rules to a specific strategy. Order is preserved."""
        if self._started:
            raise RuntimeError("cannot register rules after start()")
        self._rules.setdefault(strategy_id, []).extend(rules)

    def register_global_rules(self, rules: Iterable[AbstractRiskRule]) -> None:
        """Attach rules that apply to every signal regardless of strategy."""
        if self._started:
            raise RuntimeError("cannot register rules after start()")
        self._global_rules.extend(rules)

    @property
    def state(self) -> RiskState:
        return self._state

    @property
    def kill_switch(self) -> KillSwitch:
        return self._kill_switch

    # --- Lifecycle --------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self._bus.subscribe(Topic.SIGNALS, self._on_signal)
        await self._bus.subscribe(Topic.FILLS, self._on_fill)
        await self._bus.subscribe(Topic.POSITIONS, self._on_position_update)

    async def stop(self) -> None:
        self._started = False

    # --- Event handlers ---------------------------------------------------

    async def _on_signal(self, event: BaseEvent) -> None:
        if not isinstance(event, SignalEvent):
            return

        # Record before evaluation so the throttle window sees this signal.
        self._state.record_signal(event.strategy_id)

        # Kill switch short-circuits everything.
        if self._kill_switch.engaged:
            ks = self._kill_switch.state
            await self._publish_decision(
                event,
                approved=False,
                severity=Severity.KILL,
                rule_name=ks.triggered_by,
                reason=f"kill switch engaged: {ks.reason}",
            )
            return

        # Combine global + per-strategy rules. Global ones run first so
        # cheap structural checks (allow-list) come before stateful ones.
        rules: list[AbstractRiskRule] = [
            *self._global_rules,
            *self._rules.get(event.strategy_id, ()),
        ]

        results: list[RuleResult] = []
        for rule in rules:
            try:
                result = rule.evaluate(event, self._state)
            except Exception:
                _log.exception(
                    "rule %s raised; treating as reject", rule.name,
                    extra={"strategy_id": event.strategy_id},
                )
                # A buggy rule fails closed.
                result = RuleResult.reject(
                    rule.name,
                    reason="rule raised exception; failing closed",
                )
            results.append(result)
            # Publish WARN+ as alerts regardless of overall decision.
            if result.severity in (Severity.WARN, Severity.BLOCK, Severity.KILL):
                await self._publish_alert(rule.name, result)

        # Find the first non-approving result (or None).
        first_reject = next((r for r in results if not r.approved), None)

        if first_reject is not None:
            if first_reject.severity is Severity.KILL:
                await self._engage_kill_switch(
                    triggered_by=first_reject.rule_name,
                    reason=first_reject.reason,
                )
            await self._publish_decision(
                event,
                approved=False,
                severity=first_reject.severity,
                rule_name=first_reject.rule_name,
                reason=first_reject.reason,
            )
            return

        # All rules approved. Apply the tightest clamp.
        approved_qty = event.target_quantity
        clamping_rule: str | None = None
        for r in results:
            if r.approved_quantity is not None and r.approved_quantity < approved_qty:
                approved_qty = Quantity(r.approved_quantity)
                clamping_rule = r.rule_name

        await self._publish_decision(
            event,
            approved=True,
            severity=Severity.INFO,
            rule_name=clamping_rule,
            reason=(
                f"clamped by {clamping_rule}" if clamping_rule else ""
            ),
            approved_quantity=approved_qty,
        )

    async def _on_fill(self, event: BaseEvent) -> None:
        if isinstance(event, FillEvent):
            self._state.apply_fill(event)

    async def _on_position_update(self, event: BaseEvent) -> None:
        if isinstance(event, PositionUpdateEvent):
            self._state.apply_position_update(event)

    # --- Helpers ----------------------------------------------------------

    async def _safe_publish(self, topic: str, event: BaseEvent) -> bool:
        """Publish to the bus; absorb BackpressureError and return False if dropped."""
        try:
            await self._bus.publish(topic, event)
            return True
        except BackpressureError as exc:
            self._dropped_events += 1
            _log.critical(
                "bus backpressure; risk event dropped [total_drops=%d] "
                "topic=%r event_type=%r: %s",
                self._dropped_events, topic, type(event).__name__, exc,
            )
            return False

    async def _engage_kill_switch(self, *, triggered_by: str, reason: str) -> None:
        """Engage the switch and publish a KillSwitchEvent. Idempotent."""
        was_engaged = self._kill_switch.engaged
        state = self._kill_switch.engage(triggered_by=triggered_by, reason=reason)
        if was_engaged:
            return  # Already on; first reason wins, no duplicate event.
        await self._safe_publish(
            Topic.ALERTS,
            KillSwitchEvent(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source="risk_engine",
                triggered_by=state.triggered_by,
                reason=state.reason,
            ),
        )

    async def _publish_decision(
        self,
        signal: SignalEvent,
        *,
        approved: bool,
        severity: Severity,
        rule_name: str | None,
        reason: str,
        approved_quantity: Quantity | None = None,
    ) -> None:
        await self._safe_publish(
            Topic.RISK_DECISIONS,
            RiskDecision(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source="risk_engine",
                signal_event_id=signal.event_id,
                strategy_id=signal.strategy_id,
                approved=approved,
                severity=severity,
                rule_name=rule_name,
                reason=reason,
                approved_quantity=approved_quantity,
            ),
        )

    async def _publish_alert(self, rule_name: str, result: RuleResult) -> None:
        await self._safe_publish(
            Topic.ALERTS,
            RiskAlertEvent(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source="risk_engine",
                rule_name=rule_name,
                severity=result.severity,
                message=result.reason or f"rule {rule_name} flagged",
            ),
        )


__all__ = ["RiskEngine"]
