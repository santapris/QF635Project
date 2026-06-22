"""Risk engine.

Subscribes to:

- ``signals``  → evaluates rules, publishes RiskDecision
- ``fills``    → updates positions in state
- ``positions``→ updates positions + realized PnL in state

Publishes to:

- ``risk-decisions`` for every signal it sees (approval or rejection)
- ``alerts`` whenever a rule emits ``WARN`` or above
- ``alerts`` (KillSwitchEvent) when the kill switch trips

Composition of rule results (per signal):

1. Kill switch latched → reject immediately (no rules run).
2. Evaluate each leg independently against the rule chain.
   - A rule's ``approved=False`` rejects that leg.
   - A rule's ``approved_quantity`` clamps the leg size.
   - A ``KILL``-severity rejection engages the kill switch and stops
     evaluating remaining legs in this signal.
3. Assemble the verdict based on ``signal.atomic``:
   - ``atomic=False``: surviving legs are approved; rejected legs are
     reported in ``rejected_legs`` for audit. If no leg survives, the
     whole signal is rejected.
   - ``atomic=True``: any single leg rejection rejects the whole signal.
     No partial placement.

The engine processes signals serially per coroutine — concurrent
signal handling would race on state updates. The bus's per-subscriber
queue gives us that serialisation for free.

Signal-as-snapshot (see :class:`SignalEvent`): a signal is a strategy's full
desired resting state, not an increment. Rules see the whole ``SignalEvent``
plus the leg under evaluation, so a rule that bounds *aggregate* exposure (e.g.
MaxPosition) reasons about confirmed position plus the same-side legs in this
signal — it does not add already-working orders, because those are the prior
snapshot this signal supersedes and the OMS will reconcile them away. The
``positions`` / ``fills`` subscriptions feed confirmed position into state; the
``open_orders`` subscription feeds working exposure into state for any rule that
needs it, though the built-in MaxPosition deliberately does not.
"""

from __future__ import annotations

import structlog
from collections.abc import Iterable

from ..core.clock import Clock
from ..core.events import (
    ApprovedLeg,
    BaseEvent,
    FillEvent,
    KillSwitchEvent,
    MicrostructureSnapshotEvent,
    OpenOrdersSnapshotEvent,
    PositionUpdateEvent,
    RejectedLeg,
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

_log = structlog.get_logger(__name__)


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
        await self._bus.subscribe(Topic.OPEN_ORDERS, self._on_open_orders)
        await self._bus.subscribe(Topic.ANALYTICS, self._on_analytics)
    async def stop(self) -> None:
        self._started = False

    # --- Event handlers ---------------------------------------------------

    async def _on_signal(self, event: BaseEvent) -> None:
        if not isinstance(event, SignalEvent):
            return

        # Record before evaluation so the throttle window sees this signal.
        self._state.record_signal(event.strategy_id)

        # Kill switch short-circuits everything — reject all legs immediately.
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

        rules: list[AbstractRiskRule] = [
            *self._global_rules,
            *self._rules.get(event.strategy_id, ()),
        ]

        # Evaluate each leg independently. The atomic flag governs how
        # partial-rejection is resolved at the end.
        approved: list[ApprovedLeg] = []
        rejected: list[RejectedLeg] = []
        worst_severity = Severity.INFO
        kill_triggered = False

        for leg in event.legs:
            # If a prior leg tripped the kill switch, every remaining leg
            # is implicitly rejected — don't keep evaluating.
            if kill_triggered:
                rejected.append(RejectedLeg(
                    leg_id=leg.leg_id,
                    side=leg.side,
                    rule_name="kill_switch",
                    reason="kill switch engaged earlier in this signal",
                    severity=Severity.KILL,
                ))
                continue

            results: list[RuleResult] = []
            for rule in rules:
                try:
                    result = rule.evaluate(event, leg, self._state)
                except Exception:
                    _log.exception(
                        "rule_raised_treating_as_reject", rule_name=rule.name,
                        strategy_id=event.strategy_id,
                    )
                    result = RuleResult.reject(
                        rule.name,
                        reason="rule raised exception; failing closed",
                    )
                results.append(result)
                if result.severity in (Severity.WARN, Severity.BLOCK, Severity.KILL):
                    await self._publish_alert(rule.name, result)
                    if result.severity.value > worst_severity.value:
                        worst_severity = result.severity

            first_reject = next((r for r in results if not r.approved), None)
            if first_reject is not None:
                if first_reject.severity is Severity.KILL:
                    await self._engage_kill_switch(
                        triggered_by=first_reject.rule_name,
                        reason=first_reject.reason,
                    )
                    kill_triggered = True
                _log.info(
                    "leg_rejected_by_risk",
                    strategy_id=event.strategy_id,
                    leg_id=leg.leg_id,
                    side=leg.side.value,
                    rule=first_reject.rule_name,
                    reason=first_reject.reason,
                )
                rejected.append(RejectedLeg(
                    leg_id=leg.leg_id,
                    side=leg.side,
                    rule_name=first_reject.rule_name,
                    reason=first_reject.reason,
                    severity=first_reject.severity,
                ))
                continue

            # Apply tightest clamp for this leg.
            qty = leg.quantity
            for r in results:
                if r.approved_quantity is not None and r.approved_quantity < qty:
                    qty = Quantity(r.approved_quantity)

            # Min-notional backstop. A clamp (e.g. MaxPosition trimming a buy to
            # fit remaining headroom) can drive the final size below the venue's
            # minimum notional, producing an order the exchange rejects every
            # tick (Binance -4164). Peer rules can't catch this — they see the
            # leg's *requested* quantity, not the post-clamp result — so the
            # check lives here, after the tightest clamp is known. Drop the leg
            # rather than emit a doomed order. No price reference → can't judge,
            # so let it through and rely on the venue.
            min_notional = event.instrument.min_notional
            ref_price = leg.price
            if (
                min_notional is not None
                and ref_price is not None
                and ref_price > 0
                and ref_price * qty < min_notional
            ):
                _log.info(
                    "leg_dropped_below_min_notional",
                    strategy_id=event.strategy_id,
                    leg_id=leg.leg_id,
                    side=leg.side.value,
                    clamped_qty=str(qty),
                    notional=str(ref_price * qty),
                    min_notional=str(min_notional),
                )
                rejected.append(RejectedLeg(
                    leg_id=leg.leg_id,
                    side=leg.side,
                    rule_name="min_notional",
                    reason=(
                        f"clamped notional {ref_price * qty} < venue minimum "
                        f"{min_notional} (clamped qty {qty})"
                    ),
                ))
                continue

            approved.append(ApprovedLeg(
                leg_id=leg.leg_id, side=leg.side, approved_quantity=qty,
            ))

        # Verdict assembly.
        if event.atomic and rejected:
            # Atomic signal with any rejection → reject the whole thing.
            # No legs are placed even though some passed individually.
            first = rejected[0]
            await self._publish_decision(
                event,
                approved=False,
                severity=worst_severity,
                rule_name=first.rule_name,
                reason=f"atomic signal rejected: {first.reason}",
                rejected_legs=tuple(rejected),
            )
            return

        if not approved:
            await self._publish_decision(
                event,
                approved=False,
                severity=worst_severity,
                rule_name=rejected[0].rule_name if rejected else None,
                reason="all legs rejected by risk",
                rejected_legs=tuple(rejected),
            )
            return

        await self._publish_decision(
            event,
            approved=True,
            severity=Severity.INFO,
            rule_name=None,
            reason="" if not rejected else "some legs rejected",
            approved_legs=tuple(approved),
            rejected_legs=tuple(rejected),
        )

    async def _on_fill(self, event: BaseEvent) -> None:
        if isinstance(event, FillEvent):
            self._state.apply_fill(event)

    async def _on_position_update(self, event: BaseEvent) -> None:
        if isinstance(event, PositionUpdateEvent):
            self._state.apply_position_update(event)

    async def _on_open_orders(self, event: BaseEvent) -> None:
        if isinstance(event, OpenOrdersSnapshotEvent):
            self._state.apply_open_orders_snapshot(event)

    async def _on_analytics(self, event: BaseEvent) -> None:
        if isinstance(event, MicrostructureSnapshotEvent):
            self._state.apply_analytics_snapshot(event)

    # --- Helpers ----------------------------------------------------------

    async def _safe_publish(self, topic: str, event: BaseEvent) -> bool:
        """Publish to the bus; absorb BackpressureError and return False if dropped."""
        try:
            await self._bus.publish(topic, event)
            return True
        except BackpressureError as exc:
            self._dropped_events += 1
            _log.critical(
                "bus_backpressure_risk_event_dropped",
                total_drops=self._dropped_events, topic=topic,
                event_type=type(event).__name__,
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
        approved_legs: tuple[ApprovedLeg, ...] = (),
        rejected_legs: tuple[RejectedLeg, ...] = (),
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
                approved_legs=approved_legs,
                rejected_legs=rejected_legs,
            ),
        )

    def snapshot(self) -> dict:
        """Return a point-in-time dict of operational counters."""
        ks = self._kill_switch.state if self._kill_switch.engaged else None
        return {
            "kill_switch_engaged": self._kill_switch.engaged,
            "kill_switch_reason": ks.reason if ks else None,
            "dropped_events": self._dropped_events,
        }

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
