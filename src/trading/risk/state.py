"""Risk state.

The single read model that every rule queries. Owned by the
:class:`RiskEngine`; rules never mutate it.

State tracked:

- current (confirmed) position per (strategy, instrument)
- working-order exposure per (strategy, instrument): leaves on open orders
  not yet filled, separated by side. Fed from the OMS's
  OpenOrdersSnapshotEvent so position-limit rules see *effective* exposure
  (confirmed + in-flight), not just confirmed fills.
- realized PnL today, per strategy (resets on session rollover)
- signal-emit timestamps per strategy (for throttle rules)

Concurrency: the engine is single-coroutine, so we don't lock. Anyone
plugging in concurrent updates from background tasks must add their own
synchronisation.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal

from ..core.clock import Clock
from ..core.events import FillEvent, OpenOrdersSnapshotEvent, PositionUpdateEvent
from ..core.instruments import Instrument
from ..core.types import Price, Quantity, StrategyId, Timestamp

_NS_PER_DAY = 86_400_000_000_000


@dataclass(slots=True)
class _StrategyDailyPnL:
    """Day-bucketed realized PnL for one strategy.

    The position engine reports *cumulative* (lifetime) realized PnL. To get
    "today's" PnL we subtract the cumulative value captured at the start of the
    current UTC day (``baseline_pnl``) from the latest cumulative value
    (``cumulative_pnl``). The day rolls over automatically when the UTC calendar
    day changes — see :meth:`RiskState._maybe_rollover` — so the daily loss cap
    resets each day without needing an external scheduler.
    """

    cumulative_pnl: Price = Decimal(0)
    baseline_pnl: Price = Decimal(0)
    day_index: int = -1


class RiskState:
    """Mutable state the engine maintains; rules see it as read-only."""

    def __init__(self, *, clock: Clock, throttle_window_seconds: float = 60.0) -> None:
        self._clock = clock
        self._throttle_window_ns = int(throttle_window_seconds * 1_000_000_000)

        self._positions: dict[tuple[StrategyId, str], Quantity] = {}
        # Working-order exposure: (working_buy, working_sell) per key. Replaced
        # wholesale on each OpenOrdersSnapshotEvent (snapshot semantics).
        self._working: dict[tuple[StrategyId, str], tuple[Quantity, Quantity]] = {}
        self._daily_pnl: dict[StrategyId, _StrategyDailyPnL] = {}
        # One sliding window of recent signal timestamps per strategy.
        self._recent_signals: dict[StrategyId, deque[Timestamp]] = defaultdict(deque)

    # --- Read API (used by rules) -----------------------------------------

    def get_position(
        self, strategy_id: StrategyId, instrument: Instrument
    ) -> Quantity:
        """Signed confirmed position: +long, -short, 0 flat.

        This is fills-only. MaxPosition checks the cap against this plus the
        *desired legs in the signal* (signals are full-state snapshots; the OMS
        reconciles resting orders to them), not against working orders — see
        :meth:`get_working` for when working exposure is the right input instead.
        """
        return self._positions.get(
            (strategy_id, instrument.instrument_id), Quantity(Decimal(0))
        )

    def get_working(
        self, strategy_id: StrategyId, instrument: Instrument
    ) -> tuple[Quantity, Quantity]:
        """Working-order exposure as ``(working_buy, working_sell)``.

        Both are non-negative sums of ``leaves_quantity`` over open orders on
        that side. Empty when the OMS has published no open orders for this
        key. Note this lags signal evaluation by one in-flight signal — see
        :meth:`apply_open_orders_snapshot`.

        Currently unused by the built-in rules: MaxPosition treats signals as
        desired-state snapshots and counts the signal's own legs, not working
        orders (counting both double-counts a re-quote against its own resting
        order). Retained for rules that must bound *incremental*-signal
        strategies, where each signal is additive rather than a full snapshot.
        """
        return self._working.get(
            (strategy_id, instrument.instrument_id),
            (Quantity(Decimal(0)), Quantity(Decimal(0))),
        )

    def get_realized_pnl_today(self, strategy_id: StrategyId) -> Price:
        bucket = self._daily_pnl.get(strategy_id)
        if bucket is None:
            return Decimal(0)
        # Roll the day forward on read too, so an idle strategy's stale loss
        # doesn't keep the kill switch armed past midnight UTC.
        self._maybe_rollover(bucket)
        return bucket.cumulative_pnl - bucket.baseline_pnl

    def signals_in_window(
        self, strategy_id: StrategyId, *, window_seconds: float | None = None
    ) -> int:
        """Count of signals from ``strategy_id`` within the throttle window.

        ``window_seconds`` overrides the default if the calling rule wants
        a tighter window. Stale entries are evicted lazily as we count.
        """
        window_ns = (
            int(window_seconds * 1_000_000_000)
            if window_seconds is not None
            else self._throttle_window_ns
        )
        now_ns = self._clock.now_ns()
        cutoff = now_ns - window_ns
        dq = self._recent_signals.get(strategy_id)
        if dq is None:
            return 0
        while dq and dq[0] < cutoff:
            dq.popleft()
        return len(dq)

    # --- Write API (used by the engine only) ------------------------------

    def record_signal(self, strategy_id: StrategyId) -> None:
        """Engine calls this for every approved-or-not signal it has seen.

        We record both approved and rejected signals because the throttle
        rule is meant to bound the *strategy's emission rate*, not its
        successful-pass rate. A strategy that spams will keep being
        throttled even when every signal is being rejected for other
        reasons.
        """
        self._recent_signals[strategy_id].append(self._clock.now_ns())

    def apply_fill(self, fill: FillEvent) -> None:
        """Update position from a fill. PnL is updated by apply_position_update."""
        key = (fill.strategy_id, fill.instrument.instrument_id)
        current = self._positions.get(key, Quantity(Decimal(0)))
        delta = fill.fill_quantity * Decimal(fill.side.sign)
        self._positions[key] = Quantity(current + delta)

    def apply_position_update(self, update: PositionUpdateEvent) -> None:
        """Update position + realized PnL from the position engine's snapshot.

        The position engine is the source of truth for realized PnL. The
        risk module trusts it.
        """
        key = (update.strategy_id, update.instrument.instrument_id)
        self._positions[key] = update.quantity

        bucket = self._daily_pnl.get(update.strategy_id)
        if bucket is None:
            # First sight of this strategy: treat today's PnL as the full
            # cumulative value (baseline 0), matching a flat day start.
            self._daily_pnl[update.strategy_id] = _StrategyDailyPnL(
                cumulative_pnl=update.realized_pnl,
                baseline_pnl=Decimal(0),
                day_index=self._day_index(),
            )
        else:
            # Roll over *before* recording the new value so the baseline
            # captures yesterday's ending cumulative PnL, not today's.
            self._maybe_rollover(bucket)
            bucket.cumulative_pnl = update.realized_pnl

    def apply_open_orders_snapshot(self, snapshot: OpenOrdersSnapshotEvent) -> None:
        """Replace working-order exposure from the OMS's snapshot.

        Snapshot semantics: the new map fully replaces the old, so a key
        absent from ``exposures`` correctly drops to zero working orders.

        Lag note (option a): this snapshot arrives on a separate topic from
        SIGNALS, so between the OMS placing an order and this update landing,
        risk may evaluate one more signal against slightly-stale working
        state. ``max_position`` is a backstop and tolerates one in-flight
        signal of lag. TODO(risk-b): tighten by having the engine
        optimistically self-increment working exposure on its own approval
        before the OMS confirms — at the cost of re-coupling risk to order
        placement. Revisit only if the one-signal lag proves exploitable.
        """
        self._working = {
            (e.strategy_id, e.instrument.instrument_id): (e.working_buy, e.working_sell)
            for e in snapshot.exposures
        }

    def start_new_session(self) -> None:
        """Force a day-bucket rollover for every strategy.

        Rollover is automatic on UTC-day change (see :meth:`_maybe_rollover`),
        so this is only needed to *manually* reset the daily loss cap mid-day —
        e.g. an operator clearing the kill switch and starting a fresh session.
        """
        today_idx = self._day_index()
        for bucket in self._daily_pnl.values():
            bucket.baseline_pnl = bucket.cumulative_pnl
            bucket.day_index = today_idx

    # --- Day-rollover helpers ---------------------------------------------

    def _day_index(self) -> int:
        """UTC day number (days since the epoch) for the current clock time."""
        return self._clock.now_ns() // _NS_PER_DAY

    def _maybe_rollover(self, bucket: _StrategyDailyPnL) -> None:
        """Reset the daily baseline if the UTC calendar day has advanced."""
        today_idx = self._day_index()
        if bucket.day_index != today_idx:
            bucket.baseline_pnl = bucket.cumulative_pnl
            bucket.day_index = today_idx


__all__ = ["RiskState"]
