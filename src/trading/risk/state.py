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
from ..core.events import FillEvent, MicrostructureSnapshotEvent, OpenOrdersSnapshotEvent, PositionUpdateEvent
from ..core.instruments import Instrument
from ..core.types import Price, Quantity, StrategyId, Timestamp


@dataclass(slots=True)
class _StrategyDailyPnL:
    """Day-bucketed realized PnL for one strategy."""

    realized_pnl: Price = Decimal(0)
    day_start_ns: Timestamp = 0


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
        # Latest VPIN value per instrument_id (VPIN is instrument-level, not strategy-level)
        self._vpin_values: dict[str, float] = {}
        # Consecutive ticks above VPIN threshold per instrument_id and threshold_str to support multiple VPIN-based rules with different thresholds
        self._vpin_breach_ticks: dict[tuple[str, str], int] = {}

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
        return bucket.realized_pnl if bucket else Decimal(0)
    
    def get_vpin(self, strategy_id: StrategyId, instrument_id: str) -> float | None:
        """Get the latest VPIN value for the given strategy and instrument.
        Returns None if no VPIN value is available (e.g., still warming up).
        VPIN is instrument-level, so strategy_id is not used in this implementation, but included in the signature for potential future extensions where VPIN might be strategy-specific.
        """
        return self._vpin_values.get(instrument_id)
    
    def get_vpin_breach_ticks(self, strategy_id: StrategyId, instrument_id: str, threshold: float = 0.8) -> int:
        """Get the number of consecutive ticks the VPIN has been above the specified threshold for the given instrument.
        Returns 0 if no breach is currently active.
        """
        key = (instrument_id, str(threshold))
        return self._vpin_breach_ticks.get(key, 0)

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
            self._daily_pnl[update.strategy_id] = _StrategyDailyPnL(
                realized_pnl=update.realized_pnl,
                day_start_ns=self._clock.now_ns(),
            )
        else:
            bucket.realized_pnl = update.realized_pnl

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

    def apply_analytics_snapshot(self, snapshot: MicrostructureSnapshotEvent) -> None:
        """Update cached VPIN values and breach ticks from microstructure data.

        Called by engine on every microstructure snapshot, which includes VPIN data for all instruments. We update our internal state to reflect the latest VPIN values and calculate breach ticks for any thresholds we are tracking.
        Prepopulates common thresholds to avoid cold start
        
        """
        instrument_id = snapshot.instrument.instrument_id
        vpin_value = snapshot.vpin
        if vpin_value is None:
            return
        self._vpin_values[instrument_id] = vpin_value

        # Update breach ticks for all tracked thresholds for this instrument
        for threshold in (0.7, 0.8, 0.9):  # Prepopulate common thresholds; can be extended to track more
            key = (instrument_id, str(float(threshold)))
            if vpin_value >= threshold:
                self._vpin_breach_ticks[key] = self._vpin_breach_ticks.get(key, 0) + 1
            else:
                self._vpin_breach_ticks[key] = 0

    def start_new_session(self) -> None:
        """Reset day-bucketed counters. Called by the engine at session rollover."""
        now_ns = self._clock.now_ns()
        for bucket in self._daily_pnl.values():
            bucket.realized_pnl = Decimal(0)
            bucket.day_start_ns = now_ns


__all__ = ["RiskState"]
