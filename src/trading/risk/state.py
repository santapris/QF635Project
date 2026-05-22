"""Risk state.

The single read model that every rule queries. Owned by the
:class:`RiskEngine`; rules never mutate it.

State tracked:

- current position per (strategy, instrument)
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
from ..core.events import FillEvent, PositionUpdateEvent
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
        self._daily_pnl: dict[StrategyId, _StrategyDailyPnL] = {}
        # One sliding window of recent signal timestamps per strategy.
        self._recent_signals: dict[StrategyId, deque[Timestamp]] = defaultdict(deque)

    # --- Read API (used by rules) -----------------------------------------

    def get_position(
        self, strategy_id: StrategyId, instrument: Instrument
    ) -> Quantity:
        """Signed position: +long, -short, 0 flat."""
        return self._positions.get(
            (strategy_id, instrument.instrument_id), Quantity(Decimal(0))
        )

    def get_realized_pnl_today(self, strategy_id: StrategyId) -> Price:
        bucket = self._daily_pnl.get(strategy_id)
        return bucket.realized_pnl if bucket else Decimal(0)

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

    def start_new_session(self) -> None:
        """Reset day-bucketed counters. Called by the engine at session rollover."""
        now_ns = self._clock.now_ns()
        for bucket in self._daily_pnl.values():
            bucket.realized_pnl = Decimal(0)
            bucket.day_start_ns = now_ns


__all__ = ["RiskState"]
