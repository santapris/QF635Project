"""Execution router — maps a strategy's *intent* to execution *mechanics*.

A strategy declares an :class:`~trading.core.types.ExecutionIntent` on each
leg (PASSIVE / NORMAL / URGENT) — a stance about time and price. It never
names an algorithm. The router is the single place that turns that stance,
combined with market state and venue rules the strategy cannot see, into a
concrete :class:`~trading.oms.execution_algos.ExecutionAlgo` (to slice) or
``None`` (place once, reconcile in place).

This separation means execution policy can change without redeploying
strategies: swap the router at app-construction time and every strategy's
orders execute differently.

A :class:`RoutingDecision` is returned rather than a bare algo so the OMS
can emit an audit record (which algo, and why) — execution is otherwise
invisible from the signal alone once intent is NORMAL.

VWAP is not yet wired here: it needs live traded-volume in
:class:`RoutingContext`, which the OMS does not yet cache. See the TODO in
``DefaultExecutionRouter.route``.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from ..core.events import OrderLeg
from ..core.instruments import Instrument
from ..core.types import ExecutionIntent, Price, Timestamp
from .execution_algos import ExecutionAlgo, TWAPAlgo


@dataclass(frozen=True, slots=True)
class RoutingContext:
    """What the router needs that the strategy cannot see.

    Populated by the OMS at reconciliation time from its own clock and its
    cache of the latest mark per instrument. Grows as routing policy gets
    smarter (top-of-book depth, spread, recent traded volume for VWAP).
    """

    now_ns: Timestamp
    instrument: Instrument
    last_mark: Price | None = None


@dataclass(frozen=True, slots=True)
class RoutingDecision:
    """The router's verdict for one leg.

    ``algo is None`` means place the leg as a single order and reconcile it
    in place (the PASSIVE / small-clip path). Otherwise the OMS owns the algo
    and drives it to emit child slices. ``reason`` is for the audit event.
    """

    algo: ExecutionAlgo | None
    reason: str

    @property
    def algo_name(self) -> str:
        return type(self.algo).__name__ if self.algo is not None else "immediate"


class ExecutionRouter(Protocol):
    """Policy object: intent + market context -> execution decision."""

    def route(self, leg: OrderLeg, ctx: RoutingContext) -> RoutingDecision:
        ...


class DefaultExecutionRouter:
    """Ships sane defaults; subclass or replace for custom execution policy.

    - PASSIVE: always place-in-place (no slicing). Preserves market-making
      queue position; this is the default and reproduces prior behaviour.
    - URGENT: cross now. One clip if it fits under ``max_single_notional``,
      otherwise a fast TWAP to avoid blowing through the book in one shot.
    - NORMAL: place-in-place when small; slice via TWAP once the leg's
      notional exceeds ``slice_notional_threshold``.
    """

    def __init__(
        self,
        *,
        slice_notional_threshold: Decimal = Decimal("25000"),
        max_single_notional: Decimal = Decimal("50000"),
        twap_slices: int = 5,
        twap_seconds: float = 60.0,
        urgent_twap_seconds: float = 5.0,
    ) -> None:
        self._slice_threshold = slice_notional_threshold
        self._max_single = max_single_notional
        self._twap_slices = twap_slices
        self._twap_seconds = twap_seconds
        self._urgent_twap_seconds = urgent_twap_seconds

    def route(self, leg: OrderLeg, ctx: RoutingContext) -> RoutingDecision:
        if leg.intent is ExecutionIntent.PASSIVE:
            return RoutingDecision(algo=None, reason="passive: place in place")

        notional = self._notional(leg, ctx)

        if leg.intent is ExecutionIntent.URGENT:
            if notional is None or notional <= self._max_single:
                return RoutingDecision(
                    algo=None,
                    reason="urgent: single clip within max_single_notional",
                )
            return RoutingDecision(
                algo=self._make_twap(leg, ctx, self._urgent_twap_seconds),
                reason=(
                    f"urgent: notional {notional} > max_single {self._max_single}; "
                    f"fast TWAP over {self._urgent_twap_seconds}s"
                ),
            )

        # NORMAL
        if notional is None:
            # No price reference to judge size — place once and let other
            # controls (risk, venue) catch egregious orders.
            return RoutingDecision(
                algo=None, reason="normal: no mark/price to size against; single clip"
            )
        if notional <= self._slice_threshold:
            return RoutingDecision(
                algo=None,
                reason=f"normal: notional {notional} <= threshold {self._slice_threshold}",
            )
        # TODO(vwap): when RoutingContext carries recent traded volume, choose
        # VWAPAlgo over TWAPAlgo for NORMAL legs above the threshold so the
        # slice schedule tracks the volume profile instead of plain time.
        return RoutingDecision(
            algo=self._make_twap(leg, ctx, self._twap_seconds),
            reason=(
                f"normal: notional {notional} > threshold {self._slice_threshold}; "
                f"TWAP {self._twap_slices} slices over {self._twap_seconds}s"
            ),
        )

    # --- helpers ----------------------------------------------------------

    def _notional(self, leg: OrderLeg, ctx: RoutingContext) -> Decimal | None:
        ref = leg.price if leg.price is not None else ctx.last_mark
        if ref is None or ref <= 0:
            return None
        return Decimal(ref) * Decimal(leg.quantity)

    def _make_twap(
        self, leg: OrderLeg, ctx: RoutingContext, seconds: float
    ) -> TWAPAlgo:
        return TWAPAlgo(
            quantity=leg.quantity,
            duration_seconds=seconds,
            num_slices=self._twap_slices,
            start_ns=ctx.now_ns,
            order_type=leg.order_type,
            time_in_force=leg.time_in_force,
            price=leg.price,
        )


__all__ = [
    "DefaultExecutionRouter",
    "ExecutionRouter",
    "RoutingContext",
    "RoutingDecision",
]
