"""Backtest engine.

The replay loop:

1. Pull next event from the data source.
2. Advance the :class:`SimulatedClock` to ``event.ts_event``.
3. Drain any gateway-scheduled events whose due time is now <= clock.
4. Publish the data event onto the bus.
5. Run all bus subscribers to quiescence (let chains of events settle).
6. Drain any *new* gateway-scheduled events that fired as a result.
7. Sample equity if it's a snapshot interval.
8. Loop.

When the data source is exhausted, drain any remaining scheduled
events (advancing the clock as needed) so cancels and final fills
complete cleanly.

Concurrency: this engine drives one process. The bus is
:class:`AsyncioBus` with bounded queues; we use a short cooperative
``await asyncio.sleep(0)`` cycle after each publish to let consumer
tasks drain. For determinism, the engine never uses real wall-clock
time — only ``SimulatedClock``.
"""

from __future__ import annotations

import asyncio
import structlog
from dataclasses import dataclass
from typing import Final

from ..core.clock import SimulatedClock
from ..core.events import BaseEvent, FillEvent, PnLSnapshotEvent
from ..event_bus.asyncio_bus import AsyncioBus
from ..event_bus.base import AbstractEventBus, Topic
from ..position.engine import PositionEngine
from .data_source import DataSource
from .gateway import BacktestGateway
from .report import BacktestReport

_log = structlog.get_logger(__name__)

_NS_PER_SECOND: Final[int] = 1_000_000_000


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    snapshot_interval_seconds: float = 60.0
    """How often to take a PnL snapshot. Smaller = finer equity curve, slower."""

    settle_iterations: int = 4
    """How many ``await asyncio.sleep(0)`` cycles to run after each publish.
    Higher = more chains-of-events processed per step. 4 is typically enough
    for a 5-deep pipeline (data -> strategy -> risk -> oms -> gateway)."""

    initial_equity: float = 0.0
    """Starting equity for total_return/drawdown calculations."""

    periods_per_year: float = 252
    """For annualised metrics. 252 = daily; 1440*252 = 1-minute; etc."""


class BacktestEngine:
    """Drives the replay loop. Owns the simulated clock."""

    def __init__(
        self,
        *,
        clock: SimulatedClock,
        bus: AsyncioBus,
        data_source: DataSource,
        gateway: BacktestGateway,
        position_engine: PositionEngine,
        config: BacktestConfig | None = None,
    ) -> None:
        self._clock = clock
        self._bus = bus
        self._data_source = data_source
        self._gateway = gateway
        self._position_engine = position_engine
        self._cfg = config or BacktestConfig()

        self._report = BacktestReport()
        self._last_snapshot_ns: int = 0
        self._snapshot_interval_ns: int = int(
            self._cfg.snapshot_interval_seconds * _NS_PER_SECOND
        )

    @property
    def report(self) -> BacktestReport:
        return self._report

    # --- Top-level run ---------------------------------------------------

    async def run(self) -> BacktestReport:
        """Execute the backtest end-to-end. Returns the finalized report."""
        # Recording hooks: subscribe before anyone publishes.
        await self._bus.subscribe(Topic.FILLS, self._record_fill)
        await self._bus.subscribe(Topic.POSITIONS, self._record_pnl)

        await self._bus.start()

        try:
            await self._replay_loop()
            # After data is exhausted, drain any pending scheduled
            # gateway events (final fills, late cancels, etc.).
            await self._drain_remaining()
            # Final mark-to-market for any still-open positions.
            await self._position_engine.mark_to_market_all()
            await self._settle()
            # Final portfolio snapshot.
            await self._position_engine.publish_portfolio_snapshot()
            await self._settle()
        finally:
            await self._bus.stop()

        self._report.finalize(
            initial_equity=self._cfg.initial_equity,
            periods_per_year=self._cfg.periods_per_year,
        )
        return self._report

    # --- Inner loop ------------------------------------------------------

    async def _replay_loop(self) -> None:
        async for event in self._data_source:
            # Step 1-2: advance clock to event time.
            # The data source can be slightly out of order vs scheduled
            # gateway events; we drain the gateway's heap up to (but not
            # past) the new event's timestamp first.
            await self._drain_up_to(event.ts_event)
            self._clock.set_time(event.ts_event)

            # Step 3-4: publish the data event to the right topic.
            topic = _topic_for(event)
            if topic is not None:
                await self._bus.publish(topic, event)
                await self._settle()

            # Step 5: drain any gateway events that fired as a result.
            while await self._gateway.drain_due() > 0:
                await self._settle()

            # Step 6: equity snapshot if due.
            await self._maybe_snapshot()

    async def _drain_up_to(self, target_ns: int) -> None:
        """Drain gateway events whose due time falls between now and ``target_ns``."""
        while True:
            due = self._gateway.next_due_ns()
            if due is None or due > target_ns:
                return
            self._clock.set_time(due)
            await self._gateway.drain_due()
            await self._settle()

    async def _drain_remaining(self) -> None:
        """At end-of-data, fast-forward through any tail-end gateway events."""
        while True:
            due = self._gateway.next_due_ns()
            if due is None:
                return
            if due > self._clock.now_ns():
                self._clock.set_time(due)
            await self._gateway.drain_due()
            await self._settle()

    async def _maybe_snapshot(self) -> None:
        now = self._clock.now_ns()
        if now - self._last_snapshot_ns < self._snapshot_interval_ns:
            return
        self._last_snapshot_ns = now
        await self._position_engine.mark_to_market_all()
        await self._settle()
        await self._position_engine.publish_portfolio_snapshot()
        await self._settle()

    async def _settle(self) -> None:
        """Yield enough times for the bus's per-subscriber tasks to drain."""
        for _ in range(self._cfg.settle_iterations):
            await asyncio.sleep(0)

    # --- Recorders -------------------------------------------------------

    async def _record_fill(self, event: BaseEvent) -> None:
        if isinstance(event, FillEvent):
            self._report.record_fill(event)

    async def _record_pnl(self, event: BaseEvent) -> None:
        if isinstance(event, PnLSnapshotEvent):
            self._report.record_pnl_snapshot(event)


def _topic_for(event: BaseEvent) -> str | None:
    """Map a data-source event to its destination topic.

    Returns ``None`` for event types the data source might emit that
    shouldn't be republished (none currently, but defensive).
    """
    # Lazy import avoids a top-level cycle.
    from ..core.events import (
        FundingRateEvent,
        OrderBookEvent,
        TickEvent,
        TradeEvent,
    )
    if isinstance(event, (TickEvent, TradeEvent, OrderBookEvent, FundingRateEvent)):
        return Topic.MARKET_DATA
    return None


__all__ = ["BacktestConfig", "BacktestEngine"]
