"""Position engine.

Subscribes to:

- ``fills``        — updates books, publishes a PositionUpdateEvent each time.
- ``market-data``  — captures latest mark price per instrument.

Publishes:

- ``positions``    — PositionUpdateEvent on every fill, and on
                     ``mark_to_market_all()`` for all open positions.
                     Also a PnLSnapshotEvent when ``publish_portfolio_snapshot()``
                     is called.

Design choices:

- One :class:`AccountingBook` per (strategy_id, instrument_id). Books
  are created lazily on first fill.
- Mark prices are captured from ``TickEvent`` (mid price). If we have
  not yet seen a tick for an instrument when a fill arrives, the mark
  falls back to the fill price — close enough at the moment of fill.
- Mark-to-market is *caller-driven*, not automatic on every tick.
  Capturing the mark is cheap; publishing PositionUpdateEvents for
  every position on every tick is not. Operators decide cadence via
  :meth:`mark_to_market_all`, typically driven by a periodic timer.
"""

from __future__ import annotations

import logging
from decimal import Decimal

from ..core.clock import Clock
from ..core.events import BaseEvent, FillEvent, TickEvent
from ..core.exceptions import BackpressureError
from ..core.instruments import Instrument
from ..core.positions import Position
from ..core.types import Price, StrategyId
from ..event_bus.base import AbstractEventBus, Topic
from .accounting import AccountingBook, AccountingMethod, make_book
from .pnl import (
    PortfolioAggregate,
    aggregate_portfolio,
    make_pnl_snapshot_event,
    make_position_update_event,
)

_log = logging.getLogger(__name__)


class PositionEngine:
    """Tracks positions and publishes PnL events."""

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        clock: Clock,
        method: AccountingMethod = AccountingMethod.WAVG,
        source: str = "position_engine",
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._method = method
        self._source = source

        # (strategy_id, instrument_id) -> book
        self._books: dict[tuple[StrategyId, str], AccountingBook] = {}
        # We also keep the Instrument object for each key so we can build
        # PositionUpdateEvents without forcing callers to pass it again.
        self._instruments: dict[tuple[StrategyId, str], Instrument] = {}
        # instrument_id -> last seen mark (mid) price
        self._marks: dict[str, Price] = {}
        self._started = False
        self._dropped_events: int = 0

    # --- Lifecycle --------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self._bus.subscribe(Topic.FILLS, self._on_fill)
        await self._bus.subscribe(Topic.MARKET_DATA, self._on_market_data)

    async def stop(self) -> None:
        self._started = False

    # --- Event handlers ---------------------------------------------------

    async def _on_fill(self, event: BaseEvent) -> None:
        if not isinstance(event, FillEvent):
            return
        key = (event.strategy_id, event.instrument.instrument_id)
        book = self._books.get(key)
        if book is None:
            book = make_book(self._method)
            self._books[key] = book
            self._instruments[key] = event.instrument

        try:
            book.apply_fill(event)
        except Exception:
            _log.exception(
                "book.apply_fill raised; book state may be inconsistent",
                extra={
                    "strategy_id": event.strategy_id,
                    "instrument": event.instrument.instrument_id,
                },
            )
            return

        # Use latest mark if known; otherwise fill price as a fallback so
        # the resulting event still carries a sensible unrealized number.
        mark = self._marks.get(event.instrument.instrument_id, event.fill_price)
        await self._publish_position_update(
            event.strategy_id, event.instrument, book, mark
        )

    async def _on_market_data(self, event: BaseEvent) -> None:
        # We only update the mark on TickEvent (top-of-book mid). Trade
        # events would also work but are noisier; books typically only
        # use ticks for marks anyway.
        if isinstance(event, TickEvent):
            self._marks[event.instrument.instrument_id] = event.mid

    # --- Caller-driven snapshots -----------------------------------------

    async def mark_to_market_all(self) -> None:
        """Publish a PositionUpdateEvent for every non-flat position.

        Typically called by a periodic timer (every few seconds) to
        refresh unrealized PnL for dashboards and the risk engine. Books
        with zero quantity are skipped — there's nothing to mark.
        """
        for key, book in self._books.items():
            if book.quantity == 0:
                continue
            instrument = self._instruments[key]
            mark = self._marks.get(instrument.instrument_id)
            if mark is None:
                # Never seen a tick for this instrument. Skip rather than
                # publish a misleading number.
                continue
            await self._publish_position_update(key[0], instrument, book, mark)

    async def publish_portfolio_snapshot(
        self, *, strategy_id: StrategyId | None = None
    ) -> None:
        """Publish a PnLSnapshotEvent aggregating across positions.

        ``strategy_id=None`` (default) aggregates every position the
        engine knows about. Pass a specific id to scope the snapshot.
        """
        keys = (
            [k for k in self._books if strategy_id is None or k[0] == strategy_id]
        )
        items = []
        for key in keys:
            book = self._books[key]
            instrument = self._instruments[key]
            mark = self._marks.get(instrument.instrument_id)
            if mark is None:
                # No mark yet — treat as flat for aggregation purposes.
                # Realized PnL is still meaningful, so use 0 for the
                # mark which yields zero unrealized for this leg.
                mark = Price(Decimal(0))
            items.append((book, mark))
        aggregate = aggregate_portfolio(items)
        await self._safe_publish(
            Topic.POSITIONS,
            make_pnl_snapshot_event(
                aggregate=aggregate,
                clock=self._clock,
                source=self._source,
                strategy_id=strategy_id,
            ),
        )

    # --- Read API (used by PortfolioView adapter, dashboards) ------------

    def get_all_books(self) -> dict[tuple[StrategyId, str], AccountingBook]:
        """Shallow copy of all position books, keyed by (strategy_id, instrument_id)."""
        return dict(self._books)

    def get_book(
        self, strategy_id: StrategyId, instrument: Instrument
    ) -> AccountingBook | None:
        return self._books.get((strategy_id, instrument.instrument_id))

    def get_position(
        self, strategy_id: StrategyId, instrument: Instrument
    ) -> Position | None:
        book = self._books.get((strategy_id, instrument.instrument_id))
        if book is None or book.quantity == 0:
            return None
        mark = self._marks.get(instrument.instrument_id, book.average_entry_price)
        return Position(
            strategy_id=strategy_id,
            instrument=instrument,
            quantity=book.quantity,
            average_entry_price=book.average_entry_price,
            realized_pnl=book.realized_pnl,
            unrealized_pnl=book.unrealized_pnl(mark),
            mark_price=mark,
        )

    def get_positions(self, strategy_id: StrategyId) -> dict[Instrument, Position]:
        out: dict[Instrument, Position] = {}
        for (sid, _iid), book in self._books.items():
            if sid != strategy_id or book.quantity == 0:
                continue
            instrument = self._instruments[(sid, _iid)]
            position = self.get_position(sid, instrument)
            if position is not None:
                out[instrument] = position
        return out

    def get_portfolio_aggregate(
        self, *, strategy_id: StrategyId | None = None
    ) -> PortfolioAggregate:
        items = []
        for key, book in self._books.items():
            if strategy_id is not None and key[0] != strategy_id:
                continue
            instrument = self._instruments[key]
            mark = self._marks.get(instrument.instrument_id, Price(Decimal(0)))
            items.append((book, mark))
        return aggregate_portfolio(items)

    # --- Helpers ---------------------------------------------------------

    async def _safe_publish(self, topic: str, event: BaseEvent) -> bool:
        """Publish to the bus; absorb BackpressureError and return False if dropped."""
        try:
            await self._bus.publish(topic, event)
            return True
        except BackpressureError as exc:
            self._dropped_events += 1
            _log.error(
                "bus backpressure; position event dropped [total_drops=%d] "
                "topic=%r event_type=%r: %s",
                self._dropped_events, topic, type(event).__name__, exc,
            )
            return False

    async def _publish_position_update(
        self,
        strategy_id: StrategyId,
        instrument: Instrument,
        book: AccountingBook,
        mark: Price,
    ) -> None:
        await self._safe_publish(
            Topic.POSITIONS,
            make_position_update_event(
                strategy_id=strategy_id,
                instrument=instrument,
                book=book,
                mark_price=mark,
                clock=self._clock,
                source=self._source,
            ),
        )


__all__ = ["PositionEngine"]
