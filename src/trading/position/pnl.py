"""PnL aggregation.

The engine owns one :class:`AccountingBook` per (strategy, instrument).
This module supplies the small helpers that turn books into the events
the rest of the platform consumes.
"""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import NamedTuple

from ..core.clock import Clock
from ..core.events import PnLSnapshotEvent, PositionUpdateEvent
from ..core.instruments import Instrument
from ..core.types import Price, StrategyId
from .accounting import AccountingBook


def make_position_update_event(
    *,
    strategy_id: StrategyId,
    instrument: Instrument,
    book: AccountingBook,
    mark_price: Price,
    clock: Clock,
    source: str,
) -> PositionUpdateEvent:
    """Snapshot the book into a PositionUpdateEvent."""
    return PositionUpdateEvent(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source=source,
        strategy_id=strategy_id,
        instrument=instrument,
        quantity=book.quantity,
        average_entry_price=book.average_entry_price,
        realized_pnl=book.realized_pnl,
        unrealized_pnl=book.unrealized_pnl(mark_price),
        mark_price=mark_price,
    )


class PortfolioAggregate(NamedTuple):
    realized_pnl: Price
    unrealized_pnl: Price
    total_pnl: Price
    gross_exposure: Price
    net_exposure: Price


def aggregate_portfolio(
    books_and_marks: Iterable[tuple[AccountingBook, Price]],
) -> PortfolioAggregate:
    """Aggregate across many books to portfolio totals.

    - ``gross_exposure`` is the sum of |quantity * mark_price| — total
      capital at risk to price moves in either direction.
    - ``net_exposure`` is the sum of ``quantity * mark_price`` (signed)
      — directional bias.
    """
    realized = Decimal(0)
    unrealized = Decimal(0)
    gross = Decimal(0)
    net = Decimal(0)
    for book, mark in books_and_marks:
        realized += book.realized_pnl
        unrealized += book.unrealized_pnl(mark)
        notional = book.quantity * mark
        gross += abs(notional)
        net += notional
    return PortfolioAggregate(
        realized_pnl=Price(realized),
        unrealized_pnl=Price(unrealized),
        total_pnl=Price(realized + unrealized),
        gross_exposure=Price(gross),
        net_exposure=Price(net),
    )


def make_pnl_snapshot_event(
    *,
    aggregate: PortfolioAggregate,
    clock: Clock,
    source: str,
    strategy_id: StrategyId | None = None,
) -> PnLSnapshotEvent:
    return PnLSnapshotEvent(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source=source,
        strategy_id=strategy_id,
        realized_pnl=aggregate.realized_pnl,
        unrealized_pnl=aggregate.unrealized_pnl,
        total_pnl=aggregate.total_pnl,
        gross_exposure=aggregate.gross_exposure,
        net_exposure=aggregate.net_exposure,
    )


__all__ = [
    "PortfolioAggregate",
    "aggregate_portfolio",
    "make_pnl_snapshot_event",
    "make_position_update_event",
]
