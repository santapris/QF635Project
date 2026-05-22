"""Backtest data source.

A *data source* is anything that produces a chronologically ordered
stream of canonical events (ticks, trades, books). The replay engine
consumes from one source per backtest and pumps events onto the bus.

Two implementations land here:

- :class:`InMemoryDataSource` — for tests and quick studies. Caller
  hands in a list of pre-built events.
- :class:`CSVDataSource` — reads OHLCV bars from CSV files and
  synthesizes ticks. Production backtests would add Parquet, Arrow,
  or direct-from-exchange-archive sources following the same protocol.

The source is responsible for *ordering*. Events emerge sorted by
``ts_event``. This is critical: the replay engine advances the
:class:`SimulatedClock` to each event's timestamp before dispatching,
so out-of-order events would corrupt the clock invariant.
"""

from __future__ import annotations

import csv
from collections.abc import AsyncIterator, Iterable, Iterator
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Protocol, runtime_checkable

from ..core.events import BaseEvent, TickEvent, TradeEvent
from ..core.instruments import Instrument
from ..core.types import Timestamp


@runtime_checkable
class DataSource(Protocol):
    """A chronologically ordered stream of events."""

    def __aiter__(self) -> AsyncIterator[BaseEvent]:
        ...

    async def __anext__(self) -> BaseEvent:
        ...


class InMemoryDataSource:
    """Wrap a pre-sorted list of events."""

    def __init__(self, events: Iterable[BaseEvent]) -> None:
        self._events: list[BaseEvent] = list(events)
        # Validate ordering at construction so backtests fail loud rather
        # than producing subtly wrong results.
        for prev, curr in zip(self._events, self._events[1:]):
            if curr.ts_event < prev.ts_event:
                raise ValueError(
                    f"events not in chronological order: "
                    f"{prev.ts_event} -> {curr.ts_event}"
                )
        self._iter: Iterator[BaseEvent] = iter(self._events)

    def __aiter__(self) -> InMemoryDataSource:
        return self

    async def __anext__(self) -> BaseEvent:
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


@dataclass(frozen=True, slots=True)
class CSVColumns:
    """Column mapping for OHLCV CSV files.

    Defaults match the most common Crypto exchange archive format:
    ``timestamp,open,high,low,close,volume`` with timestamps in
    milliseconds. Override for other conventions.
    """

    timestamp: str = "timestamp"
    open: str = "open"
    high: str = "high"
    low: str = "low"
    close: str = "close"
    volume: str = "volume"
    timestamp_unit: str = "ms"  # one of: "ns", "us", "ms", "s"


_TIMESTAMP_MULTIPLIERS = {
    "ns": 1,
    "us": 1_000,
    "ms": 1_000_000,
    "s": 1_000_000_000,
}


class CSVDataSource:
    """Read OHLCV bars from CSV; synthesize one tick per bar.

    Each bar produces a single :class:`TickEvent` at the bar's close
    time, with bid=close-tick and ask=close+tick (a simple stand-in for
    book data). This is enough for strategies that key off mid-price.

    For trade-driven strategies that need trades to be visible, also
    emit a :class:`TradeEvent` per bar at the close price. Disable with
    ``emit_trades=False`` when not needed.
    """

    def __init__(
        self,
        *,
        path: str | Path,
        instrument: Instrument,
        columns: CSVColumns | None = None,
        emit_trades: bool = True,
        source: str = "csv",
    ) -> None:
        self._path = Path(path)
        self._instrument = instrument
        self._columns = columns or CSVColumns()
        self._emit_trades = emit_trades
        self._source = source
        self._buffer: list[BaseEvent] = []
        self._iter: Iterator[BaseEvent] | None = None

    def _load(self) -> Iterator[BaseEvent]:
        mult = _TIMESTAMP_MULTIPLIERS[self._columns.timestamp_unit]
        c = self._columns
        last_ts: int | None = None
        with self._path.open("r", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                raw_ts = int(float(row[c.timestamp]))
                ts_event = Timestamp(raw_ts * mult)
                if last_ts is not None and ts_event < last_ts:
                    raise ValueError(
                        f"CSV not in chronological order at row {row}: "
                        f"{last_ts} -> {ts_event}"
                    )
                last_ts = ts_event
                close = Decimal(row[c.close])
                tick = self._instrument.tick_size
                yield TickEvent(
                    ts_event=ts_event, ts_ingest=ts_event, source=self._source,
                    instrument=self._instrument,
                    bid_price=close - tick, bid_size=Decimal("1"),
                    ask_price=close + tick, ask_size=Decimal("1"),
                )
                if self._emit_trades:
                    volume = Decimal(row.get(c.volume, "0") or "0")
                    if volume > 0:
                        yield TradeEvent(
                            ts_event=ts_event, ts_ingest=ts_event,
                            source=self._source, instrument=self._instrument,
                            price=close, quantity=volume,
                        )

    def __aiter__(self) -> CSVDataSource:
        self._iter = self._load()
        return self

    async def __anext__(self) -> BaseEvent:
        assert self._iter is not None, "must enter __aiter__ first"
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


__all__ = ["CSVColumns", "CSVDataSource", "DataSource", "InMemoryDataSource"]
