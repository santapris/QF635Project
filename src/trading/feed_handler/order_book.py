"""L2 (price-level) order book reconstruction.

Most spot and perp venues stream order book updates as a snapshot
followed by a stream of deltas. This module owns that state machine.

Design choices:

- Bids and asks are kept as ``dict[Price, Quantity]``. Removal is
  signalled by an incoming quantity of zero.
- Top-of-book is computed lazily via ``min(asks)`` / ``max(bids)``. For
  books with up to a few thousand levels this is fast enough and keeps
  the code dependency-free. If profiling shows it's a hot spot, swap in
  a ``sortedcontainers.SortedDict`` without changing the public API.
- The book carries a sequence number; deltas with non-monotonic
  sequences raise :class:`SequenceGapError` so the engine can request
  a fresh snapshot. We never silently drop or reorder.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..core.events import OrderBookEvent, OrderBookLevel, TickEvent
from ..core.exceptions import SequenceGapError
from ..core.instruments import Instrument
from ..core.types import Price, Quantity, Timestamp


@dataclass(frozen=True, slots=True)
class TopOfBook:
    bid_price: Price
    bid_size: Quantity
    ask_price: Price
    ask_size: Quantity

    @property
    def mid(self) -> Price:
        return (self.bid_price + self.ask_price) / 2

    @property
    def spread(self) -> Price:
        return self.ask_price - self.bid_price


class L2OrderBook:
    """A price-level order book for a single instrument."""

    __slots__ = ("instrument", "_bids", "_asks", "_sequence", "_initialized")

    def __init__(self, instrument: Instrument) -> None:
        self.instrument = instrument
        self._bids: dict[Price, Quantity] = {}
        self._asks: dict[Price, Quantity] = {}
        self._sequence: int | None = None
        self._initialized = False

    # --- State -------------------------------------------------------------

    @property
    def sequence(self) -> int | None:
        return self._sequence

    @property
    def is_initialized(self) -> bool:
        """True once a snapshot has been applied. Deltas before this raise."""
        return self._initialized

    def reset(self) -> None:
        """Clear book state. Used after a sequence gap before re-snapshot."""
        self._bids.clear()
        self._asks.clear()
        self._sequence = None
        self._initialized = False

    # --- Updates -----------------------------------------------------------

    def apply_snapshot(
        self,
        sequence: int,
        bids: list[tuple[Price, Quantity]],
        asks: list[tuple[Price, Quantity]],
    ) -> None:
        """Replace the book with a fresh snapshot. Always succeeds."""
        self._bids = {price: qty for price, qty in bids if qty > 0}
        self._asks = {price: qty for price, qty in asks if qty > 0}
        self._sequence = sequence
        self._initialized = True

    def apply_delta(
        self,
        sequence: int,
        bids: list[tuple[Price, Quantity]],
        asks: list[tuple[Price, Quantity]],
    ) -> None:
        """Apply incremental updates. Raises on gap or pre-snapshot delta."""
        if not self._initialized:
            raise SequenceGapError(
                "delta received before snapshot",
                instrument=self.instrument.instrument_id,
                sequence=sequence,
            )
        if self._sequence is not None and sequence != self._sequence + 1:
            raise SequenceGapError(
                "non-monotonic sequence",
                instrument=self.instrument.instrument_id,
                expected=self._sequence + 1,
                received=sequence,
            )

        for price, qty in bids:
            if qty == 0:
                self._bids.pop(price, None)
            else:
                self._bids[price] = qty
        for price, qty in asks:
            if qty == 0:
                self._asks.pop(price, None)
            else:
                self._asks[price] = qty

        self._sequence = sequence

    # --- Queries -----------------------------------------------------------

    def best_bid(self) -> tuple[Price, Quantity] | None:
        if not self._bids:
            return None
        price = max(self._bids)
        return price, self._bids[price]

    def best_ask(self) -> tuple[Price, Quantity] | None:
        if not self._asks:
            return None
        price = min(self._asks)
        return price, self._asks[price]

    def top_of_book(self) -> TopOfBook | None:
        bid = self.best_bid()
        ask = self.best_ask()
        if bid is None or ask is None:
            return None
        return TopOfBook(bid[0], bid[1], ask[0], ask[1])

    def top_n(
        self, n: int = 10
    ) -> tuple[list[OrderBookLevel], list[OrderBookLevel]]:
        """Top n levels each side. Bids descending, asks ascending."""
        bid_prices = sorted(self._bids, reverse=True)[:n]
        ask_prices = sorted(self._asks)[:n]
        bids = [OrderBookLevel(price=p, quantity=self._bids[p]) for p in bid_prices]
        asks = [OrderBookLevel(price=p, quantity=self._asks[p]) for p in ask_prices]
        return bids, asks

    def depth(self) -> tuple[int, int]:
        """(bid_levels, ask_levels). Useful for monitoring book health."""
        return len(self._bids), len(self._asks)

    # --- Event construction ------------------------------------------------

    def make_tick_event(
        self, ts_event: Timestamp, ts_ingest: Timestamp, source: str
    ) -> TickEvent | None:
        """Build a top-of-book TickEvent. Returns None if the book is empty."""
        tob = self.top_of_book()
        if tob is None:
            return None
        return TickEvent(
            ts_event=ts_event,
            ts_ingest=ts_ingest,
            source=source,
            instrument=self.instrument,
            bid_price=tob.bid_price,
            bid_size=tob.bid_size,
            ask_price=tob.ask_price,
            ask_size=tob.ask_size,
        )

    def make_book_event(
        self,
        ts_event: Timestamp,
        ts_ingest: Timestamp,
        source: str,
        depth_levels: int = 10,
        is_snapshot: bool = False,
    ) -> OrderBookEvent | None:
        """Build a top-N OrderBookEvent. Returns None if book is empty."""
        if not self._initialized or self._sequence is None:
            return None
        bids, asks = self.top_n(depth_levels)
        return OrderBookEvent(
            ts_event=ts_event,
            ts_ingest=ts_ingest,
            source=source,
            instrument=self.instrument,
            bids=tuple(bids),
            asks=tuple(asks),
            sequence=self._sequence,
            is_snapshot=is_snapshot,
        )


__all__ = ["L2OrderBook", "TopOfBook"]
