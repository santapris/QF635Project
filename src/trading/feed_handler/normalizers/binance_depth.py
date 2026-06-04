"""Stateful normalizer for the Binance depth diff stream.

Unlike :class:`BinanceNormalizer` (which is stateless), this normalizer
owns one :class:`DepthBookManager` per instrument and maintains a
continuous L2 order book from the ``@depth@100ms`` differential stream.

On :class:`SequenceGapError` the manager's book is reset and the wire
symbol is added to ``needs_rebootstrap`` — the owning
:class:`~trading.order_gateways.binance.l2_feed.BinanceL2Feed` polls
this set and re-bootstraps asynchronously.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ...core.events import BaseEvent
from ...core.exceptions import SequenceGapError
from ...core.types import Timestamp
from ..base import AbstractNormalizer, InstrumentLookup, RawMessage

if TYPE_CHECKING:
    from ...order_gateways.binance.depth_book import DepthBookManager


class BinanceDepthNormalizer(AbstractNormalizer):
    """Stateful normalizer for Binance ``@depth@{speed}ms`` diff streams.

    Construct once per :class:`BinanceL2Feed`, injecting a map of
    ``wire_symbol → DepthBookManager``. The managers must be bootstrapped
    (REST snapshot fetched) before this normalizer can emit events — until
    then, ``depthUpdate`` messages are buffered inside the manager.
    """

    def __init__(self, managers: dict[str, DepthBookManager]) -> None:
        self._managers = managers
        # Wire symbols that need async re-bootstrap after a gap.
        # Checked and cleared by BinanceL2Feed._rebootstrap_loop.
        self.needs_rebootstrap: set[str] = set()

    def normalize(
        self,
        raw: RawMessage,
        instrument_lookup: InstrumentLookup,
    ) -> list[BaseEvent]:
        msg = raw.payload
        if not isinstance(msg, dict):
            return []

        # Combined-stream endpoint wraps inner message under "data".
        if "data" in msg and "stream" in msg:
            msg = msg["data"]
            if not isinstance(msg, dict):
                return []

        if msg.get("e") != "depthUpdate":
            return []

        wire: str = msg.get("s", "")
        mgr = self._managers.get(wire)
        if mgr is None:
            return []

        # Buffer during bootstrap — manager returns nothing until snapshot applied.
        if not mgr.is_initialized:
            mgr.apply_diff(msg)
            return []

        try:
            mgr.apply_diff(msg)
        except SequenceGapError:
            self.needs_rebootstrap.add(wire)
            return []

        # Emit top-10 levels as an OrderBookEvent.
        ts_ns = (
            Timestamp(int(msg["E"]) * 1_000_000)
            if "E" in msg
            else raw.ts_ingest
        )
        book_event = mgr.book.make_book_event(
            ts_event=ts_ns,
            ts_ingest=raw.ts_ingest,
            source=raw.source,
            depth_levels=10,
        )
        return [book_event] if book_event is not None else []


__all__ = ["BinanceDepthNormalizer"]
