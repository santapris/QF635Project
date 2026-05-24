"""Reference Binance normalizer.

Demonstrates the pattern for a stateless normalizer. Handles two of the
most common Binance Spot WebSocket streams:

- ``aggTrade``: aggregated trade prints. Yields :class:`TradeEvent`.
- ``bookTicker``: best bid/ask updates. Yields :class:`TickEvent`.

The depth stream (``depthUpdate``) is intentionally not handled here.
Correctly maintaining a Binance L2 book requires interleaving a REST
snapshot with the WebSocket delta stream and dropping deltas with
``u <= snapshot_lastUpdateId``. That logic belongs in the engine
together with :class:`L2OrderBook`, not in a stateless normalizer; it
will arrive in a later pass when real exchange integration is wired up.
"""

from __future__ import annotations

from decimal import Decimal

from ...core.events import BaseEvent, TickEvent, TradeEvent
from ...core.types import Side, Symbol, Timestamp
from ..base import AbstractNormalizer, InstrumentLookup, RawMessage


def _ms_to_ns(ms: int) -> Timestamp:
    return Timestamp(ms * 1_000_000)


class BinanceNormalizer(AbstractNormalizer):
    """Normalizes Binance Spot WebSocket frames into canonical events."""

    def normalize(
        self,
        raw: RawMessage,
        instrument_lookup: InstrumentLookup,
    ) -> list[BaseEvent]:
        msg = raw.payload
        if not isinstance(msg, dict):
            return []

        # Some Binance streams wrap the inner message under "data" when
        # subscribed via the combined-stream endpoint. Unwrap if present.
        if "data" in msg and "stream" in msg:
            msg = msg["data"]
            if not isinstance(msg, dict):
                return []

        event_kind = msg.get("e")

        if event_kind == "aggTrade":
            return self._handle_agg_trade(msg, raw, instrument_lookup)
        if event_kind == "trade":
            return self._handle_trade(msg, raw, instrument_lookup)

        # Spot bookTicker frames lack the ``e`` field; futures bookTicker
        # frames carry ``e == "bookTicker"``. Handle both.
        if event_kind == "bookTicker" or (
            event_kind is None and {"s", "b", "B", "a", "A"}.issubset(msg)
        ):
            return self._handle_book_ticker(msg, raw, instrument_lookup)

        # Subscription confirmations, heartbeats, anything we don't
        # recognise: silently ignore. Do not raise — venues add new
        # message types at will.
        return []

    # --- Per-frame handlers ------------------------------------------------

    def _handle_agg_trade(
        self,
        msg: dict,
        raw: RawMessage,
        instrument_lookup: InstrumentLookup,
    ) -> list[BaseEvent]:
        symbol: Symbol = msg["s"]
        instrument = instrument_lookup(symbol)
        # ``m`` is true if the buyer was the maker, i.e. a sell hit the
        # bid. So aggressor side is SELL when m is True, BUY when False.
        aggressor = Side.SELL if msg.get("m") else Side.BUY
        return [
            TradeEvent(
                ts_event=_ms_to_ns(int(msg["T"])),
                ts_ingest=raw.ts_ingest,
                source=raw.source,
                instrument=instrument,
                price=Decimal(msg["p"]),
                quantity=Decimal(msg["q"]),
                aggressor_side=aggressor,
                venue_trade_id=str(msg.get("a", "")),
            )
        ]

    def _handle_trade(
        self,
        msg: dict,
        raw: RawMessage,
        instrument_lookup: InstrumentLookup,
    ) -> list[BaseEvent]:
        symbol: Symbol = msg["s"]
        instrument = instrument_lookup(symbol)
        aggressor = Side.SELL if msg.get("m") else Side.BUY
        return [
            TradeEvent(
                ts_event=_ms_to_ns(int(msg["T"])),
                ts_ingest=raw.ts_ingest,
                source=raw.source,
                instrument=instrument,
                price=Decimal(msg["p"]),
                quantity=Decimal(msg["q"]),
                aggressor_side=aggressor,
                venue_trade_id=str(msg.get("t", "")),
            )
        ]

    def _handle_book_ticker(
        self,
        msg: dict,
        raw: RawMessage,
        instrument_lookup: InstrumentLookup,
    ) -> list[BaseEvent]:
        symbol: Symbol = msg["s"]
        instrument = instrument_lookup(symbol)
        # bookTicker frames carry no event timestamp; use ts_ingest as
        # a best-effort fallback. Real integrations should prefer the
        # exchange-provided "E" field when present.
        ts_event = _ms_to_ns(int(msg["E"])) if "E" in msg else raw.ts_ingest
        return [
            TickEvent(
                ts_event=ts_event,
                ts_ingest=raw.ts_ingest,
                source=raw.source,
                instrument=instrument,
                bid_price=Decimal(msg["b"]),
                bid_size=Decimal(msg["B"]),
                ask_price=Decimal(msg["a"]),
                ask_size=Decimal(msg["A"]),
            )
        ]


__all__ = ["BinanceNormalizer"]
