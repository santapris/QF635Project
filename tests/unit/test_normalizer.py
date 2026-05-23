"""Unit tests for BinanceNormalizer."""

from __future__ import annotations

from decimal import Decimal

from trading.core import Side, TickEvent, TradeEvent
from trading.feed_handler.base import RawMessage
from trading.feed_handler.normalizers import BinanceNormalizer

TS_INGEST = 1_700_000_000_000_000_000
TS_EVENT_MS = 1_700_000_000_000
TS_EVENT_NS = TS_EVENT_MS * 1_000_000
SOURCE = "binance-public"


def _lookup(instrument):
    return instrument


def _raw(payload: dict) -> RawMessage:
    return RawMessage(payload=payload, ts_ingest=TS_INGEST, source=SOURCE)


# --- aggTrade ---------------------------------------------------------------


def test_agg_trade_buy_aggressor(btc):
    normalizer = BinanceNormalizer()
    msg = {
        "e": "aggTrade",
        "E": TS_EVENT_MS,
        "s": "BTCUSDT",
        "p": "30000.50",
        "q": "0.25",
        "m": False,
        "a": 99887766,
        "T": TS_EVENT_MS,
    }
    events = normalizer.normalize(_raw(msg), lambda s: btc)
    assert len(events) == 1
    trade = events[0]
    assert isinstance(trade, TradeEvent)
    assert trade.instrument == btc
    assert trade.price == Decimal("30000.50")
    assert trade.quantity == Decimal("0.25")
    assert trade.aggressor_side == Side.BUY
    assert trade.venue_trade_id == "99887766"
    assert trade.source == SOURCE
    assert trade.ts_event == TS_EVENT_NS
    assert trade.ts_ingest == TS_INGEST


def test_agg_trade_sell_aggressor(btc):
    normalizer = BinanceNormalizer()
    msg = {
        "e": "aggTrade",
        "E": TS_EVENT_MS,
        "s": "BTCUSDT",
        "p": "30000.50",
        "q": "0.25",
        "m": True,
        "a": 99887766,
        "T": TS_EVENT_MS,
    }
    events = normalizer.normalize(_raw(msg), lambda s: btc)
    assert len(events) == 1
    assert events[0].aggressor_side == Side.SELL


# --- trade (non-aggregated) -------------------------------------------------


def test_trade_event(btc):
    normalizer = BinanceNormalizer()
    msg = {
        "e": "trade",
        "E": TS_EVENT_MS,
        "s": "BTCUSDT",
        "p": "30000.50",
        "q": "0.25",
        "m": False,
        "t": 12345,
        "T": TS_EVENT_MS,
    }
    events = normalizer.normalize(_raw(msg), lambda s: btc)
    assert len(events) == 1
    trade = events[0]
    assert isinstance(trade, TradeEvent)
    assert trade.venue_trade_id == "12345"


# --- bookTicker -------------------------------------------------------------


def test_book_ticker(btc):
    normalizer = BinanceNormalizer()
    msg = {
        "s": "BTCUSDT",
        "b": "50000.00",
        "B": "1.50000000",
        "a": "50002.00",
        "A": "2.00000000",
        "E": TS_EVENT_MS,
    }
    events = normalizer.normalize(_raw(msg), lambda s: btc)
    assert len(events) == 1
    tick = events[0]
    assert isinstance(tick, TickEvent)
    assert tick.instrument == btc
    assert tick.bid_price == Decimal("50000.00")
    assert tick.bid_size == Decimal("1.50000000")
    assert tick.ask_price == Decimal("50002.00")
    assert tick.ask_size == Decimal("2.00000000")
    assert tick.source == SOURCE
    assert tick.ts_event == TS_EVENT_NS


def test_book_ticker_falls_back_to_ts_ingest_when_no_E_field(btc):
    normalizer = BinanceNormalizer()
    msg = {
        "s": "BTCUSDT",
        "b": "50000.00",
        "B": "1.50000000",
        "a": "50002.00",
        "A": "2.00000000",
    }
    events = normalizer.normalize(_raw(msg), lambda s: btc)
    assert len(events) == 1
    assert events[0].ts_event == TS_INGEST


# --- Combined stream unwrapping ---------------------------------------------


def test_combined_stream_unwrap(btc):
    normalizer = BinanceNormalizer()
    msg = {
        "stream": "btcusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": TS_EVENT_MS,
            "s": "BTCUSDT",
            "p": "30000.50",
            "q": "0.25",
            "m": False,
            "a": 99887766,
            "T": TS_EVENT_MS,
        },
    }
    events = normalizer.normalize(_raw(msg), lambda s: btc)
    assert len(events) == 1
    assert isinstance(events[0], TradeEvent)


# --- Unknown events ---------------------------------------------------------


def test_unknown_event_returns_empty(btc):
    normalizer = BinanceNormalizer()
    msg = {"e": "unknownEvent", "data": "irrelevant"}
    events = normalizer.normalize(_raw(msg), lambda s: btc)
    assert events == []


def test_non_dict_payload_returns_empty(btc):
    normalizer = BinanceNormalizer()
    raw = RawMessage(payload="not a dict", ts_ingest=TS_INGEST, source=SOURCE)
    events = normalizer.normalize(raw, lambda s: btc)
    assert events == []


def test_combined_stream_non_dict_data_returns_empty(btc):
    normalizer = BinanceNormalizer()
    msg = {"stream": "btcusdt@aggTrade", "data": "not a dict"}
    events = normalizer.normalize(_raw(msg), lambda s: btc)
    assert events == []
