from __future__ import annotations

import pytest

from trading.feed_handler.normalizer import normalize_agg_trade, normalize_depth5


AGG_TRADE_MSG = {
    "e": "aggTrade",
    "E": 1700000000000,
    "s": "BTCUSDT",
    "p": "30000.50",
    "q": "0.25",
    "m": False,
    "a": 99887766,
}

DEPTH5_MSG = {
    "s": "ETHUSDT",
    "b": [["2000.10", "1.5"], ["1999.00", "3.0"]],
    "a": [["2001.00", "0.8"], ["2002.50", "2.0"]],
}


def test_normalize_agg_trade_fields():
    trade = normalize_agg_trade(AGG_TRADE_MSG)
    assert trade.instrument_id == "BTCUSDT"
    assert trade.price == pytest.approx(30000.50)
    assert trade.quantity == pytest.approx(0.25)
    assert trade.side == "buy"  # m=False => buyer aggressive
    assert trade.trade_id == "99887766"
    assert trade.exchange == "binance"


def test_normalize_agg_trade_seller_aggressive():
    msg = {**AGG_TRADE_MSG, "m": True}
    trade = normalize_agg_trade(msg)
    assert trade.side == "sell"


def test_normalize_agg_trade_underscore_symbol():
    msg = {**AGG_TRADE_MSG, "s": "BTC_USDT"}
    trade = normalize_agg_trade(msg)
    assert trade.instrument_id == "BTC-USDT"


def test_normalize_depth5_fields():
    ob = normalize_depth5(DEPTH5_MSG)
    assert ob.instrument_id == "ETHUSDT"
    assert ob.exchange == "binance"
    assert ob.is_snapshot is True
    assert len(ob.bids) == 2
    assert len(ob.asks) == 2


def test_normalize_depth5_price_types():
    ob = normalize_depth5(DEPTH5_MSG)
    for price, qty in ob.bids + ob.asks:
        assert isinstance(price, float)
        assert isinstance(qty, float)


def test_normalize_depth5_empty_book():
    ob = normalize_depth5({"s": "BTCUSDT", "b": [], "a": []})
    assert ob.bids == []
    assert ob.asks == []


def test_normalize_depth5_underscore_symbol():
    msg = {**DEPTH5_MSG, "s": "ETH_USDT"}
    ob = normalize_depth5(msg)
    assert ob.instrument_id == "ETH-USDT"
