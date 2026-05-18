from __future__ import annotations

from uuid import UUID

import pytest

from trading.core.events import (
    BaseEvent,
    OrderBookEvent,
    RiskDecision,
    SignalEvent,
    TickEvent,
    TradeEvent,
)


def test_base_event_defaults():
    evt = BaseEvent(event_type="test")
    assert isinstance(evt.event_id, UUID)
    assert evt.schema_version == 1
    assert evt.trace_id is None
    assert evt.timestamp_received is not None


def test_base_event_is_immutable():
    evt = BaseEvent(event_type="test")
    with pytest.raises(Exception):
        evt.event_type = "other"  # type: ignore[misc]


def test_tick_event_fields():
    tick = TickEvent(
        instrument_id="BTC-USDT",
        bid_price=30000.0,
        ask_price=30001.0,
        bid_size=1.5,
        ask_size=2.0,
        exchange="binance",
    )
    assert tick.event_type == "tick"
    assert tick.bid_price < tick.ask_price


def test_trade_event_side_literal():
    trade = TradeEvent(
        instrument_id="BTC-USDT",
        price=30000.0,
        quantity=0.5,
        side="buy",
        trade_id="t1",
        exchange="binance",
    )
    assert trade.side == "buy"

    with pytest.raises(Exception):
        TradeEvent(
            instrument_id="BTC-USDT",
            price=30000.0,
            quantity=0.5,
            side="invalid",  # type: ignore[arg-type]
            trade_id="t2",
            exchange="binance",
        )


def test_order_book_event_bids_asks():
    ob = OrderBookEvent(
        instrument_id="ETH-USDT",
        exchange="binance",
        bids=[(2000.0, 1.0), (1999.0, 2.0)],
        asks=[(2001.0, 1.5), (2002.0, 0.5)],
        is_snapshot=True,
    )
    assert ob.event_type == "order_book"
    assert ob.bids[0][0] > ob.bids[1][0]  # bids descending


def test_signal_event_defaults():
    sig = SignalEvent(
        strategy_id="mm-v1",
        instrument_id="BTC-USDT",
        side="buy",
        target_quantity=0.1,
    )
    assert sig.confidence == 1.0
    assert sig.target_price is None
    assert sig.rationale == ""


def test_risk_decision():
    ok = RiskDecision(passed=True)
    blocked = RiskDecision(passed=False, reason="position limit breached")
    assert ok.passed
    assert not blocked.passed
    assert "position" in blocked.reason
