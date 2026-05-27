"""Unit tests for core primitives — types, clock, instruments, events."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from trading.core import (
    EventId,
    OrderBookEvent,
    OrderBookLevel,
    OrderLeg,
    OrderStatus,
    RiskDecision,
    Side,
    SimulatedClock,
    SignalEvent,
    StrategyId,
    TickEvent,
    TradeEvent,
)


# --- Side / OrderStatus -----------------------------------------------------

def test_side_sign() -> None:
    assert Side.BUY.sign == 1
    assert Side.SELL.sign == -1


def test_order_status_terminality() -> None:
    assert OrderStatus.FILLED.is_terminal
    assert OrderStatus.CANCELLED.is_terminal
    assert OrderStatus.REJECTED.is_terminal
    assert OrderStatus.EXPIRED.is_terminal
    assert not OrderStatus.PARTIALLY_FILLED.is_terminal
    assert not OrderStatus.ACKNOWLEDGED.is_terminal


# --- SimulatedClock ---------------------------------------------------------

def test_simulated_clock_advances(sim_clock: SimulatedClock) -> None:
    start = sim_clock.now_ns()
    sim_clock.advance(1_000_000_000)
    assert sim_clock.now_ns() == start + 1_000_000_000


def test_simulated_clock_rejects_backwards(sim_clock: SimulatedClock) -> None:
    sim_clock.set_time(sim_clock.now_ns() + 1_000_000_000)
    with pytest.raises(ValueError, match="backwards"):
        sim_clock.set_time(0)


def test_simulated_clock_from_datetime() -> None:
    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    clk = SimulatedClock(start=dt)
    assert clk.now().year == 2024


# --- Instrument -------------------------------------------------------------

def test_instrument_rounds_price(btc) -> None:
    assert btc.round_price(Decimal("50123.4567")) == Decimal("50123.45")


def test_instrument_rounds_quantity(btc) -> None:
    assert btc.round_quantity(Decimal("0.123456")) == Decimal("0.1234")


def test_instrument_id(btc) -> None:
    assert btc.instrument_id == "BINANCE:BTC-USDT"


# --- Events -----------------------------------------------------------------

def test_event_is_frozen(clock, btc, strategy_id) -> None:
    sig = SignalEvent(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source="test",
        strategy_id=strategy_id,
        instrument=btc,
        legs=(OrderLeg(side=Side.BUY, quantity=Decimal("1")),),
    )
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        sig.legs = ()


def test_event_decimal_round_trip(clock, btc, strategy_id) -> None:
    sig = SignalEvent(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source="test",
        strategy_id=strategy_id,
        instrument=btc,
        legs=(OrderLeg(side=Side.BUY, quantity=Decimal("0.5")),),
    )
    js = sig.model_dump_json()
    sig2 = SignalEvent.model_validate_json(js)
    assert sig2.legs[0].quantity == Decimal("0.5")
    assert isinstance(sig2.legs[0].quantity, Decimal)


def test_tick_event_mid_and_spread(clock, btc) -> None:
    tick = TickEvent(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source="md",
        instrument=btc,
        bid_price=Decimal("50000"),
        bid_size=Decimal("1"),
        ask_price=Decimal("50002"),
        ask_size=Decimal("1"),
    )
    assert tick.mid == Decimal("50001")
    assert tick.spread == Decimal("2")


def test_tick_event_bid_less_than_ask(clock, btc) -> None:
    tick = TickEvent(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source="md",
        instrument=btc,
        bid_price=Decimal("50000"),
        bid_size=Decimal("1"),
        ask_price=Decimal("50002"),
        ask_size=Decimal("1"),
    )
    assert tick.bid_price < tick.ask_price


def test_trade_event_aggressor_side(clock, btc) -> None:
    trade = TradeEvent(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source="md",
        instrument=btc,
        price=Decimal("50000"),
        quantity=Decimal("0.5"),
        aggressor_side=Side.BUY,
        venue_trade_id="t1",
    )
    assert trade.aggressor_side == Side.BUY
    assert trade.venue_trade_id == "t1"


def test_order_book_event_fields(clock, btc) -> None:
    ob = OrderBookEvent(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source="md",
        instrument=btc,
        bids=(
            OrderBookLevel(price=Decimal("50000"), quantity=Decimal("1")),
            OrderBookLevel(price=Decimal("49999"), quantity=Decimal("2")),
        ),
        asks=(
            OrderBookLevel(price=Decimal("50001"), quantity=Decimal("1.5")),
            OrderBookLevel(price=Decimal("50002"), quantity=Decimal("0.5")),
        ),
        sequence=1,
        is_snapshot=True,
    )
    assert ob.event_type == "order_book"
    assert ob.is_snapshot is True
    assert len(ob.bids) == 2
    assert len(ob.asks) == 2
    assert ob.bids[0].price > ob.bids[1].price


def test_signal_event_defaults(clock, btc, strategy_id) -> None:
    sig = SignalEvent(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source="test",
        strategy_id=strategy_id,
        instrument=btc,
        legs=(OrderLeg(side=Side.BUY, quantity=Decimal("0.1")),),
    )
    assert sig.rationale == ""
    assert sig.metadata == {}
    assert sig.atomic is False
    assert sig.legs[0].price is None


def test_risk_decision_fields(clock, btc, strategy_id) -> None:
    sig_id = EventId(uuid4())
    approved = RiskDecision(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source="risk",
        signal_event_id=sig_id,
        strategy_id=strategy_id,
        approved=True,
    )
    assert approved.approved is True
    assert approved.reason == ""

    blocked = RiskDecision(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source="risk",
        signal_event_id=sig_id,
        strategy_id=strategy_id,
        approved=False,
        reason="position limit breached",
    )
    assert not blocked.approved
    assert "position" in blocked.reason
