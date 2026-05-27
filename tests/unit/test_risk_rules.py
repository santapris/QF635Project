"""Unit tests for risk rules and engine."""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.core import (
    OrderLeg,
    OrderType,
    PositionUpdateEvent,
    Severity,
    Side,
    SignalEvent,
    TimeInForce,
)
from trading.risk import RiskState
from trading.risk.rules import (
    DailyLossLimitRule,
    InstrumentAllowlistRule,
    MaxOrderSizeRule,
    MaxPositionRule,
    ThrottleRule,
)


def _leg(qty="1", side=Side.BUY) -> OrderLeg:
    return OrderLeg(
        side=side,
        quantity=Decimal(qty),
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.IOC,
    )


def _signal(clock, btc, strategy_id, qty="1", side=Side.BUY) -> SignalEvent:
    return SignalEvent(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source="test",
        strategy_id=strategy_id,
        instrument=btc,
        legs=(_leg(qty=qty, side=side),),
    )


def test_max_position_clamps_to_headroom(clock, btc, strategy_id) -> None:
    state = RiskState(clock=clock)
    state.apply_position_update(PositionUpdateEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc,
        quantity=Decimal("8"), average_entry_price=Decimal("50000"),
        realized_pnl=Decimal(0), unrealized_pnl=Decimal(0),
        mark_price=Decimal("50000"),
    ))
    rule = MaxPositionRule(max_long=Decimal("10"), max_short=Decimal("10"))
    sig = _signal(clock, btc, strategy_id, qty="5")
    result = rule.evaluate(sig, sig.legs[0], state)
    assert result.approved
    assert result.approved_quantity == Decimal("2")


def test_max_position_rejects_at_cap(clock, btc, strategy_id) -> None:
    state = RiskState(clock=clock)
    state.apply_position_update(PositionUpdateEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc,
        quantity=Decimal("10"), average_entry_price=Decimal("50000"),
        realized_pnl=Decimal(0), unrealized_pnl=Decimal(0),
        mark_price=Decimal("50000"),
    ))
    rule = MaxPositionRule(max_long=Decimal("10"), max_short=Decimal("10"))
    sig = _signal(clock, btc, strategy_id)
    result = rule.evaluate(sig, sig.legs[0], state)
    assert not result.approved


def test_max_order_size_clamps(clock, btc, strategy_id) -> None:
    state = RiskState(clock=clock)
    rule = MaxOrderSizeRule(max_quantity=Decimal("2"))
    sig = _signal(clock, btc, strategy_id, qty="5")
    result = rule.evaluate(sig, sig.legs[0], state)
    assert result.approved
    assert result.approved_quantity == Decimal("2")


def test_daily_loss_limit_triggers_kill_severity(clock, btc, strategy_id) -> None:
    state = RiskState(clock=clock)
    state.apply_position_update(PositionUpdateEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc,
        quantity=Decimal(0), average_entry_price=Decimal(0),
        realized_pnl=Decimal("-2000"), unrealized_pnl=Decimal(0),
        mark_price=Decimal("50000"),
    ))
    rule = DailyLossLimitRule(max_loss=Decimal("1000"))
    sig = _signal(clock, btc, strategy_id)
    result = rule.evaluate(sig, sig.legs[0], state)
    assert not result.approved
    assert result.severity == Severity.KILL


def test_instrument_allowlist_rejects(clock, btc, eth, strategy_id) -> None:
    state = RiskState(clock=clock)
    rule = InstrumentAllowlistRule(allowed_instrument_ids=["BINANCE:BTC-USDT"])
    sig_btc = _signal(clock, btc, strategy_id)
    assert rule.evaluate(sig_btc, sig_btc.legs[0], state).approved
    sig_eth = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=eth,
        legs=(_leg(),),
    )
    assert not rule.evaluate(sig_eth, sig_eth.legs[0], state).approved


def test_throttle_rejects_above_cap(sim_clock, btc, strategy_id) -> None:
    state = RiskState(clock=sim_clock)
    rule = ThrottleRule(max_signals=3, window_seconds=60)
    for _ in range(4):
        state.record_signal(strategy_id)
    sig = SignalEvent(
        ts_event=sim_clock.now_ns(), ts_ingest=sim_clock.now_ns(),
        source="t", strategy_id=strategy_id, instrument=btc,
        legs=(_leg(),),
    )
    assert not rule.evaluate(sig, sig.legs[0], state).approved
