"""Unit tests for risk rules and engine."""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.core import (
    OpenOrdersSnapshotEvent,
    OrderLeg,
    OrderType,
    PositionUpdateEvent,
    Severity,
    Side,
    SignalEvent,
    TimeInForce,
    WorkingExposure,
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


def _set_working(state, strategy_id, btc, *, buy="0", sell="0") -> None:
    state.apply_open_orders_snapshot(OpenOrdersSnapshotEvent(
        ts_event=0, ts_ingest=0, source="t",
        exposures=(WorkingExposure(
            strategy_id=strategy_id, instrument=btc,
            working_buy=Decimal(buy), working_sell=Decimal(sell),
            open_order_count=1,
        ),),
    ))


def _set_position(clock, state, strategy_id, btc, qty: str) -> None:
    state.apply_position_update(PositionUpdateEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc,
        quantity=Decimal(qty), average_entry_price=Decimal("50000"),
        realized_pnl=Decimal(0), unrealized_pnl=Decimal(0),
        mark_price=Decimal("50000"),
    ))


def _ladder_signal(clock, btc, strategy_id, qtys, side) -> SignalEvent:
    """A signal with multiple same-side legs (a price ladder)."""
    return SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="test",
        strategy_id=strategy_id, instrument=btc,
        legs=tuple(_leg(qty=q, side=side) for q in qtys),
    )


def test_max_position_requote_ignores_working_orders(clock, btc, strategy_id) -> None:
    """Reproduces the place/ack/cancel thrash: a short position one tick from
    the cap, re-quoting the same sell. The signal is a desired-state snapshot,
    so the resting order (working_sell) must NOT count against the cap — else
    the re-quote is rejected, the OMS cancels the resting order as 'withdrawn',
    and the cycle repeats forever. Approval here means the reconciler
    match/amends instead of cancel-replacing."""
    state = RiskState(clock=clock)
    _set_position(clock, state, strategy_id, btc, "-0.0081")
    # A sell quote is already resting; under the old (working-counting) logic
    # this was exactly what consumed the last of the cap.
    _set_working(state, strategy_id, btc, sell="0.0019")
    rule = MaxPositionRule(max_long=Decimal("0.01"), max_short=Decimal("0.01"))
    sig = _signal(clock, btc, strategy_id, qty="0.0019", side=Side.SELL)

    result = rule.evaluate(sig, sig.legs[0], state)

    assert result.approved
    assert result.approved_quantity is None  # full size, no clamp


def test_max_position_ladder_legs_share_the_cap(clock, btc, strategy_id) -> None:
    """Same-side legs *within one signal* are summed against each other, so a
    ladder is still bounded by the cap as a whole even though working orders
    are ignored."""
    state = RiskState(clock=clock)
    _set_position(clock, state, strategy_id, btc, "-0.0081")  # 0.0019 of room
    rule = MaxPositionRule(max_long=Decimal("0.01"), max_short=Decimal("0.01"))
    # Two sell legs totalling 0.0038 — more than the 0.0019 of remaining room.
    sig = _ladder_signal(clock, btc, strategy_id, ["0.0019", "0.0019"], Side.SELL)

    # First leg: sibling = 0.0019, headroom = (-0.0081 - 0.0019) + 0.01 = 0 -> reject.
    r0 = rule.evaluate(sig, sig.legs[0], state)
    assert not r0.approved
    # Symmetric for the second leg.
    r1 = rule.evaluate(sig, sig.legs[1], state)
    assert not r1.approved


def test_max_position_ladder_within_cap_approves_each_leg(clock, btc, strategy_id) -> None:
    """A ladder whose total fits the cap approves every leg at full size."""
    state = RiskState(clock=clock)
    _set_position(clock, state, strategy_id, btc, "0")  # flat: full 0.01 of room
    rule = MaxPositionRule(max_long=Decimal("0.01"), max_short=Decimal("0.01"))
    # Two sell legs totalling 0.004 — well within the 0.01 short cap.
    sig = _ladder_signal(clock, btc, strategy_id, ["0.002", "0.002"], Side.SELL)

    for leg in sig.legs:
        r = rule.evaluate(sig, leg, state)
        assert r.approved
        assert r.approved_quantity is None  # full size, no clamp


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


# --- Engine: min-notional backstop -----------------------------------------

def _btc_min_notional(min_notional: str) -> "Instrument":
    """A BTC instrument carrying a venue minimum notional (Binance -4164)."""
    from trading.core import AssetType, Instrument

    return Instrument(
        symbol="BTC-USDT", exchange="BINANCE", asset_type=AssetType.SPOT,
        base_currency="BTC", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.0001"),
        min_notional=Decimal(min_notional),
    )


async def test_engine_drops_leg_clamped_below_min_notional(clock, strategy_id) -> None:
    """A MaxPosition clamp that drives the buy leg below the venue's min
    notional must be DROPPED by the engine, not approved. Otherwise the OMS
    places a dust order the venue rejects every tick (Binance -4164), the exact
    reject loop seen in production when inventory sits just under the long cap.
    """
    from trading.core import RiskDecision
    from trading.event_bus import MemoryBus, Topic
    from trading.risk import RiskEngine

    btc = _btc_min_notional("50")  # Binance spot BTCUSDT minimum
    bus = MemoryBus()
    risk = RiskEngine(bus=bus, clock=clock)
    # max_long 8.0008 with a confirmed long of 8 leaves 0.0008 of headroom.
    risk.register_rules(
        strategy_id,
        [MaxPositionRule(max_long=Decimal("8.0008"), max_short=Decimal("10"))],
    )
    await risk.start()
    await bus.publish(Topic.POSITIONS, PositionUpdateEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc,
        quantity=Decimal("8"), average_entry_price=Decimal("61548"),
        realized_pnl=Decimal(0), unrealized_pnl=Decimal(0),
        mark_price=Decimal("61548"),
    ))

    sig = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="strat",
        strategy_id=strategy_id, instrument=btc,
        legs=(OrderLeg(
            side=Side.BUY, quantity=Decimal("0.002"),
            price=Decimal("61548"),  # 0.0008 * 61548 = ~49.24 < 50
            order_type=OrderType.POST_ONLY, time_in_force=TimeInForce.GTC,
        ),),
    )
    await bus.publish(Topic.SIGNALS, sig)

    decisions = [e for e in bus.published_on(Topic.RISK_DECISIONS)
                 if isinstance(e, RiskDecision)]
    assert len(decisions) == 1
    decision = decisions[0]
    # The only leg was dropped → whole signal rejected, no approved legs.
    assert not decision.approved
    assert not decision.approved_legs
    assert len(decision.rejected_legs) == 1
    assert decision.rejected_legs[0].rule_name == "min_notional"


async def test_engine_keeps_leg_above_min_notional(clock, strategy_id) -> None:
    """A leg whose clamped notional clears the venue minimum is approved at the
    clamped quantity — the backstop only drops sub-minimum legs."""
    from trading.core import RiskDecision
    from trading.event_bus import MemoryBus, Topic
    from trading.risk import RiskEngine

    btc = _btc_min_notional("50")
    bus = MemoryBus()
    risk = RiskEngine(bus=bus, clock=clock)
    # 0.01 of headroom -> 0.01 * 61548 = ~615 notional, well above 50.
    risk.register_rules(
        strategy_id,
        [MaxPositionRule(max_long=Decimal("8.01"), max_short=Decimal("10"))],
    )
    await risk.start()
    await bus.publish(Topic.POSITIONS, PositionUpdateEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc,
        quantity=Decimal("8"), average_entry_price=Decimal("61548"),
        realized_pnl=Decimal(0), unrealized_pnl=Decimal(0),
        mark_price=Decimal("61548"),
    ))

    sig = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="strat",
        strategy_id=strategy_id, instrument=btc,
        legs=(OrderLeg(
            side=Side.BUY, quantity=Decimal("0.5"), price=Decimal("61548"),
            order_type=OrderType.POST_ONLY, time_in_force=TimeInForce.GTC,
        ),),
    )
    await bus.publish(Topic.SIGNALS, sig)

    decisions = [e for e in bus.published_on(Topic.RISK_DECISIONS)
                 if isinstance(e, RiskDecision)]
    assert len(decisions) == 1
    assert decisions[0].approved
    assert len(decisions[0].approved_legs) == 1
    assert decisions[0].approved_legs[0].approved_quantity == Decimal("0.01")
