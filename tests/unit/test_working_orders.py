"""Working-order exposure: OMS producer, risk consumer, double-approve fix.

The core safety property: MaxPositionRule must count in-flight (unfilled)
orders against the cap, not just confirmed fills. Without it, N signals each
approved against stale confirmed-position-only state can collectively exceed
the limit once they fill.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.core import (
    OpenOrdersSnapshotEvent,
    OrderLeg,
    OrderType,
    Side,
    SignalEvent,
    TimeInForce,
    WorkingExposure,
)
from trading.core.types import OrderStatus
from trading.risk import RiskState
from trading.risk.rules import MaxPositionRule


class _NullBus:
    async def publish(self, topic, event): pass
    async def subscribe(self, topic, handler): pass
    async def subscribe_many(self, topics, handler): pass
    async def start(self): pass
    async def stop(self): pass


class _CaptureBus:
    def __init__(self):
        self.published: list[tuple[str, object]] = []
    async def publish(self, topic, event):
        self.published.append((topic, event))
    async def subscribe(self, topic, handler): pass
    async def subscribe_many(self, topics, handler): pass
    async def start(self): pass
    async def stop(self): pass


# ---------------------------------------------------------------------------
# OMS producer: working_exposures() aggregation
# ---------------------------------------------------------------------------

def _leg(side, qty, price="50000", intent=None):
    kwargs = dict(side=side, quantity=Decimal(qty), price=Decimal(price),
                  order_type=OrderType.LIMIT, time_in_force=TimeInForce.GTC)
    if intent is not None:
        kwargs["intent"] = intent
    return OrderLeg(**kwargs)


async def test_working_exposures_aggregates_by_side(clock, btc, strategy_id) -> None:
    from trading.oms import OMSEngine
    oms = OMSEngine(bus=_NullBus(), clock=clock)

    sig_buy = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc, legs=(_leg(Side.BUY, "0.3"),),
    )
    await oms._place_quote(sig_buy, sig_buy.legs[0])
    sig_buy2 = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc, legs=(_leg(Side.BUY, "0.2"),),
    )
    await oms._place_quote(sig_buy2, sig_buy2.legs[0])
    sig_sell = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc, legs=(_leg(Side.SELL, "0.1"),),
    )
    await oms._place_quote(sig_sell, sig_sell.legs[0])

    exposures = oms.working_exposures()
    assert len(exposures) == 1
    e = exposures[0]
    assert e.working_buy == Decimal("0.5")
    assert e.working_sell == Decimal("0.1")
    assert e.open_order_count == 3


async def test_terminal_orders_excluded_from_exposure(clock, btc, strategy_id) -> None:
    from trading.oms import OMSEngine
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    sig = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc, legs=(_leg(Side.BUY, "1"),),
    )
    await oms._place_quote(sig, sig.legs[0])
    (order,) = oms._orders.values()
    order.transition_to(OrderStatus.ACKNOWLEDGED, at_ns=clock.now_ns())
    order.transition_to(OrderStatus.CANCELLED, at_ns=clock.now_ns())

    assert oms.working_exposures() == ()


async def test_oms_publishes_open_orders_snapshot(clock, btc, strategy_id) -> None:
    from trading.oms import OMSEngine
    from trading.event_bus.base import Topic
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=clock)
    leg = _leg(Side.BUY, "0.1", intent=None)
    sig = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc, legs=(leg,),
    )
    await oms._reconcile_quotes(sig)

    snaps = [e for t, e in bus.published
             if t == Topic.OPEN_ORDERS and isinstance(e, OpenOrdersSnapshotEvent)]
    assert len(snaps) >= 1
    last = snaps[-1]
    assert any(x.working_buy == Decimal("0.1") for x in last.exposures)


# ---------------------------------------------------------------------------
# RiskState consumer + MaxPositionRule headroom
# ---------------------------------------------------------------------------

def _snapshot(strategy_id, btc, *, buy="0", sell="0"):
    return OpenOrdersSnapshotEvent(
        ts_event=0, ts_ingest=0, source="oms",
        exposures=(WorkingExposure(
            strategy_id=strategy_id, instrument=btc,
            working_buy=Decimal(buy), working_sell=Decimal(sell),
            open_order_count=1,
        ),),
    )


def test_risk_state_applies_and_replaces_working(clock, btc, strategy_id) -> None:
    state = RiskState(clock=clock)
    assert state.get_working(strategy_id, btc) == (Decimal(0), Decimal(0))

    state.apply_open_orders_snapshot(_snapshot(strategy_id, btc, buy="0.5"))
    assert state.get_working(strategy_id, btc) == (Decimal("0.5"), Decimal(0))

    # Snapshot semantics: empty snapshot clears prior working state.
    state.apply_open_orders_snapshot(OpenOrdersSnapshotEvent(
        ts_event=0, ts_ingest=0, source="oms", exposures=(),
    ))
    assert state.get_working(strategy_id, btc) == (Decimal(0), Decimal(0))


def _signal(clock, btc, strategy_id, qty, side=Side.BUY):
    return SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc,
        legs=(_leg(side, qty),),
    )


def test_max_position_counts_working_buy(clock, btc, strategy_id) -> None:
    """Confirmed 0, but 0.8 working buy against a cap of 1.0 leaves 0.2 headroom."""
    state = RiskState(clock=clock)
    state.apply_open_orders_snapshot(_snapshot(strategy_id, btc, buy="0.8"))
    rule = MaxPositionRule(max_long=Decimal("1.0"), max_short=Decimal("1.0"))

    sig = _signal(clock, btc, strategy_id, "0.5")
    result = rule.evaluate(sig, sig.legs[0], state)
    assert result.approved
    assert result.approved_quantity == Decimal("0.2")  # clamped to headroom


def test_max_position_rejects_when_working_fills_cap(clock, btc, strategy_id) -> None:
    state = RiskState(clock=clock)
    state.apply_open_orders_snapshot(_snapshot(strategy_id, btc, buy="1.0"))
    rule = MaxPositionRule(max_long=Decimal("1.0"), max_short=Decimal("1.0"))

    sig = _signal(clock, btc, strategy_id, "0.5")
    result = rule.evaluate(sig, sig.legs[0], state)
    assert not result.approved


def test_double_approve_bug_is_fixed(clock, btc, strategy_id) -> None:
    """The original hole: two 1.0 buys both approved against a 1.0 cap because
    risk only saw confirmed fills. With working-order tracking the second is
    rejected once the first is in flight."""
    state = RiskState(clock=clock)
    rule = MaxPositionRule(max_long=Decimal("1.0"), max_short=Decimal("1.0"))

    # First signal: nothing working yet, fully approved.
    sig1 = _signal(clock, btc, strategy_id, "1.0")
    r1 = rule.evaluate(sig1, sig1.legs[0], state)
    assert r1.approved and r1.approved_quantity is None  # full size

    # OMS places it; working exposure now reflects the resting buy.
    state.apply_open_orders_snapshot(_snapshot(strategy_id, btc, buy="1.0"))

    # Second identical signal: now rejected, not silently approved.
    sig2 = _signal(clock, btc, strategy_id, "1.0")
    r2 = rule.evaluate(sig2, sig2.legs[0], state)
    assert not r2.approved
