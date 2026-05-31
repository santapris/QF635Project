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
    PositionUpdateEvent,
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
    # Snapshot also carries per-order detail for the dashboard.
    assert len(last.orders) == 1
    detail = last.orders[0]
    assert detail.side is Side.BUY
    assert detail.leaves_quantity == Decimal("0.1")
    assert detail.instrument.symbol == btc.symbol


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


def test_max_position_ignores_working_orders_snapshot_semantics(
    clock, btc, strategy_id
) -> None:
    """A signal is the strategy's full desired resting state, so MaxPosition
    does NOT count working orders against the cap — the OMS reconciler drives
    resting orders to the signal's legs (match-or-amend-or-cancel). Counting
    working caused a place/ack/cancel thrash: a re-quote was rejected because
    its own resting order consumed the cap, the OMS cancelled it as withdrawn,
    and the cycle repeated. Here a 0.8 working buy must NOT clamp a fresh 0.5
    desired buy against a flat position and a 1.0 cap."""
    state = RiskState(clock=clock)
    state.apply_open_orders_snapshot(_snapshot(strategy_id, btc, buy="0.8"))
    rule = MaxPositionRule(max_long=Decimal("1.0"), max_short=Decimal("1.0"))

    sig = _signal(clock, btc, strategy_id, "0.5")
    result = rule.evaluate(sig, sig.legs[0], state)
    assert result.approved
    assert result.approved_quantity is None  # full size — working ignored


def test_max_position_rejects_on_confirmed_position_not_working(
    clock, btc, strategy_id
) -> None:
    """The cap is enforced against *confirmed* position (plus same-signal legs),
    not working orders. A confirmed long at the cap rejects further buys; a
    working order alone does not."""
    state = RiskState(clock=clock)
    rule = MaxPositionRule(max_long=Decimal("1.0"), max_short=Decimal("1.0"))

    # Working alone does not reject (it's the desired state being reconciled).
    state.apply_open_orders_snapshot(_snapshot(strategy_id, btc, buy="1.0"))
    sig = _signal(clock, btc, strategy_id, "0.5")
    assert rule.evaluate(sig, sig.legs[0], state).approved

    # A confirmed fill to the cap does reject.
    state.apply_position_update(PositionUpdateEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc, quantity=Decimal("1.0"),
        average_entry_price=Decimal("50000"), realized_pnl=Decimal(0),
        unrealized_pnl=Decimal(0), mark_price=Decimal("50000"),
    ))
    assert not rule.evaluate(sig, sig.legs[0], state).approved


async def test_independent_same_side_signals_do_not_double_place_at_oms(
    clock, btc, strategy_id
) -> None:
    """The double-approve hole is closed structurally by the OMS reconciler,
    not by counting working orders in risk. Two independent single-leg buy
    signals collapse to ONE resting order: the second signal's reconcile sees
    the first's in-flight (PENDING_NEW) order as a same-side match and leaves
    it alone rather than placing a second. (A strategy that genuinely wants two
    resting orders sends two legs in one signal — a ladder — which MaxPosition
    bounds by summing siblings.)"""
    from trading.core.events import OrderRequest
    from trading.event_bus.base import Topic
    from trading.oms import OMSEngine

    published: list = []

    class _Bus:
        async def publish(self, topic, event):
            published.append((topic, event))
        async def subscribe(self, *a): pass
        async def subscribe_many(self, *a): pass
        async def start(self): pass
        async def stop(self): pass

    oms = OMSEngine(bus=_Bus(), clock=clock)
    leg = OrderLeg(side=Side.BUY, quantity=Decimal("1.0"), price=Decimal("50000"),
                   order_type=OrderType.LIMIT, time_in_force=TimeInForce.GTC)
    sig = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=strategy_id, instrument=btc, legs=(leg,),
    )
    # Two identical signals back-to-back, before any ack lands.
    await oms._reconcile_quotes(sig)
    await oms._reconcile_quotes(sig)

    placed = sum(
        1 for t, e in published
        if t == Topic.ORDERS and isinstance(e, OrderRequest)
    )
    assert placed == 1  # second signal absorbed by same-side match, not placed
