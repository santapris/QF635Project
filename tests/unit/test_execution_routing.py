"""Intent-based execution routing and OMS slicing.

Covers:
  - DefaultExecutionRouter maps intent -> algo-or-immediate by notional.
  - PASSIVE legs never slice (market-making behaviour preserved).
  - A NORMAL leg over threshold gets sliced: the OMS owns an algo and emits
    child orders stamped with parent_leg_id.
  - Withdrawing a sliced leg cancels the algo and its in-flight children.
  - Re-signalling the same leg_id resumes (does not restart) the algo.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from trading.core import (
    ExecutionIntent,
    OrderLeg,
    OrderType,
    Side,
    SignalEvent,
    TimeInForce,
)
from trading.core.types import OrderStatus
from trading.oms import DefaultExecutionRouter, OMSEngine, RoutingContext


# ---------------------------------------------------------------------------
# Router unit tests (pure, no bus)
# ---------------------------------------------------------------------------

def _leg(intent, qty="1", price="50000") -> OrderLeg:
    return OrderLeg(
        side=Side.BUY,
        quantity=Decimal(qty),
        price=Decimal(price) if price is not None else None,
        order_type=OrderType.LIMIT,
        time_in_force=TimeInForce.GTC,
        intent=intent,
    )


def _ctx(clock, btc, mark="50000"):
    return RoutingContext(
        now_ns=clock.now_ns(),
        instrument=btc,
        last_mark=Decimal(mark) if mark is not None else None,
    )


def test_passive_never_slices(clock, btc) -> None:
    router = DefaultExecutionRouter()
    # Even a huge passive leg stays a single resting order.
    d = router.route(_leg(ExecutionIntent.PASSIVE, qty="100"), _ctx(clock, btc))
    assert d.algo is None
    assert d.algo_name == "immediate"


def test_normal_small_is_immediate(clock, btc) -> None:
    router = DefaultExecutionRouter(slice_notional_threshold=Decimal("25000"))
    # 0.1 * 50000 = 5000 notional, under threshold.
    d = router.route(_leg(ExecutionIntent.NORMAL, qty="0.1"), _ctx(clock, btc))
    assert d.algo is None


def test_normal_large_slices(clock, btc) -> None:
    router = DefaultExecutionRouter(slice_notional_threshold=Decimal("25000"))
    # 1 * 50000 = 50000 notional, over threshold -> TWAP.
    d = router.route(_leg(ExecutionIntent.NORMAL, qty="1"), _ctx(clock, btc))
    assert d.algo is not None
    assert d.algo_name == "TWAPAlgo"


def test_urgent_small_is_single_clip(clock, btc) -> None:
    router = DefaultExecutionRouter(max_single_notional=Decimal("50000"))
    d = router.route(_leg(ExecutionIntent.URGENT, qty="0.5"), _ctx(clock, btc))
    assert d.algo is None


def test_urgent_large_slices_fast(clock, btc) -> None:
    router = DefaultExecutionRouter(max_single_notional=Decimal("50000"))
    # 2 * 50000 = 100000 > max_single -> fast TWAP.
    d = router.route(_leg(ExecutionIntent.URGENT, qty="2"), _ctx(clock, btc))
    assert d.algo is not None
    assert d.algo_name == "TWAPAlgo"


def test_normal_no_price_reference_is_immediate(clock, btc) -> None:
    router = DefaultExecutionRouter()
    leg = _leg(ExecutionIntent.NORMAL, qty="100", price=None)
    d = router.route(leg, _ctx(clock, btc, mark=None))
    assert d.algo is None


# ---------------------------------------------------------------------------
# OMS slicing integration (capture bus, no real gateway)
# ---------------------------------------------------------------------------

class _CaptureBus:
    def __init__(self):
        self.published: list[tuple[str, object]] = []
    async def publish(self, topic, event):
        self.published.append((topic, event))
    async def subscribe(self, topic, handler): pass
    async def subscribe_many(self, topics, handler): pass
    async def start(self): pass
    async def stop(self): pass


def _signal(clock, btc, strategy_id, leg) -> SignalEvent:
    return SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="test",
        strategy_id=strategy_id, instrument=btc, legs=(leg,),
    )


async def test_passive_leg_places_directly_no_algo(clock, btc, strategy_id) -> None:
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=clock)
    leg = OrderLeg(side=Side.BUY, quantity=Decimal("0.1"), price=Decimal("50000"),
                   order_type=OrderType.LIMIT, time_in_force=TimeInForce.GTC,
                   intent=ExecutionIntent.PASSIVE)
    await oms._reconcile_quotes(_signal(clock, btc, strategy_id, leg))

    assert len(oms._algos) == 0
    # One plain order placed (plus the routing audit event).
    order_reqs = [e for t, e in bus.published if type(e).__name__ == "OrderRequest"]
    assert len(order_reqs) == 1
    (order,) = oms._orders.values()
    assert order.parent_leg_id is None


async def test_normal_large_leg_creates_algo_and_emits_child(clock, btc, strategy_id) -> None:
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=clock,
                    router=DefaultExecutionRouter(slice_notional_threshold=Decimal("25000")))
    leg = OrderLeg(side=Side.BUY, quantity=Decimal("1"), price=Decimal("50000"),
                   order_type=OrderType.LIMIT, time_in_force=TimeInForce.GTC,
                   intent=ExecutionIntent.NORMAL)
    await oms._reconcile_quotes(_signal(clock, btc, strategy_id, leg))

    # Algo registered under the leg_id.
    assert leg.leg_id in oms._algos
    # First slice kicked immediately — a child order with parent_leg_id set.
    children = [o for o in oms._orders.values() if o.parent_leg_id == leg.leg_id]
    assert len(children) == 1
    assert children[0].quantity < Decimal("1")  # a slice, not the whole clip

    # An ExecutionRoutedEvent was published for observability.
    routed = [e for t, e in bus.published if type(e).__name__ == "ExecutionRoutedEvent"]
    assert len(routed) == 1
    assert routed[0].algo == "TWAPAlgo"


async def test_withdrawing_sliced_leg_cancels_algo_and_children(clock, btc, strategy_id) -> None:
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=clock,
                    router=DefaultExecutionRouter(slice_notional_threshold=Decimal("25000")))
    leg = OrderLeg(side=Side.BUY, quantity=Decimal("1"), price=Decimal("50000"),
                   order_type=OrderType.LIMIT, time_in_force=TimeInForce.GTC,
                   intent=ExecutionIntent.NORMAL)
    await oms._reconcile_quotes(_signal(clock, btc, strategy_id, leg))
    assert leg.leg_id in oms._algos
    child = next(o for o in oms._orders.values() if o.parent_leg_id == leg.leg_id)
    # Ack it so it's cancellable.
    child.transition_to(OrderStatus.ACKNOWLEDGED, at_ns=clock.now_ns())

    # Re-signal with no legs → withdraw everything.
    empty = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="test",
        strategy_id=strategy_id, instrument=btc, legs=(),
    )
    await oms._reconcile_quotes(empty)

    assert leg.leg_id not in oms._algos
    # The child got a cancel.
    cancels = [e for t, e in bus.published if type(e).__name__ == "CancelRequest"]
    assert any(c.order_id == child.order_id for c in cancels)


async def test_resignal_same_leg_id_resumes_algo(clock, btc, strategy_id) -> None:
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=clock,
                    router=DefaultExecutionRouter(slice_notional_threshold=Decimal("25000")))
    leg = OrderLeg(side=Side.BUY, quantity=Decimal("1"), price=Decimal("50000"),
                   order_type=OrderType.LIMIT, time_in_force=TimeInForce.GTC,
                   intent=ExecutionIntent.NORMAL)
    await oms._reconcile_quotes(_signal(clock, btc, strategy_id, leg))
    algo_before = oms._algos[leg.leg_id]

    # Same leg_id re-signalled → same algo instance, not a fresh one.
    await oms._reconcile_quotes(_signal(clock, btc, strategy_id, leg))
    assert oms._algos[leg.leg_id] is algo_before
