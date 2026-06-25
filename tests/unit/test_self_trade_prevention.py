"""Unit tests: OMS internal netting / self-trade prevention.

The OMS holds every strategy's orders in one map, so it is the only place that
can see a would-be cross between two strategies on the same instrument. When
STP is on, a leg that would lift/hit a *sibling* strategy's resting order is
held back rather than routed to the venue — no wasted fees/spread, no wash
trade. These tests drive the reconcile path directly and assert which
OrderRequests reach the bus.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from trading.core import (
    AssetType,
    Instrument,
    LiveClock,
    OrderType,
    Side,
    StrategyId,
    TimeInForce,
)
from trading.core.events import OrderLeg, OrderRequest, SignalEvent
from trading.core.types import ClientOrderId, OrderId, OrderStatus
from trading.oms.engine import OMSEngine
from trading.oms.order import Order


@pytest.fixture
def inst() -> Instrument:
    return Instrument(
        symbol="BTC-USDT", exchange="BINANCE", asset_type=AssetType.SPOT,
        base_currency="BTC", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.00001"),
    )


class _CaptureBus:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event) -> None:
        self.published.append((topic, event))

    async def subscribe(self, topic, handler) -> None: ...
    async def subscribe_many(self, topics, handler) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


def _resting(inst: Instrument, *, strategy: str, side: Side, price: str) -> Order:
    o = Order(
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId(f"{strategy}-{uuid4().hex[:12]}"),
        strategy_id=StrategyId(strategy),
        instrument=inst,
        side=side,
        order_type=OrderType.POST_ONLY,
        quantity=Decimal("0.01"),
        price=Decimal(price),
        time_in_force=TimeInForce.GTC,
        created_at_ns=0,
        parent_leg_id=None,
    )
    o.status = OrderStatus.ACKNOWLEDGED
    return o


def _signal(inst: Instrument, *, strategy: str, side: Side, price: str) -> SignalEvent:
    return SignalEvent(
        ts_event=0, ts_ingest=0, source="test",
        strategy_id=StrategyId(strategy), instrument=inst,
        legs=(OrderLeg(
            side=side, quantity=Decimal("0.01"), price=Decimal(price),
            order_type=OrderType.POST_ONLY, time_in_force=TimeInForce.GTC,
        ),),
    )


def _order_requests(bus: _CaptureBus) -> list[OrderRequest]:
    return [e for _, e in bus.published if isinstance(e, OrderRequest)]


async def test_crossing_buy_is_suppressed(inst) -> None:
    """B's buy at a sibling's resting sell price is held back."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    a_sell = _resting(inst, strategy="A", side=Side.SELL, price="50000.50")
    oms._orders[a_sell.order_id] = a_sell

    await oms._reconcile_quotes(_signal(inst, strategy="B", side=Side.BUY, price="50000.50"))

    assert _order_requests(bus) == []
    assert oms.snapshot()["self_trade_prevented"] == 1


async def test_crossing_sell_is_suppressed(inst) -> None:
    """Symmetric: B's sell at/below a sibling's resting buy is held back."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    a_buy = _resting(inst, strategy="A", side=Side.BUY, price="50000.50")
    oms._orders[a_buy.order_id] = a_buy

    await oms._reconcile_quotes(_signal(inst, strategy="B", side=Side.SELL, price="50000.50"))

    assert _order_requests(bus) == []
    assert oms.snapshot()["self_trade_prevented"] == 1


async def test_non_crossing_leg_is_placed(inst) -> None:
    """A buy strictly below the sibling's resting sell does not cross — placed."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    a_sell = _resting(inst, strategy="A", side=Side.SELL, price="50000.50")
    oms._orders[a_sell.order_id] = a_sell

    await oms._reconcile_quotes(_signal(inst, strategy="B", side=Side.BUY, price="50000.00"))

    reqs = _order_requests(bus)
    assert len(reqs) == 1
    assert reqs[0].side is Side.BUY
    assert oms.snapshot()["self_trade_prevented"] == 0


async def test_same_strategy_does_not_self_trade_prevent(inst) -> None:
    """STP is sibling-scoped: a strategy's own resting order never blocks it."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    # A's own resting sell at 50000.50; A re-quotes a buy at the same price.
    a_sell = _resting(inst, strategy="A", side=Side.SELL, price="50000.50")
    oms._orders[a_sell.order_id] = a_sell

    await oms._reconcile_quotes(_signal(inst, strategy="A", side=Side.BUY, price="50000.50"))

    assert len(_order_requests(bus)) == 1
    assert oms.snapshot()["self_trade_prevented"] == 0


async def test_disabled_allows_crossing(inst) -> None:
    """With STP off, the crossing leg is routed to the venue."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock(), self_trade_prevention=False)
    a_sell = _resting(inst, strategy="A", side=Side.SELL, price="50000.50")
    oms._orders[a_sell.order_id] = a_sell

    await oms._reconcile_quotes(_signal(inst, strategy="B", side=Side.BUY, price="50000.50"))

    assert len(_order_requests(bus)) == 1
    assert oms.snapshot()["self_trade_prevented"] == 0


async def test_terminal_sibling_order_does_not_block(inst) -> None:
    """A filled/terminal sibling order is off the book — must not block."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    a_sell = _resting(inst, strategy="A", side=Side.SELL, price="50000.50")
    a_sell.status = OrderStatus.FILLED
    oms._orders[a_sell.order_id] = a_sell

    await oms._reconcile_quotes(_signal(inst, strategy="B", side=Side.BUY, price="50000.50"))

    assert len(_order_requests(bus)) == 1
    assert oms.snapshot()["self_trade_prevented"] == 0
