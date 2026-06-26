"""OMSEngine.cancel_strategy_orders — scoped cancellation for a paused strategy.

Cancels only the target strategy's non-terminal orders (across all its
instruments), leaves other strategies untouched, and tolerates a strategy with
nothing resting.
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
from trading.core.types import ClientOrderId, OrderId, OrderStatus
from trading.oms.engine import OMSEngine
from trading.oms.order import Order

_INST = Instrument(
    symbol="BTC-USDT", exchange="BINANCE", asset_type=AssetType.SPOT,
    base_currency="BTC", quote_currency="USDT",
    tick_size=Decimal("0.01"), lot_size=Decimal("0.00001"),
)


class _NullBus:
    async def publish(self, topic, event): pass
    async def subscribe(self, topic, handler): pass
    async def subscribe_many(self, topics, handler): pass
    async def start(self): pass
    async def stop(self): pass


def _resting_order(strategy_id: str) -> Order:
    o = Order(
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId(f"test-{uuid4().hex[:8]}"),
        strategy_id=StrategyId(strategy_id),
        instrument=_INST,
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("1"),
        price=Decimal("50000"),
        time_in_force=TimeInForce.GTC,
        created_at_ns=0,
    )
    # Drive to a resting (cancellable) state.
    o.transition_to(OrderStatus.ACKNOWLEDGED, at_ns=0)
    return o


def _oms_with(*orders: Order) -> OMSEngine:
    oms = OMSEngine(bus=_NullBus(), clock=LiveClock())
    for o in orders:
        oms._orders[o.order_id] = o
    return oms


async def test_cancels_only_target_strategy() -> None:
    a1, a2 = _resting_order("strat-a"), _resting_order("strat-a")
    b1 = _resting_order("strat-b")
    oms = _oms_with(a1, a2, b1)

    cancelled = await oms.cancel_strategy_orders(StrategyId("strat-a"))

    assert cancelled == 2
    assert a1.status is OrderStatus.PENDING_CANCEL
    assert a2.status is OrderStatus.PENDING_CANCEL
    assert b1.status is OrderStatus.ACKNOWLEDGED  # untouched


async def test_skips_terminal_orders() -> None:
    live = _resting_order("strat-a")
    done = _resting_order("strat-a")
    done.transition_to(OrderStatus.FILLED, at_ns=0)
    oms = _oms_with(live, done)

    cancelled = await oms.cancel_strategy_orders(StrategyId("strat-a"))

    assert cancelled == 1
    assert live.status is OrderStatus.PENDING_CANCEL
    assert done.status is OrderStatus.FILLED


async def test_no_resting_orders_is_noop() -> None:
    oms = _oms_with(_resting_order("strat-b"))
    cancelled = await oms.cancel_strategy_orders(StrategyId("strat-a"))
    assert cancelled == 0
