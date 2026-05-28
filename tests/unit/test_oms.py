"""Unit tests for OMS engine internals — child/leg bookkeeping for sliced legs."""

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


class _NullBus:
    async def publish(self, topic, event): pass
    async def subscribe(self, topic, handler): pass
    async def subscribe_many(self, topics, handler): pass
    async def start(self): pass
    async def stop(self): pass


def _make_order(
    *,
    leg_id: str | None = None,
    quantity: str = "1",
    filled: str = "0",
    terminal: bool = False,
) -> Order:
    o = Order(
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId(f"test-{uuid4().hex[:8]}"),
        strategy_id=StrategyId("test-strat"),
        instrument=Instrument(
            symbol="BTC-USDT",
            exchange="BINANCE",
            asset_type=AssetType.SPOT,
            base_currency="BTC",
            quote_currency="USDT",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.00001"),
        ),
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal(quantity),
        price=Decimal("50000"),
        time_in_force=TimeInForce.GTC,
        created_at_ns=0,
        parent_leg_id=leg_id,
    )
    o.cumulative_filled = Decimal(filled)
    if terminal:
        o.status = OrderStatus.FILLED
    return o


def _oms() -> OMSEngine:
    return OMSEngine(bus=_NullBus(), clock=LiveClock())


def test_leg_has_live_children_true_when_open_child_exists() -> None:
    oms = _oms()
    child = _make_order(leg_id="leg-1")
    oms._orders[child.order_id] = child
    assert oms._leg_has_live_children("leg-1") is True


def test_leg_has_live_children_false_when_only_terminal_children() -> None:
    oms = _oms()
    done = _make_order(leg_id="leg-1", quantity="1", filled="1", terminal=True)
    oms._orders[done.order_id] = done
    assert oms._leg_has_live_children("leg-1") is False


def test_leg_has_live_children_ignores_other_legs() -> None:
    oms = _oms()
    other = _make_order(leg_id="leg-2")
    oms._orders[other.order_id] = other
    assert oms._leg_has_live_children("leg-1") is False


def test_leg_has_live_children_ignores_plain_orders() -> None:
    """Orders with no parent_leg_id are plain quotes, not slice children."""
    oms = _oms()
    plain = _make_order(leg_id=None)
    oms._orders[plain.order_id] = plain
    assert oms._leg_has_live_children("leg-1") is False


def test_retire_algo_removes_state() -> None:
    from trading.oms.execution_algos import TWAPAlgo

    oms = _oms()
    algo = TWAPAlgo(
        quantity=Decimal("1"), duration_seconds=60, num_slices=5, start_ns=0,
    )
    oms._algos["leg-1"] = algo
    oms._algo_ctx["leg-1"] = (None, None)  # context not needed for retire

    oms._retire_algo("leg-1")
    assert "leg-1" not in oms._algos
    assert "leg-1" not in oms._algo_ctx
    assert algo.is_done()  # cancel() makes it done
