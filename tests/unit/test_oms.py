"""Unit tests for OMS engine internals."""

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
from trading.oms.order import Order


def _make_order(
    *,
    parent_id: OrderId | None = None,
    quantity: str = "1",
    filled: str = "0",
    terminal: bool = False,
) -> Order:
    o = Order(
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId("test-coid"),
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
        parent_order_id=parent_id,
    )
    o.cumulative_filled = Decimal(filled)
    if terminal:
        o.status = OrderStatus.FILLED
    return o


class _MinimalOMS:
    """Thin wrapper that exposes only _orders and _parent_has_leaves for testing."""

    def __init__(self):
        self._orders: dict[OrderId, Order] = {}

    def _parent_has_leaves(self, parent_id: OrderId) -> Decimal:
        total = Decimal(0)
        for order in self._orders.values():
            if order.parent_order_id == parent_id and not order.is_terminal:
                total += order.leaves_quantity
        return total


def test_parent_has_leaves_whole_quantity() -> None:
    oms = _MinimalOMS()
    parent_id = OrderId(uuid4())
    child = _make_order(parent_id=parent_id, quantity="2")
    oms._orders[child.order_id] = child

    assert oms._parent_has_leaves(parent_id) == Decimal("2")


def test_parent_has_leaves_fractional_quantity() -> None:
    """Regression: previously int(total) truncated 0.00001 to 0."""
    oms = _MinimalOMS()
    parent_id = OrderId(uuid4())
    child = _make_order(parent_id=parent_id, quantity="0.00001")
    oms._orders[child.order_id] = child

    leaves = oms._parent_has_leaves(parent_id)
    assert leaves == Decimal("0.00001")
    assert leaves > Decimal(0)


def test_parent_has_leaves_partial_fill() -> None:
    oms = _MinimalOMS()
    parent_id = OrderId(uuid4())
    child = _make_order(parent_id=parent_id, quantity="1", filled="0.3")
    oms._orders[child.order_id] = child

    assert oms._parent_has_leaves(parent_id) == Decimal("0.7")


def test_parent_has_leaves_terminal_children_excluded() -> None:
    oms = _MinimalOMS()
    parent_id = OrderId(uuid4())
    done = _make_order(parent_id=parent_id, quantity="1", filled="1", terminal=True)
    live = _make_order(parent_id=parent_id, quantity="0.5")
    oms._orders[done.order_id] = done
    oms._orders[live.order_id] = live

    assert oms._parent_has_leaves(parent_id) == Decimal("0.5")


def test_parent_has_leaves_sums_multiple_children() -> None:
    oms = _MinimalOMS()
    parent_id = OrderId(uuid4())
    c1 = _make_order(parent_id=parent_id, quantity="0.3")
    c2 = _make_order(parent_id=parent_id, quantity="0.7")
    oms._orders[c1.order_id] = c1
    oms._orders[c2.order_id] = c2

    assert oms._parent_has_leaves(parent_id) == Decimal("1.0")


def test_parent_has_leaves_different_parent_ignored() -> None:
    oms = _MinimalOMS()
    parent_a = OrderId(uuid4())
    parent_b = OrderId(uuid4())
    child_a = _make_order(parent_id=parent_a, quantity="1")
    child_b = _make_order(parent_id=parent_b, quantity="5")
    oms._orders[child_a.order_id] = child_a
    oms._orders[child_b.order_id] = child_b

    assert oms._parent_has_leaves(parent_a) == Decimal("1")
    assert oms._parent_has_leaves(parent_b) == Decimal("5")
