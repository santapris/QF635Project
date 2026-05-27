"""Batch 5: snapshot() methods on all engines + LiveApp.metrics_snapshot().

Each test constructs a minimal engine, optionally seeds some state, then
calls snapshot() and asserts the expected keys and values are present.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from trading.core import (
    AssetType,
    FillEvent,
    Instrument,
    OrderType,
    Side,
    SignalEvent,
    StrategyId,
    TimeInForce,
)
from trading.core.types import (
    ClientOrderId,
    ExchangeOrderId,
    FillId,
    OrderId,
    OrderStatus,
    Price,
    Quantity,
)
from trading.oms import OMSEngine
from trading.oms.order import Order
from trading.position import PositionEngine
from trading.risk import RiskEngine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def btc() -> Instrument:
    return Instrument(
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


# ---------------------------------------------------------------------------
# OMSEngine.snapshot()
# ---------------------------------------------------------------------------

def test_oms_snapshot_empty(clock) -> None:
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    snap = oms.snapshot()
    assert snap["open_orders"] == 0
    assert snap["total_orders"] == 0
    assert snap["dropped_events"] == 0


def test_oms_snapshot_counts_open_orders(clock, btc, strategy_id) -> None:
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    oid = OrderId(uuid4())
    order = Order(
        order_id=oid,
        client_order_id=ClientOrderId("coid-1"),
        strategy_id=strategy_id,
        instrument=btc,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("1"),
        price=None,
        time_in_force=TimeInForce.IOC,
        created_at_ns=clock.now_ns(),
    )
    oms._orders[oid] = order

    snap = oms.snapshot()
    assert snap["open_orders"] == 1
    assert snap["total_orders"] == 1


def test_oms_snapshot_terminal_orders_not_in_open(clock, btc, strategy_id) -> None:
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    oid = OrderId(uuid4())
    order = Order(
        order_id=oid,
        client_order_id=ClientOrderId("coid-2"),
        strategy_id=strategy_id,
        instrument=btc,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("1"),
        price=None,
        time_in_force=TimeInForce.IOC,
        created_at_ns=clock.now_ns(),
    )
    order.transition_to(OrderStatus.REJECTED, at_ns=clock.now_ns())
    oms._orders[oid] = order

    snap = oms.snapshot()
    assert snap["open_orders"] == 0
    assert snap["total_orders"] == 1


# ---------------------------------------------------------------------------
# RiskEngine.snapshot()
# ---------------------------------------------------------------------------

def test_risk_snapshot_default(clock) -> None:
    risk = RiskEngine(bus=_NullBus(), clock=clock)
    snap = risk.snapshot()
    assert snap["kill_switch_engaged"] is False
    assert snap["kill_switch_reason"] is None
    assert snap["dropped_events"] == 0


async def test_risk_snapshot_after_kill_switch(clock, btc, strategy_id) -> None:
    from trading.core.exceptions import BackpressureError

    class _FullBus(_NullBus):
        async def publish(self, topic, event):
            raise BackpressureError("full", topic=topic)

    risk = RiskEngine(bus=_FullBus(), clock=clock)
    await risk._engage_kill_switch(triggered_by="test_rule", reason="unit test")

    snap = risk.snapshot()
    assert snap["kill_switch_engaged"] is True
    assert snap["kill_switch_reason"] == "unit test"


# ---------------------------------------------------------------------------
# PositionEngine.snapshot()
# ---------------------------------------------------------------------------

def test_position_snapshot_empty(clock) -> None:
    pos = PositionEngine(bus=_NullBus(), clock=clock)
    snap = pos.snapshot()
    assert snap["open_positions"] == 0
    assert snap["total_books"] == 0
    assert snap["dropped_events"] == 0


async def test_position_snapshot_after_fill(clock, btc, strategy_id) -> None:
    pos = PositionEngine(bus=_NullBus(), clock=clock)
    fill = FillEvent(
        fill_id=FillId(uuid4()),
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="gw",
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId("c1"),
        exchange_order_id=ExchangeOrderId("e1"),
        strategy_id=strategy_id,
        instrument=btc,
        side=Side.BUY,
        fill_price=Price(Decimal("50000")),
        fill_quantity=Quantity(Decimal("1")),
        cumulative_quantity=Quantity(Decimal("1")),
        leaves_quantity=Quantity(Decimal("0")),
    )
    await pos._on_fill(fill)

    snap = pos.snapshot()
    assert snap["total_books"] == 1
    assert snap["open_positions"] == 1


# ---------------------------------------------------------------------------
# LiveApp.metrics_snapshot()
# ---------------------------------------------------------------------------

def test_live_app_metrics_snapshot_keys() -> None:
    """metrics_snapshot() returns a dict with the expected top-level keys."""
    from trading.config import build_live_app, load_config_from_dict

    raw = {
        "instruments": [{
            "symbol": "BTC-USDT", "exchange": "BINANCE", "asset_type": "SPOT",
            "base_currency": "BTC", "quote_currency": "USDT",
            "tick_size": "0.01", "lot_size": "0.00001",
        }],
        "bus": {"backend": "memory"},
        "order_gateways": [{"venue": "BINANCE", "type": "simulation"}],
    }
    cfg = load_config_from_dict(raw)
    app = build_live_app(cfg)

    snap = app.metrics_snapshot()
    assert "oms" in snap
    assert "risk" in snap
    assert "position" in snap
    assert "order_gateways" in snap


def test_live_app_metrics_snapshot_oms_structure() -> None:
    from trading.config import build_live_app, load_config_from_dict

    raw = {
        "instruments": [{
            "symbol": "BTC-USDT", "exchange": "BINANCE", "asset_type": "SPOT",
            "base_currency": "BTC", "quote_currency": "USDT",
            "tick_size": "0.01", "lot_size": "0.00001",
        }],
        "bus": {"backend": "memory"},
        "order_gateways": [{"venue": "BINANCE", "type": "simulation"}],
    }
    cfg = load_config_from_dict(raw)
    app = build_live_app(cfg)
    snap = app.metrics_snapshot()

    oms_snap = snap["oms"]
    assert "open_orders" in oms_snap
    assert "dropped_events" in oms_snap

    risk_snap = snap["risk"]
    assert "kill_switch_engaged" in risk_snap

    pos_snap = snap["position"]
    assert "open_positions" in pos_snap
