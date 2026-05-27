"""Batch 2: verify every engine survives BackpressureError from the bus.

Each test wires an engine to a stub bus that always raises BackpressureError
on publish, then exercises a code path that calls publish.  Assertions check:
  - the engine does not re-raise (caller is shielded)
  - _dropped_events counter increments
  - a CRITICAL/ERROR log line is emitted
  - for the OMS _submit_child path, the order state is rolled back to REJECTED
"""

from __future__ import annotations

import logging
from decimal import Decimal
from uuid import uuid4

import pytest

from trading.core import (
    AssetType,
    FillEvent,
    Instrument,
    LiveClock,
    OrderLeg,
    OrderType,
    Side,
    SignalEvent,
    StrategyId,
    TimeInForce,
)
from trading.core.events import (
    OrderRequest,
    RiskDecision,
    TickEvent,
)
from trading.core.exceptions import BackpressureError
from trading.core.types import (
    ClientOrderId,
    ExchangeOrderId,
    FillId,
    OrderId,
    OrderStatus,
    Price,
    Quantity,
)
from trading.order_gateways.binance.order_gateway import BinanceOrderGateway
from trading.oms import OMSEngine
from trading.oms.order import Order
from trading.position import PositionEngine
from trading.risk import RiskEngine


# ---------------------------------------------------------------------------
# Shared stub bus
# ---------------------------------------------------------------------------

class _AlwaysFullBus:
    """Stub bus that raises BackpressureError on every publish call."""

    async def publish(self, topic: str, event):
        raise BackpressureError("test queue full", topic=topic)

    async def subscribe(self, topic, handler): pass
    async def subscribe_many(self, topics, handler): pass
    async def start(self): pass
    async def stop(self): pass


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def full_bus() -> _AlwaysFullBus:
    return _AlwaysFullBus()


@pytest.fixture
def btc_inst() -> Instrument:
    return Instrument(
        symbol="BTC-USDT", exchange="BINANCE", asset_type=AssetType.SPOT,
        base_currency="BTC", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.00001"),
    )


def _signal(clock, inst, strategy_id, qty="1") -> SignalEvent:
    return SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="test",
        strategy_id=strategy_id, instrument=inst,
        legs=(OrderLeg(
            side=Side.BUY,
            quantity=Decimal(qty),
            order_type=OrderType.MARKET,
            time_in_force=TimeInForce.IOC,
        ),),
    )


# ---------------------------------------------------------------------------
# 2.1  RiskEngine
# ---------------------------------------------------------------------------

async def test_risk_engine_backpressure_on_decision(
    full_bus, clock, btc_inst, strategy_id, caplog
) -> None:
    """A full bus must not crash the risk engine; drop counter increments."""
    risk = RiskEngine(bus=full_bus, clock=clock)
    sig = _signal(clock, btc_inst, strategy_id)

    with caplog.at_level(logging.CRITICAL, logger="trading.risk.engine"):
        # _on_signal → _publish_decision → _safe_publish → backpressure
        await risk._on_signal(sig)

    assert risk._dropped_events >= 1
    assert "backpressure" in caplog.text.lower()


async def test_risk_engine_backpressure_on_alert(
    full_bus, clock, btc_inst, strategy_id, caplog
) -> None:
    """Alert publish failure is also swallowed (doesn't stop decision flow)."""
    from trading.risk.base import RuleResult
    from trading.core.types import Severity

    risk = RiskEngine(bus=full_bus, clock=clock)

    with caplog.at_level(logging.CRITICAL, logger="trading.risk.engine"):
        await risk._publish_alert(
            "test_rule",
            RuleResult(
                approved=False, rule_name="test_rule",
                reason="test", severity=Severity.WARN,
            ),
        )

    assert risk._dropped_events >= 1


async def test_risk_engine_backpressure_on_kill_switch(
    full_bus, clock, caplog
) -> None:
    """Kill switch event drop is logged; the switch state is still latched."""
    risk = RiskEngine(bus=full_bus, clock=clock)

    with caplog.at_level(logging.CRITICAL, logger="trading.risk.engine"):
        await risk._engage_kill_switch(triggered_by="test", reason="unit test")

    # The in-memory switch IS engaged even though the event was dropped.
    assert risk.kill_switch.engaged
    assert risk._dropped_events >= 1


# ---------------------------------------------------------------------------
# 2.2  OMSEngine
# ---------------------------------------------------------------------------

def _make_order(inst: Instrument, strategy_id: StrategyId) -> Order:
    oid = OrderId(uuid4())
    return Order(
        order_id=oid,
        client_order_id=ClientOrderId(f"test-{oid.hex[:8]}"),
        strategy_id=strategy_id,
        instrument=inst,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("1"),
        price=None,
        time_in_force=TimeInForce.IOC,
        created_at_ns=0,
    )


async def test_oms_place_quote_backpressure_rejects_order(
    full_bus, clock, btc_inst, strategy_id, caplog
) -> None:
    """When the OrderRequest publish fails, the order is transitioned to
    REJECTED — no stuck PENDING_NEW orders."""
    oms = OMSEngine(bus=full_bus, clock=clock)
    sig = _signal(clock, btc_inst, strategy_id)
    leg = sig.legs[0]

    with caplog.at_level(logging.CRITICAL, logger="trading.oms.engine"):
        await oms._place_quote(sig, leg)

    assert oms._dropped_events >= 1
    assert "backpressure" in caplog.text.lower()

    # The one order created must be REJECTED.
    assert len(oms._orders) == 1
    (order,) = oms._orders.values()
    assert order.status == OrderStatus.REJECTED
    assert order.reject_reason is not None


async def test_oms_cancel_order_backpressure_logged(
    full_bus, clock, btc_inst, strategy_id, caplog
) -> None:
    """A dropped CancelRequest is logged at CRITICAL; cancel_order does not raise."""
    oms = OMSEngine(bus=full_bus, clock=clock)
    order = _make_order(btc_inst, strategy_id)
    # Manually advance to ACKNOWLEDGED so cancel is legal.
    order.transition_to(OrderStatus.ACKNOWLEDGED, at_ns=clock.now_ns())
    oms._orders[order.order_id] = order

    with caplog.at_level(logging.CRITICAL, logger="trading.oms.engine"):
        await oms.cancel_order(order.order_id)

    assert oms._dropped_events >= 1
    assert "backpressure" in caplog.text.lower()
    # Order should be in PENDING_CANCEL (the publish failed, not the transition).
    assert order.status == OrderStatus.PENDING_CANCEL


# ---------------------------------------------------------------------------
# 2.3  PositionEngine
# ---------------------------------------------------------------------------

def _fill(clock, inst: Instrument, strategy_id: StrategyId) -> FillEvent:
    oid = OrderId(uuid4())
    return FillEvent(
        fill_id=FillId(uuid4()),
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="gw",
        order_id=oid,
        client_order_id=ClientOrderId("c1"),
        exchange_order_id=ExchangeOrderId("e1"),
        strategy_id=strategy_id,
        instrument=inst,
        side=Side.BUY,
        fill_price=Price(Decimal("50000")),
        fill_quantity=Quantity(Decimal("1")),
        cumulative_quantity=Quantity(Decimal("1")),
        leaves_quantity=Quantity(Decimal("0")),
    )


async def test_position_engine_backpressure_on_fill(
    full_bus, clock, btc_inst, strategy_id, caplog
) -> None:
    """A dropped PositionUpdateEvent is logged at ERROR; the fill is still applied."""
    pos = PositionEngine(bus=full_bus, clock=clock)
    fill = _fill(clock, btc_inst, strategy_id)

    with caplog.at_level(logging.ERROR, logger="trading.position.engine"):
        await pos._on_fill(fill)

    assert pos._dropped_events >= 1
    assert "backpressure" in caplog.text.lower()
    # Book was updated even though the event was dropped.
    book = pos.get_book(strategy_id, btc_inst)
    assert book is not None
    assert book.quantity == Decimal("1")


async def test_position_engine_backpressure_on_portfolio_snapshot(
    full_bus, clock, btc_inst, strategy_id, caplog
) -> None:
    """publish_portfolio_snapshot drop is logged and does not raise."""
    pos = PositionEngine(bus=full_bus, clock=clock)

    with caplog.at_level(logging.ERROR, logger="trading.position.engine"):
        await pos.publish_portfolio_snapshot()

    # No books — nothing to publish, so no drop here.  Seed a fill and retry.
    fill = _fill(clock, btc_inst, strategy_id)
    await pos._on_fill(fill)          # book created; drop counter = 1 (from fill)
    drops_before = pos._dropped_events

    with caplog.at_level(logging.ERROR, logger="trading.position.engine"):
        await pos.publish_portfolio_snapshot()

    assert pos._dropped_events == drops_before + 1


# ---------------------------------------------------------------------------
# 2.4  BinanceOrderGateway (publish helpers only — no live network)
# ---------------------------------------------------------------------------

@pytest.fixture
def binance_gw(full_bus, clock, btc_inst):
    from trading.order_gateways.binance.config import BinanceConfig, BinanceCredentials
    from trading.order_gateways.binance.symbols import SymbolMapper

    cfg = BinanceConfig(
        spot_rest_base="https://testnet.binance.vision",
        spot_ws_base="wss://testnet.binance.vision",
        futures_rest_base="",
        futures_ws_base="",
    )
    creds = BinanceCredentials(api_key="k", api_secret="s")
    symbols = SymbolMapper([btc_inst])

    class _FakeREST:
        async def connect(self): pass
        async def close(self): pass

    return BinanceOrderGateway(
        bus=full_bus, clock=clock, config=cfg,
        credentials=creds, symbols=symbols,
        rest_client=_FakeREST(),
    )


async def test_order_gateway_backpressure_on_ack(binance_gw, btc_inst, clock, caplog) -> None:
    oid = OrderId(uuid4())
    req = OrderRequest(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="test",
        order_id=oid, client_order_id=ClientOrderId("c1"),
        strategy_id=StrategyId("s1"), instrument=btc_inst,
        side=Side.BUY, order_type=OrderType.MARKET,
        quantity=Decimal("1"), price=None, time_in_force=TimeInForce.IOC,
    )
    with caplog.at_level(logging.CRITICAL, logger="trading.order_gateways.binance.order_gateway"):
        await binance_gw._publish_ack(req, ExchangeOrderId("ex-1"))

    assert binance_gw._dropped_events >= 1
    assert "backpressure" in caplog.text.lower()


async def test_order_gateway_backpressure_on_reject(binance_gw, btc_inst, clock, caplog) -> None:
    oid = OrderId(uuid4())
    req = OrderRequest(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="test",
        order_id=oid, client_order_id=ClientOrderId("c2"),
        strategy_id=StrategyId("s1"), instrument=btc_inst,
        side=Side.BUY, order_type=OrderType.MARKET,
        quantity=Decimal("1"), price=None, time_in_force=TimeInForce.IOC,
    )
    with caplog.at_level(logging.CRITICAL, logger="trading.order_gateways.binance.order_gateway"):
        await binance_gw._publish_reject(req, "insufficient balance")

    assert binance_gw._dropped_events >= 1
