"""End-to-end pipeline: signal -> risk -> OMS -> sim gateway -> position."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from trading.core import (
    OrderType,
    Side,
    SignalEvent,
    TickEvent,
    TimeInForce,
)
from trading.event_bus import AsyncioBus, Topic
from trading.gateways import (
    FeeModel,
    LatencyModel,
    SimulationGateway,
    SimulationGatewayConfig,
)
from trading.oms import OMSEngine
from trading.position import AccountingMethod, PositionEngine
from trading.risk import RiskEngine
from trading.risk.rules import MaxPositionRule


pytestmark = pytest.mark.integration


async def test_full_pipeline_executes_signal(clock, btc, strategy_id) -> None:
    bus = AsyncioBus(queue_size=1000)
    risk = RiskEngine(bus=bus, clock=clock)
    risk.register_rules(
        strategy_id,
        [MaxPositionRule(max_long=Decimal("10"), max_short=Decimal("10"))],
    )
    oms = OMSEngine(bus=bus, clock=clock)
    pos = PositionEngine(bus=bus, clock=clock, method=AccountingMethod.WAVG)
    gw = SimulationGateway(
        bus=bus, clock=clock,
        config=SimulationGatewayConfig(
            venue="BINANCE",
            latency=LatencyModel(submit_ack_ms=0, fill_ms=0, cancel_ack_ms=0),
            fees=FeeModel(maker_bps=1, taker_bps=5),
            seed=1,
        ),
    )

    await risk.start()
    await oms.start()
    await pos.start()
    await gw.start()
    await bus.start()
    try:
        # Tick first so the gateway has a book.
        await bus.publish(Topic.MARKET_DATA, TickEvent(
            ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="md",
            instrument=btc,
            bid_price=Decimal("49999"), bid_size=Decimal("1"),
            ask_price=Decimal("50001"), ask_size=Decimal("1"),
        ))
        await asyncio.sleep(0.05)

        sig = SignalEvent(
            ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="strat",
            strategy_id=strategy_id, instrument=btc, side=Side.BUY,
            target_quantity=Decimal("0.5"),
            order_type=OrderType.MARKET, time_in_force=TimeInForce.IOC,
        )
        await bus.publish(Topic.SIGNALS, sig)
        await asyncio.sleep(0.3)

        position = pos.get_position(strategy_id, btc)
        assert position is not None
        assert position.quantity == Decimal("0.5")
        # Fee at 5 bps on ~25000 notional -> ~12.5 realized loss
        assert position.realized_pnl < Decimal("0")
        assert position.realized_pnl > Decimal("-13")
    finally:
        await bus.stop()


async def test_risk_clamps_oversize_signal(clock, btc, strategy_id) -> None:
    """Verify risk-engine clamping reduces order size end-to-end."""
    bus = AsyncioBus(queue_size=1000)
    risk = RiskEngine(bus=bus, clock=clock)
    risk.register_rules(
        strategy_id,
        [MaxPositionRule(max_long=Decimal("0.3"), max_short=Decimal("0.3"))],
    )
    oms = OMSEngine(bus=bus, clock=clock)
    pos = PositionEngine(bus=bus, clock=clock)
    gw = SimulationGateway(
        bus=bus, clock=clock,
        config=SimulationGatewayConfig(
            venue="BINANCE", seed=1,
            latency=LatencyModel(submit_ack_ms=0, fill_ms=0, cancel_ack_ms=0),
        ),
    )

    await risk.start()
    await oms.start()
    await pos.start()
    await gw.start()
    await bus.start()
    try:
        await bus.publish(Topic.MARKET_DATA, TickEvent(
            ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="md",
            instrument=btc,
            bid_price=Decimal("49999"), bid_size=Decimal("1"),
            ask_price=Decimal("50001"), ask_size=Decimal("1"),
        ))
        await asyncio.sleep(0.05)

        # Strategy wants 1.0 BTC; cap is 0.3 -> clamp expected.
        sig = SignalEvent(
            ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="strat",
            strategy_id=strategy_id, instrument=btc, side=Side.BUY,
            target_quantity=Decimal("1.0"),
            order_type=OrderType.MARKET, time_in_force=TimeInForce.IOC,
        )
        await bus.publish(Topic.SIGNALS, sig)
        await asyncio.sleep(0.3)

        position = pos.get_position(strategy_id, btc)
        assert position is not None
        assert position.quantity == Decimal("0.3")
    finally:
        await bus.stop()
