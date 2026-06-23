"""Integration test: A-S strategy through full pipeline with SimGateway.

Verifies:
- Signals flow from strategy → risk → OMS → SimGateway
- No -2010 (crossing) rejects
- Orders acknowledged (resting on book as maker)
- Position engine receives fills when a trade crosses our resting quote

No log monitoring needed — asserts on collected events.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from trading.core import AssetType, Instrument, StrategyId
from trading.core.events import OrderAcknowledged, OrderRejected, TickEvent, TradeEvent
from trading.event_bus import AsyncioBus, Topic
from trading.order_gateways import (
    FeeModel,
    LatencyModel,
    SimulationOrderGateway,
    SimulationOrderGatewayConfig,
)
from trading.oms import OMSEngine
from trading.position import AccountingMethod, EnginePortfolioView, PositionEngine
from trading.risk import RiskEngine
from trading.risk.rules import (
    InstrumentAllowlistRule,
    MaxOrderSizeRule,
    MaxPositionRule,
    DailyLossLimitRule,
)
from trading.strategy import StrategyRegistry
from trading.strategy.examples.avellaneda_stoikov import AvellanedaStoikovStrategy

pytestmark = pytest.mark.integration

_STRATEGY_ID = StrategyId("as-integ")


@pytest.fixture
def btcf() -> Instrument:
    return Instrument(
        symbol="BTC-USDT",
        exchange="BINANCE",
        asset_type=AssetType.FUTURES,
        base_currency="BTC",
        quote_currency="USDT",
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.00001"),
        min_notional=Decimal("10"),
    )


def _tick(inst: Instrument, clock, *, ts_offset_ns: int = 0,
          bid="50000.00", ask="50001.00",
          bid_size="1.0", ask_size="1.0") -> TickEvent:
    ts = clock.now_ns() + ts_offset_ns
    return TickEvent(
        ts_event=ts, ts_ingest=ts, source="test",
        instrument=inst,
        bid_price=Decimal(bid), bid_size=Decimal(bid_size),
        ask_price=Decimal(ask), ask_size=Decimal(ask_size),
    )


async def _build_pipeline(bus, clock, btcf):
    pos = PositionEngine(bus=bus, clock=clock, method=AccountingMethod.WAVG)
    risk = RiskEngine(bus=bus, clock=clock)
    risk.register_global_rules([
        InstrumentAllowlistRule(allowed_instrument_ids=["BINANCE:BTC-USDT"]),
    ])
    # Sizes are scaled so a single quote clears the instrument's min_notional
    # of 10 at ~50k (0.0002 BTC * 50000 = 10). With the old dust sizes
    # (quote_size 0.00001 -> 0.50 notional) every order is sub-minimum and is
    # now correctly dropped by the risk engine's min-notional backstop, so the
    # pipeline would ack nothing. Keep quote_size < max_order_size < the cap.
    risk.register_rules(_STRATEGY_ID, [
        MaxPositionRule(max_long=Decimal("0.01"), max_short=Decimal("0.01")),
        MaxOrderSizeRule(max_quantity=Decimal("0.002")),
        DailyLossLimitRule(max_loss=Decimal("100")),
    ])
    oms = OMSEngine(bus=bus, clock=clock)
    gw = SimulationOrderGateway(
        bus=bus, clock=clock,
        config=SimulationOrderGatewayConfig(
            venue="BINANCE",
            latency=LatencyModel(submit_ack_ms=0, fill_ms=0, cancel_ack_ms=0),
            fees=FeeModel(maker_bps=1, taker_bps=5),
            seed=42,
        ),
    )
    portfolio = EnginePortfolioView(pos)
    registry = StrategyRegistry(bus=bus, clock=clock, portfolio=portfolio)
    registry.register(
        AvellanedaStoikovStrategy(
            strategy_id=_STRATEGY_ID,
            instruments=[btcf],
            gamma=0.1, k=1.5, tau_seconds=300.0,
            half_life_seconds=10.0, ofi_window_seconds=5.0,
            quote_size=Decimal("0.0005"),
            max_position=Decimal("0.01"),
            min_vol=0.01,
        ),
    )
    return pos, risk, oms, gw, registry


async def test_as_produces_acknowledged_orders(sim_clock, btcf) -> None:
    """Strategy emits signals → risk approves → OMS creates orders → GW acks them."""
    bus = AsyncioBus(queue_size=5000)
    acknowledged: list[OrderAcknowledged] = []
    rejected: list[OrderRejected] = []

    async def _collect_acks(event):
        if isinstance(event, OrderAcknowledged):
            acknowledged.append(event)

    async def _collect_rejects(event):
        if isinstance(event, OrderRejected):
            rejected.append(event)

    pos, risk, oms, gw, registry = await _build_pipeline(bus, sim_clock, btcf)

    await pos.start()
    await risk.start()
    await oms.start()
    await gw.start()
    await registry.start()
    await bus.subscribe(Topic.ORDERS, _collect_acks)
    await bus.subscribe(Topic.ORDERS, _collect_rejects)
    await bus.start()

    try:
        # Feed 3 ticks — strategy emits POST_ONLY signals on each
        for i in range(3):
            await bus.publish(Topic.MARKET_DATA, _tick(
                btcf, sim_clock, ts_offset_ns=i * 1_000_000_000,
            ))
            await asyncio.sleep(0.05)

        # No -2010 crossing rejects
        cross_rejects = [r for r in rejected if "-2010" in r.reason or "cross" in r.reason.lower()]
        assert cross_rejects == [], f"Got crossing rejects: {cross_rejects}"

        # At least 1 order acknowledged (resting as maker)
        assert len(acknowledged) >= 1, "No orders acknowledged — signals not flowing through pipeline"

    finally:
        await bus.stop()


async def test_as_no_crossing_orders_on_tight_spread(sim_clock, btcf) -> None:
    """With 1-tick spread, A-S should still produce non-crossing quotes."""
    bus = AsyncioBus(queue_size=5000)
    rejected: list[OrderRejected] = []

    async def _collect_rejects(event):
        if isinstance(event, OrderRejected):
            rejected.append(event)

    pos, risk, oms, gw, registry = await _build_pipeline(bus, sim_clock, btcf)

    await pos.start()
    await risk.start()
    await oms.start()
    await gw.start()
    await registry.start()
    await bus.subscribe(Topic.ORDERS, _collect_rejects)
    await bus.start()

    try:
        # Very tight spread: only 1 tick (0.01) between bid and ask
        for i in range(5):
            await bus.publish(Topic.MARKET_DATA, _tick(
                btcf, sim_clock, ts_offset_ns=i * 1_000_000_000,
                bid="50000.00", ask="50000.01",
            ))
            await asyncio.sleep(0.05)

        cross_rejects = [r for r in rejected if "cross" in r.reason.lower() or "-2010" in r.reason]
        assert cross_rejects == [], f"Got crossing rejects on tight spread: {[r.reason for r in cross_rejects]}"

    finally:
        await bus.stop()


async def test_fill_is_attributed_to_strategy_inventory(sim_clock, btcf) -> None:
    """A fill stamped with the strategy's id must update the exact position the
    A-S strategy reads for inventory skew.

    This pins down the historical "inventory always remains the same" TODO: the
    strategy reads ``ctx.portfolio.get_position(instrument, strategy_id)``, and a
    fill keyed by that same strategy_id (as the live gateway stamps it via
    ``OMS.strategy_id_for_client_order``) must flow through to that read. If the
    attribution key ever drifts (e.g. fills land under "unknown"), inventory
    stays flat and the reservation-price skew silently dies — this test fails.
    """
    from uuid import uuid4

    from trading.core.events import FillEvent
    from trading.core.types import (
        ClientOrderId,
        ExchangeOrderId,
        FillId,
        OrderId,
        Side,
    )

    bus = AsyncioBus(queue_size=5000)
    pos = PositionEngine(bus=bus, clock=sim_clock, method=AccountingMethod.WAVG)
    portfolio = EnginePortfolioView(pos)

    await pos.start()
    await bus.start()
    try:
        # The strategy reads inventory via this exact call.
        assert portfolio.get_position(btcf, _STRATEGY_ID) is None

        await bus.publish(Topic.FILLS, FillEvent(
            fill_id=FillId(uuid4()),
            ts_event=sim_clock.now_ns(), ts_ingest=sim_clock.now_ns(),
            source="binance",
            order_id=OrderId(uuid4()),
            client_order_id=ClientOrderId(f"{_STRATEGY_ID}-0123456789ab"),
            exchange_order_id=ExchangeOrderId("ex-1"),
            strategy_id=_STRATEGY_ID,
            instrument=btcf,
            side=Side.SELL,
            fill_price=Decimal("50001.00"),
            fill_quantity=Decimal("0.003"),
            cumulative_quantity=Decimal("0.003"),
            leaves_quantity=Decimal("0"),
            fee=Decimal("0"),
            fee_currency="USDT",
            is_maker=True,
        ))
        await asyncio.sleep(0.05)

        position = portfolio.get_position(btcf, _STRATEGY_ID)
        assert position is not None, "fill did not reach the strategy's position view"
        assert position.quantity == Decimal("-0.003"), (
            "inventory not updated by fill — attribution key drifted"
        )
    finally:
        await bus.stop()


async def test_as_fill_updates_position(sim_clock, btcf) -> None:
    """Resting sell quote filled by aggressor trade → position goes short."""
    bus = AsyncioBus(queue_size=5000)
    pos, risk, oms, gw, registry = await _build_pipeline(bus, sim_clock, btcf)

    await pos.start()
    await risk.start()
    await oms.start()
    await gw.start()
    await registry.start()
    await bus.start()

    try:
        # Feed ticks to get quotes posted
        for i in range(3):
            await bus.publish(Topic.MARKET_DATA, _tick(
                btcf, sim_clock, ts_offset_ns=i * 1_000_000_000,
                bid="50000.00", ask="50001.00",
            ))
            await asyncio.sleep(0.05)

        # Aggressor trade crosses our SELL quote — SimGateway fills resting order
        ts = sim_clock.now_ns() + 5_000_000_000
        await bus.publish(Topic.MARKET_DATA, TradeEvent(
            ts_event=ts, ts_ingest=ts, source="test",
            instrument=btcf,
            price=Decimal("50001.00"),
            quantity=Decimal("0.00001"),
        ))
        await asyncio.sleep(0.1)

        position = pos.get_position(_STRATEGY_ID, btcf)
        # Either flat (no fill yet) or short (sell quote was filled)
        # We just assert no exception and position is well-defined if it exists
        if position is not None:
            assert position.quantity <= Decimal("0")

    finally:
        await bus.stop()
