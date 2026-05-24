"""Stage 3: + Risk engine + OMS — adds pre-trade validation and order lifecycle.

Orders are approved or rejected by the risk engine and tracked by OMS, but
nothing is sent to the exchange. The OrderGateway is absent so any order
the OMS would submit just disappears — safe to run with real credentials.

Logs: market-data (debug), signals, risk decisions, orders topic.

Run:
    python -m trading.runners.examples.stage3_risk_oms
"""

from __future__ import annotations

import asyncio
import signal
import structlog
from decimal import Decimal

from trading.core import LiveClock, StrategyId
from trading.event_bus import AsyncioBus, Topic
from trading.feed_handler import FeedHandler, FeedHandlerConfig
from trading.feed_handler.normalizers import BinanceNormalizer
from trading.order_gateways.binance import BinancePublicWSConnector, SymbolMapper
from trading.order_gateways.binance import stream_names
from trading.oms import OMSEngine
from trading.position import AccountingMethod, EnginePortfolioView, PositionEngine
from trading.risk import RiskEngine
from trading.risk.rules import (
    DailyLossLimitRule,
    InstrumentAllowlistRule,
    MaxOrderSizeRule,
    MaxPositionRule,
)
from trading.strategy import StrategyRegistry
from trading.strategy.examples import PingPongStrategy
from trading.logging import configure_logging
from trading.config import load_settings
from trading.monitoring import BusHeartbeat, DashboardServer, subscribe_event_logging
from trading.runners.examples._runner_config import load_runner_config


async def _amain() -> None:
    configure_logging(level="INFO")
    log = structlog.get_logger("stage3")

    settings = load_settings()
    runner_cfg = load_runner_config(
        require_credentials=False,
        futures=settings.market == "futures",
    )
    config = runner_cfg.binance
    instruments = runner_cfg.instruments
    symbols = SymbolMapper(instruments)
    clock = LiveClock()
    bus = AsyncioBus(queue_size=10_000)

    await subscribe_event_logging(
        bus, log,
        topics=(Topic.SIGNALS, Topic.RISK_DECISIONS, Topic.ORDERS),
    )

    position = PositionEngine(bus=bus, clock=clock, method=AccountingMethod.WAVG)
    risk = RiskEngine(bus=bus, clock=clock)
    risk.register_global_rules([
        InstrumentAllowlistRule(allowed_instrument_ids=["BINANCE:BTC-USDT"]),
    ])
    risk.register_rules(StrategyId("ping-pong"), [
        MaxPositionRule(max_long=Decimal("0.001"), max_short=Decimal("0.001")),
        MaxOrderSizeRule(max_quantity=Decimal("0.001")),
        DailyLossLimitRule(max_loss=Decimal("50")),
    ])
    oms = OMSEngine(bus=bus, clock=clock)
    portfolio = EnginePortfolioView(position)
    strategies = StrategyRegistry(bus=bus, clock=clock, portfolio=portfolio)
    strategies.register(
        PingPongStrategy(
            strategy_id=StrategyId("ping-pong"),
            instruments=instruments,
            interval_seconds=10.0,
        ),
        parameters={"target_quantity": "0.0001", "interval_seconds": 10.0},
    )

    streams = []
    for inst in instruments:
        wire = symbols.wire_symbol(inst)
        streams.append(stream_names.book_ticker(wire))
        streams.append(stream_names.agg_trade(wire))

    feed_conn = BinancePublicWSConnector(
        config=config, streams=streams, clock=clock, source="binance-public",
    )
    feed_handler = FeedHandler(
        connector=feed_conn,
        normalizer=BinanceNormalizer(),
        bus=bus,
        clock=clock,
        instruments={symbols.wire_symbol(i): i for i in instruments},
        source="binance-public",
        config=FeedHandlerConfig(stale_threshold_seconds=30.0, max_reconnect_attempts=5),
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    dashboard = (
        DashboardServer(bus=bus, port=settings.dashboard_port)
        if settings.dashboard_port > 0
        else None
    )
    heartbeat = BusHeartbeat(bus=bus, log=log)

    log.info(
        "stage3_starting",
        note="risk+OMS active, no gateway — orders will NOT reach exchange — Ctrl-C to stop",
    )
    await position.start()
    await risk.start()
    await oms.start()
    await strategies.start()
    await bus.start()
    await heartbeat.start()
    if dashboard is not None:
        await dashboard.start()
    feed_task = asyncio.create_task(feed_handler.run(), name="feed-handler")

    try:
        await stop_event.wait()
    finally:
        log.info("stage3_stopping")
        await heartbeat.stop()
        if dashboard is not None:
            await dashboard.stop()
        await feed_handler.stop()
        try:
            await asyncio.wait_for(feed_task, timeout=5)
        except (asyncio.TimeoutError, Exception):
            pass
        await strategies.stop()
        await oms.stop()
        await risk.stop()
        await position.stop()
        await bus.stop()
        log.info("stage3_done")


if __name__ == "__main__":
    asyncio.run(_amain())
