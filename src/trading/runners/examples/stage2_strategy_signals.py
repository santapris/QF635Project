"""Stage 2: Market data + strategy — adds signal generation on top of stage 1.

Logs every normalised tick AND every order signal the strategy emits.
No risk, no OMS, no orders sent to exchange.

Run:
    python -m trading.runners.examples.stage2_strategy_signals
"""

from __future__ import annotations

import asyncio
import signal
import structlog

from trading.core import LiveClock, StrategyId
from trading.event_bus import AsyncioBus, Topic
from trading.feed_handler import FeedHandler, FeedHandlerConfig
from trading.feed_handler.normalizers import BinanceNormalizer
from trading.order_gateways.binance import BinancePublicWSConnector, SymbolMapper
from trading.order_gateways.binance import stream_names
from trading.position import AccountingMethod, EnginePortfolioView, PositionEngine
from trading.strategy import StrategyRegistry
from trading.strategy.examples import PingPongStrategy
from trading.logging import configure_logging
from trading.config import load_settings
from trading.monitoring import BusHeartbeat, DashboardServer, subscribe_event_logging
from trading.runners.examples._runner_config import load_runner_config


async def _amain() -> None:
    configure_logging(level="INFO")
    log = structlog.get_logger("stage2")

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

    await subscribe_event_logging(bus, log, topics=(Topic.SIGNALS,))

    position = PositionEngine(bus=bus, clock=clock, method=AccountingMethod.WAVG)
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
        "stage2_starting",
        note="watching market-data + signals topics — Ctrl-C to stop",
        strategy="ping-pong",
        interval_seconds=10.0,
    )
    await position.start()
    await strategies.start()
    await bus.start()
    await heartbeat.start()
    if dashboard is not None:
        await dashboard.start()
    feed_task = asyncio.create_task(feed_handler.run(), name="feed-handler")

    try:
        await stop_event.wait()
    finally:
        log.info("stage2_stopping")
        await heartbeat.stop()
        if dashboard is not None:
            await dashboard.stop()
        await feed_handler.stop()
        try:
            await asyncio.wait_for(feed_task, timeout=5)
        except (asyncio.TimeoutError, Exception):
            pass
        await strategies.stop()
        await position.stop()
        await bus.stop()
        log.info("stage2_done")


if __name__ == "__main__":
    asyncio.run(_amain())
