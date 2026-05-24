"""Stage 1: Market data only — feed connector → FeedHandler → bus.

Logs every normalised tick (QuoteTick, TradeTick) to stdout.
No strategy, no risk, no orders.

Run:
    python -m trading.runners.examples.stage1_market_data
"""

from __future__ import annotations

import asyncio
import signal
import structlog

from trading.core import LiveClock
from trading.event_bus import AsyncioBus, Topic
from trading.feed_handler import FeedHandler, FeedHandlerConfig
from trading.feed_handler.normalizers import BinanceNormalizer
from trading.order_gateways.binance import BinancePublicWSConnector, SymbolMapper
from trading.order_gateways.binance import stream_names
from trading.logging import configure_logging
from trading.config import load_settings
from trading.monitoring import BusHeartbeat, DashboardServer
from trading.runners.examples._runner_config import load_runner_config


async def _amain() -> None:
    configure_logging(level="INFO")
    log = structlog.get_logger("stage1")

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

    async def _log_market_data(event) -> None:
        log.info(
            "market_data",
            event_type=type(event).__name__,
            instrument=str(getattr(getattr(event, "instrument", None), "symbol", "?")),
            **{
                k: str(getattr(event, k))
                for k in ("bid_price", "ask_price", "price", "quantity")
                if hasattr(event, k)
            },
        )

    await bus.subscribe(Topic.MARKET_DATA, _log_market_data)

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

    log.info("stage1_starting", note="watching market-data topic — Ctrl-C to stop")
    await bus.start()
    await heartbeat.start()
    if dashboard is not None:
        await dashboard.start()
    feed_task = asyncio.create_task(feed_handler.run(), name="feed-handler")

    try:
        await stop_event.wait()
    finally:
        log.info("stage1_stopping")
        await heartbeat.stop()
        if dashboard is not None:
            await dashboard.stop()
        await feed_handler.stop()
        try:
            await asyncio.wait_for(feed_task, timeout=5)
        except (asyncio.TimeoutError, Exception):
            pass
        await bus.stop()
        log.info("stage1_done")


if __name__ == "__main__":
    asyncio.run(_amain())
