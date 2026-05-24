"""Stage 4: Full pipeline minus the balance reconciler.

Adds BinanceOrderGateway + ListenKeyManager + BinanceUserDataStream on top
of stage 3. Orders ARE sent to the Binance testnet. Requires valid testnet
API credentials in your environment / settings.

Logs: signals, risk decisions, orders, fills, positions.

Run:
    python -m trading.runners.stage4_order_gateway
"""

from __future__ import annotations

import asyncio
import signal
import structlog
from decimal import Decimal

from trading.core import AssetType, Instrument, LiveClock, StrategyId
from trading.event_bus import AsyncioBus, Topic
from trading.feed_handler import FeedHandler, FeedHandlerConfig
from trading.feed_handler.normalizers import BinanceNormalizer
from trading.order_gateways.binance import (
    BinanceConfig,
    BinanceCredentials,
    BinanceOrderGateway,
    BinancePublicWSConnector,
    BinanceRESTClient,
    BinanceUserDataStream,
    ListenKeyManager,
    SymbolMapper,
)
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


async def _amain() -> None:
    configure_logging(level="INFO")
    log = structlog.get_logger("stage4")

    settings = load_settings()
    config = BinanceConfig.from_settings(settings)
    credentials = BinanceCredentials(api_key=settings.api_key, api_secret=settings.api_secret)

    instruments = [
        Instrument(
            symbol="BTC-USDT",
            exchange="BINANCE",
            asset_type=AssetType.FUTURES,
            base_currency="BTC",
            quote_currency="USDT",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.00001"),
            min_notional=Decimal("10"),
        ),
    ]
    symbols = SymbolMapper(instruments)
    clock = LiveClock()
    bus = AsyncioBus(queue_size=10_000)

    await subscribe_event_logging(
        bus, log,
        topics=(
            Topic.SIGNALS, Topic.RISK_DECISIONS, Topic.ORDERS,
            Topic.FILLS, Topic.POSITIONS,
        ),
    )

    rest = BinanceRESTClient(config=config, credentials=credentials, clock=clock)

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
    order_gateway = BinanceOrderGateway(
        bus=bus, clock=clock, config=config, credentials=credentials,
        symbols=symbols, rest_client=rest,
    )
    listen_keys = ListenKeyManager(rest=rest, config=config)
    user_data = BinanceUserDataStream(
        bus=bus, clock=clock, config=config,
        listen_key_manager=listen_keys, symbols=symbols,
        strategy_id_lookup=oms.strategy_id_for_client_order,
    )
    portfolio = EnginePortfolioView(position)
    strategies = StrategyRegistry(bus=bus, clock=clock, portfolio=portfolio)
    strategies.register(
        PingPongStrategy(
            strategy_id=StrategyId("ping-pong"),
            instruments=instruments,
            interval_seconds=10.0,
        ),
        parameters={"target_quantity": "0.002", "interval_seconds": 10.0},
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
        "stage4_starting",
        note="ORDERS WILL BE SENT TO TESTNET — Ctrl-C to stop",
    )
    await position.start()
    await risk.start()
    await oms.start()
    await rest.connect()
    await order_gateway.start()
    await strategies.start()
    await listen_keys.start()
    await user_data.start()
    await bus.start()
    await heartbeat.start()
    if dashboard is not None:
        await dashboard.start()
    feed_task = asyncio.create_task(feed_handler.run(), name="feed-handler")

    try:
        await stop_event.wait()
    finally:
        log.info("stage4_stopping")
        await heartbeat.stop()
        if dashboard is not None:
            await dashboard.stop()
        await feed_handler.stop()
        try:
            await asyncio.wait_for(feed_task, timeout=5)
        except (asyncio.TimeoutError, Exception):
            pass
        await user_data.stop()
        await listen_keys.stop()
        await strategies.stop()
        await order_gateway.stop()
        await rest.close()
        await oms.stop()
        await risk.stop()
        await position.stop()
        await bus.stop()
        log.info("stage4_done")


if __name__ == "__main__":
    asyncio.run(_amain())
