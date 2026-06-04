"""Run the full Binance Futures testnet pipeline with Avellaneda-Stoikov MM.

End-to-end wiring:
- Feed handler: public WS connector (bookTicker + aggTrade streams).
- Risk engine + per-strategy rules.
- OMS.
- Position engine.
- :class:`BinanceOrderGateway` for order entry/cancel.
- :class:`ListenKeyManager` + :class:`BinanceUserDataStream` for the
  fill feedback loop.
- :class:`BalanceReconciler` for the safety net.
- :class:`AvellanedaStoikovStrategy` — optimal MM quoting with microprice,
  EWMA vol, OFI tilt, and VPIN toxicity gate.

To run:
    python -m trading.runners.examples.binance_testnet

This is a long-running process — Ctrl-C to stop. Watch for POST_ONLY
orders appearing in the logs within the first few ticks. If you see
-2010 (cross) rejects, the post_only_guard is misfiring — investigate
before running longer.
"""

from __future__ import annotations

import asyncio
import signal
import structlog
from decimal import Decimal

from trading.core import LiveClock, StrategyId
from trading.event_bus import AsyncioBus
from trading.feed_handler import FeedHandler, FeedHandlerConfig
from trading.feed_handler.normalizers import BinanceNormalizer
from trading.order_gateways.binance import (
    BalanceReconciler,
    BinanceOrderGateway,
    BinancePublicWSConnector,
    BinanceRESTClient,
    BinanceUserDataStream,
    ListenKeyManager,
    StateBootstrapper,
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
    ThrottleRule,
)
from trading.strategy import StrategyRegistry
from trading.strategy.examples.avellaneda_stoikov import AvellanedaStoikovStrategy
from trading.config import load_settings
from trading.analytics import AnalyticsService
from trading.order_gateways.binance import BinanceL2Feed
from trading.monitoring import BusHeartbeat, DashboardServer, subscribe_event_logging
from trading.runners.examples._runner_config import load_runner_config

_STRATEGY_ID = StrategyId("avellaneda-stoikov")


async def _amain() -> int:
    log = structlog.get_logger("binance.testnet")

    # --- Config and creds ---------------------------------------------
    settings = load_settings()
    runner_cfg = load_runner_config(
        require_credentials=True,
        futures=settings.market == "futures",
    )
    config = runner_cfg.binance
    credentials = runner_cfg.credentials
    instruments = runner_cfg.instruments
    assert credentials is not None  # require_credentials=True guarantees this
    symbols = SymbolMapper(instruments)
    clock = LiveClock()

    # --- Bus ----------------------------------------------------------
    bus = AsyncioBus(queue_size=10_000)

    # --- REST client (shared by order_gateway, listen-key, user-data, reconciler)
    rest = BinanceRESTClient(config=config, credentials=credentials, clock=clock)

    # --- Core engines -------------------------------------------------
    position = PositionEngine(bus=bus, clock=clock, method=AccountingMethod.WAVG)
    risk = RiskEngine(bus=bus, clock=clock)
    # Sensible starter limits. Adjust before live, not before testnet.
    risk.register_global_rules([
        InstrumentAllowlistRule(allowed_instrument_ids=["BINANCE:BTC-USDT"]),
    ])
    risk.register_rules(_STRATEGY_ID, [
        # Change based on whether there's stale open orders on testnet
        # TODO: might need to build a cancel and replace order update flow to avoid this guard rejecting everything after the first few ticks
        MaxPositionRule(max_long=Decimal("0.01"), max_short=Decimal("0.01")),
        MaxOrderSizeRule(max_quantity=Decimal("0.002")),
        DailyLossLimitRule(max_loss=Decimal("50")),
    ])
    oms = OMSEngine(bus=bus, clock=clock)

    # --- Strategy -----------------------------------------------------
    portfolio = EnginePortfolioView(position)
    strategies = StrategyRegistry(bus=bus, clock=clock, portfolio=portfolio)
    strategies.register(
        AvellanedaStoikovStrategy(
            strategy_id=_STRATEGY_ID,
            instruments=instruments,
            gamma=0.3,
            k=1.5,
            tau_seconds=300.0,
            half_life_seconds=60.0,
            ofi_window_seconds=10.0,
            ofi_alpha=0.001,
            vpin_bucket_volume=0.001,
            vpin_threshold=0.7,
            vpin_widen_factor=3.0,
            quote_size=Decimal("0.002"),
            max_position=Decimal("0.01"),
            min_vol=0.5,             # 50% floor — BTC annual vol, stops sub-fee spreads
            min_price_move_ticks=2,  # only re-quote when price moves ≥2 ticks
        ),
    )

    # --- OrderGateway ------------------------------------------------------
    order_gateway = BinanceOrderGateway(
        bus=bus, clock=clock, config=config, credentials=credentials,
        symbols=symbols, rest_client=rest,
    )

    # --- User data stream + listen key --------------------------------
    listen_keys = ListenKeyManager(rest=rest, config=config)
    user_data = BinanceUserDataStream(
        bus=bus, clock=clock, config=config,
        listen_key_manager=listen_keys, symbols=symbols,
        strategy_id_lookup=oms.strategy_id_for_client_order,
    )

    # --- Public market data feed --------------------------------------
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
        bus=bus, clock=clock,
        instruments={symbols.wire_symbol(i): i for i in instruments},
        source="binance-public",
        config=FeedHandlerConfig(
            stale_threshold_seconds=30.0,
            max_reconnect_attempts=10,
        ),
    )

    # --- Reconciler ---------------------------------------------------
    reconciler = BalanceReconciler(
        bus=bus, clock=clock, config=config, rest=rest,
        position_engine=position, tracked_instruments=instruments,
        mismatch_threshold=Decimal("0.00001"),
    )

    # --- State bootstrap ----------------------------------------------
    # Adopts venue orders/positions at startup (recover mid-trade across a
    # restart) and reconciles periodically to repair user-data-stream gaps.
    state_bootstrap = StateBootstrapper(
        bus=bus, clock=clock, config=config, rest=rest,
        oms=oms, symbols=symbols, tracked_instruments=instruments,
    )

    # --- Dashboard ----------------------------------------------------
    # Pass position_engine so the REST /state/positions endpoint can read
    # live state directly from the engine (no event-bus replay needed).
    dashboard = (
        DashboardServer(
            bus=bus, port=settings.dashboard_port, position_engine=position,
        )
        if settings.dashboard_port > 0
        else None
    )

    # --- Analytics service --------------------------------------------
    analytics_service = AnalyticsService(bus=bus)
    l2_feed = BinanceL2Feed(
        bus=bus, config=config, rest=rest, symbols=symbols,
        instruments=instruments, clock=clock,
    )

    # --- Event logging ------------------------------------------------
    # Same subscriptions in every environment. Market data is intentionally
    # excluded — see heartbeat below for the "is data flowing?" signal.
    await subscribe_event_logging(bus, log)
    heartbeat = BusHeartbeat(bus=bus, log=log)

    # --- Start everything ---------------------------------------------
    log.info("starting_binance_testnet_pipeline")
    await position.start()
    await risk.start()
    await oms.start()
    await rest.connect()
    await order_gateway.start()
    await strategies.start()
    await listen_keys.start()
    await user_data.start()
    await bus.start()
    await analytics_service.start()
    await l2_feed.start()
    await heartbeat.start()
    # Dashboard must start before reconciler so it subscribes to Topic.ACCOUNT
    # before the reconciler's first reconcile_once() publishes a snapshot.
    if dashboard is not None:
        await dashboard.start()
    await reconciler.start()
    # Adopt venue state once the bus is running and all consumers (risk,
    # position, dashboard) are subscribed, but before market data drives
    # strategies — so a strategy's first tick sees the recovered state.
    await state_bootstrap.start()
    feed_task = asyncio.create_task(feed_handler.run(), name="binance-feed-handler")

    async def _emit_pnl_snapshots() -> None:
        while True:
            await asyncio.sleep(1.0)
            await position.mark_to_market_all()
    
    asyncio.create_task(_emit_pnl_snapshots(), name="pnl-snapshot-emitter")
            

    # --- Shutdown handling --------------------------------------------
    stop_event = asyncio.Event()

    def _on_signal() -> None:
        log.info("shutdown_signal_received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            pass  # Windows

    try:
        await stop_event.wait()
    finally:
        log.info("stopping_binance_testnet_pipeline")
        await heartbeat.stop()
        if dashboard is not None:
            await dashboard.stop()
        await analytics_service.stop()
        await l2_feed.stop()
        await feed_handler.stop()
        try:
            await asyncio.wait_for(feed_task, timeout=5)
        except (asyncio.TimeoutError, Exception):
            pass
        await state_bootstrap.stop()
        await reconciler.stop()
        await user_data.stop()
        await listen_keys.stop()
        await strategies.stop()
        await order_gateway.stop()
        await rest.close()
        await oms.stop()
        await risk.stop()
        await position.stop()
        await bus.stop()
        log.info("pipeline_stopped")
    return 0


def main() -> int:
    return asyncio.run(_amain())


if __name__ == "__main__":
    raise SystemExit(main())
