"""Run the full Binance Spot testnet pipeline.

End-to-end wiring:
- Feed handler: public WS connector + reuses the existing
  :class:`BinanceNormalizer` from the core platform for ticker/trade
  streams.
- Risk engine + per-strategy rules.
- OMS.
- Position engine.
- :class:`BinanceOrderGateway` for order entry/cancel.
- :class:`ListenKeyManager` + :class:`BinanceUserDataStream` for the
  fill feedback loop.
- :class:`BalanceReconciler` for the safety net.
- A market-making strategy on BTC-USDT: quotes both sides around the
  mid with inventory skew to keep the book balanced.

To run:
    python -m trading.runners.examples.binance_testnet

This is a long-running process — Ctrl-C to stop. The strategy emits
POST_ONLY GTC quotes on every tick; you'll see ack/fill events in the
logs as the spread is crossed. Watch closely for the first few orders.
If anything looks off (signature errors, weird symbols, unfamiliar
message types) stop immediately and investigate before adding more
capital.

Strategy parameters (adjust before going live):
- quote_size:          0.01 BTC   (~$1000 notional at BTC ~$100k)
- target_spread_bps:   5 bps      (0.05% tight spread to attract more fills)
- max_position:        0.05 BTC   (5× quote size; then that side withdraws)
- inventory_skew_bps:  15 bps     (aggressive skew to burn down inventory fast)
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
from trading.strategy.examples import MarketMakingStrategy
from trading.config import load_settings
from trading.monitoring import BusHeartbeat, DashboardServer, subscribe_event_logging
from trading.runners.examples._runner_config import load_runner_config


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
    risk.register_rules(StrategyId("market-maker"), [
        # max_position must be >= MarketMakingStrategy.max_position so the
        # risk engine never blocks quotes that are within the strategy's own cap.
        MaxPositionRule(max_long=Decimal("0.05"), max_short=Decimal("0.05")),
        MaxOrderSizeRule(max_quantity=Decimal("0.01")),     # one quote_size at a time
        DailyLossLimitRule(max_loss=Decimal("200")),
        ThrottleRule(max_signals=10, window_seconds=1.0),  # cap at 10 signals/s (5 ticks × 2 sides)
    ])
    oms = OMSEngine(bus=bus, clock=clock)

    # --- Strategy -----------------------------------------------------
    portfolio = EnginePortfolioView(position)
    strategies = StrategyRegistry(bus=bus, clock=clock, portfolio=portfolio)
    strategies.register(
        MarketMakingStrategy(
            strategy_id=StrategyId("market-maker"),
            instruments=instruments,
            quote_size=Decimal("0.01"),         # ~$1000 notional at BTC ~$100k
            target_spread_bps=5.0,              # tight 0.05% spread to attract more fills
            max_position=Decimal("0.05"),       # hold up to 5× quote_size before withdrawing a side
            inventory_skew_bps=15.0,            # aggressive skew to burn down inventory fast
            min_quote_interval_s=1.0,           # at most one requote per second
            requote_threshold_bps=2.0,          # skip if mid moved < 0.02% since last quote
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
    await heartbeat.start()
    # Dashboard must start before reconciler so it subscribes to Topic.ACCOUNT
    # before the reconciler's first reconcile_once() publishes a snapshot.
    if dashboard is not None:
        await dashboard.start()
    await reconciler.start()
    feed_task = asyncio.create_task(feed_handler.run(), name="binance-feed-handler")

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
        await feed_handler.stop()
        try:
            await asyncio.wait_for(feed_task, timeout=5)
        except (asyncio.TimeoutError, Exception):
            pass
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
