"""Run the full Binance Futures testnet pipeline with five MM strategies.

End-to-end wiring:
- Feed handler: public WS connector (bookTicker + aggTrade streams).
- Risk engine + per-strategy rules.
- OMS.
- Position engine.
- :class:`BinanceOrderGateway` for order entry/cancel.
- :class:`ListenKeyManager` + :class:`BinanceUserDataStream` for the
  fill feedback loop.
- :class:`BalanceReconciler` for the safety net.
- Five market-making strategies quoting the *same* instrument (BTC-USDT):
  :class:`AvellanedaStoikovStrategy`, :class:`GLFTStrategy`,
  :class:`GridStrategy`, :class:`MicropriceMMStrategy`, and
  :class:`OBIAlphaStrategy`. They are given differentiated spreads so they
  rest at distinct levels.

Internal netting: the venue is a single futures account, so the exchange
already nets every fill into one net position; the per-strategy books are
attribution only. The hazard of running 5 MM strategies on one book is that
their quotes can cross *each other* (one strategy's bid resting above
another's ask) and wash-trade — paying fees to shuffle inventory between
internal books for zero firm benefit. The OMS's self-trade-prevention (STP,
enabled explicitly below) is the internal-netting guard: it holds back any
leg that would cross a sibling strategy's live resting order. Watch for
``self_trade_prevented`` log lines — they confirm netting is active.

To run:
    python -m trading.runners.examples.binance_testnet

This is a long-running process — Ctrl-C to stop. Watch for POST_ONLY
orders appearing in the logs within the first few ticks. If you see
-2010 (cross) rejects, the post_only_guard is misfiring — investigate
before running longer.
"""

from __future__ import annotations

import asyncio
import gc
import signal
from decimal import Decimal

import structlog

from trading.analytics import AnalyticsService
from trading.config import load_settings
from trading.core import LiveClock, StrategyId
from trading.event_bus import AsyncioBus
from trading.feed_handler import FeedHandler, FeedHandlerConfig
from trading.feed_handler.normalizers import BinanceNormalizer
from trading.monitoring import BusHeartbeat, DashboardServer, subscribe_event_logging
from trading.oms import OMSEngine
from trading.order_gateways.binance import (
    BalanceReconciler,
    BinanceL2Feed,
    BinanceOrderGateway,
    BinancePublicWSConnector,
    BinanceRESTClient,
    BinanceUserDataStream,
    ListenKeyManager,
    StateBootstrapper,
    SymbolMapper,
    stream_names,
)
from trading.position import AccountingMethod, EnginePortfolioView, PositionEngine
from trading.risk import RiskEngine
from trading.risk.rules import (
    DailyLossLimitRule,
    InstrumentAllowlistRule,
    MaxOrderSizeRule,
    MaxPositionRule,
)
from trading.runners.examples._runner_config import load_runner_config
from trading.strategy import StrategyRegistry
from trading.strategy.examples.avellaneda_stoikov import AvellanedaStoikovStrategy
from trading.strategy.examples.glft import GLFTStrategy
from trading.strategy.examples.grid import GridStrategy
from trading.strategy.examples.microprice_mm import MicropriceMMStrategy
from trading.strategy.examples.obi_alpha import OBIAlphaStrategy
from trading.config import load_settings
from trading.analytics import AnalyticsService
from trading.order_gateways.binance import BinanceL2Feed
from trading.monitoring import BusHeartbeat, DashboardServer, LatencyCollector, subscribe_event_logging
from trading.runners.examples._runner_config import load_runner_config

# Five MM strategies, all quoting the same instrument (BTC-USDT). Distinct
# ids keep their orders, fills, and P&L attributed separately downstream.
_AS_ID = StrategyId("as-mm")
_GLFT_ID = StrategyId("glft-mm")
_GRID_ID = StrategyId("grid-mm")
_MICRO_ID = StrategyId("micro-mm")
_OBI_ID = StrategyId("obi-mm")
_ALL_STRATEGY_IDS = (_AS_ID, _GLFT_ID, _GRID_ID, _MICRO_ID, _OBI_ID)


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
    # Per-strategy caps for all five strategies. These rules are scoped per
    # (strategy, instrument) — a strategy with no rules is bounded only by the
    # global allowlist (i.e. effectively unbounded size), so registering them
    # for every strategy is required, not optional.
    #
    # The position cap must fit each strategy's *per-side ladder*:
    # MaxPositionRule sums same-side legs within one signal, so a strategy
    # quoting N levels of `quote_size` needs a cap >= N * quote_size or its
    # whole ladder is rejected. micro/obi/as quote one level (0.01 → 0.02 cap),
    # glft two (0.02 → 0.02 cap), grid three (0.03 → 0.03 cap).
    #
    # Aggregate worst-case net = 4 × 0.02 + 0.03 = 0.11 BTC (~$7k notional) and
    # aggregate daily-loss = 5 × $50 = $250 — acceptable on testnet. A firm-level
    # *net* cap across strategies (RiskState is per-strategy today) is the
    # production follow-up.
    _MAX_POS = {
        _MICRO_ID: Decimal("0.02"),
        _OBI_ID: Decimal("0.02"),
        _AS_ID: Decimal("0.02"),
        _GLFT_ID: Decimal("0.02"),
        _GRID_ID: Decimal("0.03"),
    }
    for sid in _ALL_STRATEGY_IDS:
        cap = _MAX_POS[sid]
        risk.register_rules(sid, [
            MaxPositionRule(max_long=cap, max_short=cap),
            MaxOrderSizeRule(max_quantity=Decimal("0.01")),
            DailyLossLimitRule(max_loss=Decimal("50")),
        ])
    # self_trade_prevention=True is the internal-netting guard: when these five
    # strategies quote the same book, it holds back any leg that would cross a
    # sibling strategy's resting order, so they never wash-trade against each
    # other. On by default; set explicitly here to make the intent visible.
    oms = OMSEngine(bus=bus, clock=clock, self_trade_prevention=True)

    # --- Strategies ---------------------------------------------------
    # All five quote the same BTC-USDT book with *differentiated* spreads so
    # they rest at distinct levels (tightest → widest below). The OMS's
    # self-trade-prevention nets away any genuine cross between them. Requote
    # gates are deliberately conservative: five strategies amending one book can
    # otherwise hit Binance's per-order modify limit (-5026).
    portfolio = EnginePortfolioView(position)
    strategies = StrategyRegistry(bus=bus, clock=clock, portfolio=portfolio)

    # Tightest — microprice-anchored, quotes near the touch.
    strategies.register(
        MicropriceMMStrategy(
            strategy_id=_MICRO_ID,
            instruments=instruments,
            quote_size=Decimal("0.01"),
            target_spread_bps=1.5,
            max_position=Decimal("0.02"),
            inventory_skew_bps=1.0,
            min_quote_interval_s=0.5,    # requote gate: both interval AND price
            requote_threshold_bps=0.5,   # move must trip, so keep them small or
                                         # the strategy goes dormant in a calm book
        ),
    )
    # OBI/OFI alpha-tilted, slightly wider than microprice.
    strategies.register(
        OBIAlphaStrategy(
            strategy_id=_OBI_ID,
            instruments=instruments,
            quote_size=Decimal("0.01"),
            target_spread_bps=2.5,
            max_position=Decimal("0.02"),
            inventory_skew_bps=1.0,
            obi_alpha=0.0005,
            ofi_alpha=0.0003,
            ofi_window_seconds=10.0,
            min_price_move_ticks=10,
        ),
    )
    # Avellaneda-Stoikov — vol/inventory-driven spread (medium).
    strategies.register(
        AvellanedaStoikovStrategy(
            strategy_id=_AS_ID,
            instruments=instruments,
            gamma=0.3,
            k=1.5,
            tau_seconds=2.0,
            half_life_seconds=60.0,
            ofi_window_seconds=10.0,
            ofi_alpha=0.001,
            vpin_bucket_volume=0.001,
            vpin_threshold=0.7,
            vpin_widen_factor=3.0,
            quote_size=Decimal("0.01"),
            max_position=Decimal("0.02"),
            min_vol=0.5,             # 50% floor — BTC annual vol, stops sub-fee spreads
            min_price_move_ticks=10,  # only re-quote on a ≥10-tick (~1 USD)
                                      # move. Caps amend rate so a single resting
                                      # order stays under Binance's per-order
                                      # modify limit (-5026); also lengthens
                                      # queue position. ~20% of the half-spread.
        ),
    )
    # GLFT — closed-form optimal MM, two-level ladder (wider).
    strategies.register(
        GLFTStrategy(
            strategy_id=_GLFT_ID,
            instruments=instruments,
            gamma=0.2,
            k=1.5,
            A=140.0,
            half_life_seconds=60.0,
            ofi_window_seconds=10.0,
            ofi_alpha=0.001,
            quote_size=Decimal("0.01"),
            max_position=Decimal("0.02"),
            min_vol=0.5,
            min_price_move_ticks=10,
            n_levels=2,
            grid_step_bps=4.0,
        ),
    )
    # Grid — fixed-step ladder around microprice (widest).
    strategies.register(
        GridStrategy(
            strategy_id=_GRID_ID,
            instruments=instruments,
            quote_size=Decimal("0.01"),
            n_levels=3,
            grid_step_bps=6.0,
            max_position=Decimal("0.03"),  # 3 levels × 0.01 = 0.03; must match
                                           # the MaxPositionRule cap for grid-mm
                                           # or the whole ladder is risk-rejected
            inventory_skew_bps=2.0,
            use_microprice=True,
            min_quote_interval_s=0.5,
            requote_threshold_bps=1.0,
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

    # --- Latency collector --------------------------------------------
    latency_collector = LatencyCollector(
        bus=bus,
        signal_tick_map=strategies.signal_tick_map,
        window=200,
    )

    # --- Dashboard ----------------------------------------------------
    # Pass position_engine so the REST /state/positions endpoint can read
    # live state directly from the engine (no event-bus replay needed).
    dashboard = (
        DashboardServer(
            bus=bus, port=settings.dashboard_port, position_engine=position,
            latency_collector=latency_collector,
            risk_engine=risk,
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
    await latency_collector.start()
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
        await latency_collector.stop()
        if dashboard is not None:
            await dashboard.stop()
        await analytics_service.stop()
        await l2_feed.stop()
        await feed_handler.stop()
        try:
            await asyncio.wait_for(feed_task, timeout=5)
        except (TimeoutError, Exception):
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
    gc.collect()
    gc.freeze()
    return asyncio.run(_amain())


if __name__ == "__main__":
    raise SystemExit(main())
