"""Run the full Binance Spot testnet pipeline.

End-to-end wiring:
- Feed handler: public WS connector + reuses the existing
  :class:`BinanceNormalizer` from the core platform for ticker/trade
  streams.
- Risk engine + per-strategy rules.
- OMS.
- Position engine.
- :class:`BinanceGateway` for order entry/cancel.
- :class:`ListenKeyManager` + :class:`BinanceUserDataStream` for the
  fill feedback loop.
- :class:`BalanceReconciler` for the safety net.
- A simple momentum strategy on BTC-USDT for demonstration.

To run:

    export BINANCE_TESTNET_API_KEY=<your testnet key>
    export BINANCE_TESTNET_API_SECRET=<your testnet secret>
    python -m trading.runners.run_binance_testnet

Get testnet keys at https://testnet.binance.vision/

This is a long-running process — Ctrl-C to stop. The first run should
fire signals when EMA fast/slow cross; you'll see ack/fill events in
the logs. Watch closely for the first few orders. If anything looks
off (signature errors, weird symbols, unfamiliar message types) stop
immediately and investigate before adding more capital.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from decimal import Decimal

from trading.core import AssetType, Instrument, LiveClock, StrategyId
from trading.event_bus import AsyncioBus, Topic
from trading.feed_handler import FeedHandler, FeedHandlerConfig
from trading.feed_handler.normalizers import BinanceNormalizer
from trading.gateways.binance import (
    BalanceReconciler,
    BinanceConfig,
    BinanceCredentials,
    BinanceGateway,
    BinancePublicWSConnector,
    BinanceRESTClient,
    BinanceUserDataStream,
    ListenKeyManager,
    SymbolMapper,
)
from trading.gateways.binance import stream_names
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
from trading.strategy.examples import MomentumStrategy


def _build_instruments() -> list[Instrument]:
    """The set of instruments this run cares about.

    For more, add to this list and update the feed-handler streams.
    """
    return [
        Instrument(
            symbol="BTC-USDT",
            exchange="BINANCE",
            asset_type=AssetType.FUTURES,
            base_currency="BTC",
            quote_currency="USDT",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.00001"),
            min_notional=Decimal("10"),  # Binance testnet typical minimum
        ),
    ]


async def _amain(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("binance.testnet")

    # --- Config and creds ---------------------------------------------
    config = BinanceConfig(testnet=True, futures=True)
    credentials = BinanceCredentials.from_env(testnet=True)

    instruments = _build_instruments()
    symbols = SymbolMapper(instruments)
    clock = LiveClock()

    # --- Bus ----------------------------------------------------------
    bus = AsyncioBus(queue_size=10_000)

    # --- REST client (shared by gateway, listen-key, user-data, reconciler)
    rest = BinanceRESTClient(config=config, credentials=credentials, clock=clock)

    # --- Core engines -------------------------------------------------
    position = PositionEngine(bus=bus, clock=clock, method=AccountingMethod.WAVG)
    risk = RiskEngine(bus=bus, clock=clock)
    # Sensible starter limits. Adjust before live, not before testnet.
    risk.register_global_rules([
        InstrumentAllowlistRule(allowed_instrument_ids=["BINANCE:BTC-USDT"]),
    ])
    risk.register_rules(StrategyId("momentum"), [
        MaxPositionRule(max_long=Decimal("0.001"), max_short=Decimal("0.001")),
        MaxOrderSizeRule(max_quantity=Decimal("0.001")),
        DailyLossLimitRule(max_loss=Decimal("50")),
    ])
    oms = OMSEngine(bus=bus, clock=clock)

    # --- Strategy -----------------------------------------------------
    portfolio = EnginePortfolioView(position)
    strategies = StrategyRegistry(bus=bus, clock=clock, portfolio=portfolio)
    strategies.register(
        MomentumStrategy(
            strategy_id=StrategyId("momentum"),
            instruments=instruments,
            fast_period=20,
            slow_period=50,
        ),
        parameters={"target_quantity": "0.0001"},  # very small for testnet
    )

    # --- Gateway ------------------------------------------------------
    gateway = BinanceGateway(
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

    # --- Start everything ---------------------------------------------
    log.info("starting binance testnet pipeline")
    await position.start()
    await risk.start()
    await oms.start()
    await rest.connect()
    await gateway.start()
    await strategies.start()
    await listen_keys.start()
    await user_data.start()
    await reconciler.start()
    await bus.start()
    feed_task = asyncio.create_task(feed_handler.run(), name="binance-feed-handler")

    # --- Shutdown handling --------------------------------------------
    stop_event = asyncio.Event()

    def _on_signal() -> None:
        log.info("shutdown signal received")
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
        log.info("stopping binance testnet pipeline")
        await feed_handler.stop()
        try:
            await asyncio.wait_for(feed_task, timeout=5)
        except (asyncio.TimeoutError, Exception):
            pass
        await reconciler.stop()
        await user_data.stop()
        await listen_keys.stop()
        await strategies.stop()
        await gateway.stop()
        await rest.close()
        await oms.stop()
        await risk.stop()
        await position.stop()
        await bus.stop()
        log.info("pipeline stopped")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a Binance Spot testnet trading session."
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
