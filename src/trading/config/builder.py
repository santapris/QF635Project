"""Application builder.

Takes a validated :class:`AppConfig` and returns a wired-up
:class:`Application` ready to start. Centralises the constructor calls
that would otherwise be duplicated across runners.

Two builders:

- :func:`build_live_app` — production wiring: real bus, simulation or real
  order_gateway, strategy registry, full risk/OMS/position pipeline. The default
  for paper trading and (with real exchange adapters in place) live trading.

- :func:`build_backtest_app` — same components, plus the backtest engine
  driving everything from a :class:`SimulatedClock`.

The builders are explicit about *which* order_gateway flavour they use. Live
needs :class:`SimulationOrderGateway` (asyncio-sleep latency). Backtest needs
:class:`BacktestOrderGateway` (time-jumping). They share configuration.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from ..backtest import (
    BacktestConfig,
    BacktestEngine,
    BacktestOrderGateway,
    CSVColumns,
    CSVDataSource,
)
from ..core.clock import Clock, LiveClock, SimulatedClock
from ..core.exceptions import ConfigError
from ..core.instruments import Instrument
from ..event_bus import AsyncioBus, MemoryBus
from ..event_bus.base import AbstractEventBus
from ..order_gateways import (
    AbstractOrderGateway,
    FeeModel,
    FillModel,
    LatencyModel,
    RejectModel,
    SimulationOrderGateway,
    SimulationOrderGatewayConfig,
)
from ..order_gateways.binance.config import BinanceConfig, BinanceCredentials
from ..order_gateways.binance.order_gateway import BinanceOrderGateway
from ..order_gateways.binance.listen_key import ListenKeyManager
from ..order_gateways.binance.reconciler import BalanceReconciler
from ..order_gateways.binance.rest_client import BinanceRESTClient
from ..order_gateways.binance.symbols import SymbolMapper
from ..order_gateways.binance.user_data import BinanceUserDataStream
from ..oms import OMSEngine
from ..position import AccountingMethod, EnginePortfolioView, PositionEngine
from ..risk import RiskEngine
from ..risk.rules import (
    DailyLossLimitRule,
    InstrumentAllowlistRule,
    MaxNotionalRule,
    MaxOrderSizeRule,
    MaxPositionRule,
    ThrottleRule,
)
from ..strategy import StrategyRegistry
from ..strategy.examples import (
    MarketMakingStrategy,
    MeanReversionStrategy,
    MomentumStrategy,
)
from ..health import HealthServer
from .schema import (
    AppConfig,
    BinanceOrderGatewaySpec,
    BusBackend,
    BusConfig,
    RuleSpec,
    SimOrderGatewaySpec,
    StrategySpec,
)
from .settings import load_settings

_log = structlog.get_logger(__name__)


# --- Helpers ----------------------------------------------------------------

def _build_bus(cfg: BusConfig) -> AbstractEventBus:
    if cfg.backend is BusBackend.MEMORY:
        return MemoryBus()
    if cfg.backend is BusBackend.ASYNCIO:
        return AsyncioBus(queue_size=cfg.queue_size)
    if cfg.backend is BusBackend.KAFKA:
        if not cfg.bootstrap_servers or not cfg.client_id:
            raise ConfigError(
                "kafka bus requires bootstrap_servers and client_id"
            )
        # Lazy import: aiokafka is optional.
        from ..event_bus.kafka_bus import KafkaBus
        return KafkaBus(
            bootstrap_servers=cfg.bootstrap_servers,
            client_id=cfg.client_id,
            topic_prefix=cfg.topic_prefix,
        )
    raise ConfigError(f"unknown bus backend: {cfg.backend}")


def _instruments_by_id(cfg: AppConfig) -> dict[str, Instrument]:
    by_id: dict[str, Instrument] = {}
    for spec in cfg.instruments:
        inst = spec.to_instrument()
        by_id[inst.instrument_id] = inst
    return by_id


def _build_strategy(
    spec: StrategySpec, instruments: dict[str, Instrument]
) -> Any:
    try:
        insts = [instruments[i] for i in spec.instruments]
    except KeyError as e:
        raise ConfigError(
            f"strategy {spec.strategy_id} references unknown instrument {e}",
            strategy_id=spec.strategy_id,
        ) from e

    params = spec.parameters
    if spec.type == "momentum":
        return MomentumStrategy(
            strategy_id=spec.strategy_id, instruments=insts,
            fast_period=int(params.get("fast_period", "20")),
            slow_period=int(params.get("slow_period", "50")),
        )
    if spec.type == "mean_reversion":
        return MeanReversionStrategy(
            strategy_id=spec.strategy_id, instruments=insts,
            period=int(params.get("period", "20")),
            num_std=float(params.get("num_std", "2.0")),
        )
    if spec.type == "market_making":
        return MarketMakingStrategy(
            strategy_id=spec.strategy_id, instruments=insts,
            quote_size=Decimal(params.get("quote_size", "0.01")),
            target_spread_bps=float(params.get("target_spread_bps", "10")),
            max_position=Decimal(params.get("max_position", "0.5")),
            inventory_skew_bps=float(params.get("inventory_skew_bps", "5")),
        )
    raise ConfigError(f"unknown strategy type: {spec.type}", strategy_id=spec.strategy_id)


def _build_rule(spec: RuleSpec) -> Any:
    p = spec.params
    if spec.type == "max_position":
        return MaxPositionRule(
            max_long=Decimal(p["max_long"]),
            max_short=Decimal(p["max_short"]),
        )
    if spec.type == "max_order_size":
        return MaxOrderSizeRule(max_quantity=Decimal(p["max_quantity"]))
    if spec.type == "max_notional":
        return MaxNotionalRule(max_notional=Decimal(p["max_notional"]))
    if spec.type == "throttle":
        return ThrottleRule(
            max_signals=int(p["max_signals"]),
            window_seconds=float(p.get("window_seconds", "60")),
        )
    if spec.type == "daily_loss_limit":
        return DailyLossLimitRule(max_loss=Decimal(p["max_loss"]))
    if spec.type == "instrument_allowlist":
        ids = p.get("allowed_instrument_ids", "").split(",")
        return InstrumentAllowlistRule(
            allowed_instrument_ids=[i.strip() for i in ids if i.strip()]
        )
    raise ConfigError(f"unknown rule type: {spec.type}")


def _build_sim_order_gateway_config(spec: SimOrderGatewaySpec) -> SimulationOrderGatewayConfig:
    return SimulationOrderGatewayConfig(
        venue=spec.venue,
        fees=FeeModel(maker_bps=spec.maker_bps, taker_bps=spec.taker_bps),
        latency=LatencyModel(
            submit_ack_ms=spec.submit_ack_ms,
            cancel_ack_ms=spec.cancel_ack_ms,
            fill_ms=spec.fill_ms,
        ),
        fills=FillModel(
            partial_fill_probability=spec.partial_fill_probability,
            slippage_ticks=spec.slippage_ticks,
        ),
        rejects=RejectModel(),
        seed=spec.seed,
    )


def _build_binance_components(
    spec: BinanceOrderGatewaySpec,
    *,
    bus: AbstractEventBus,
    clock: Clock,
    instruments: dict[str, Instrument],
    oms: OMSEngine,
    pos: PositionEngine,
) -> tuple[BinanceOrderGateway, list]:
    """Build the Binance order_gateway plus its supporting services.

    Returns ``(order_gateway, services)`` where ``services`` is a list of objects
    with ``start()``/``stop()`` coroutines: [ListenKeyManager,
    BinanceUserDataStream, BalanceReconciler].  The caller is responsible
    for starting and stopping them in order.

    Credentials are read from environment variables at build time; the
    process must have the right env set before calling this.

    URLs are read from the environment-aware settings so that dev and prod
    can point at different endpoints without code changes.
    """
    settings = load_settings()
    cfg = BinanceConfig.from_settings(
        settings,
        reconcile_interval_seconds=spec.reconcile_interval_seconds,
    )
    if not settings.api_key or not settings.api_secret:
        raise ConfigError(
            "missing Binance API credentials; set BINANCE_API_KEY and "
            "BINANCE_API_SECRET in the environment or vault"
        )
    creds = BinanceCredentials(api_key=settings.api_key, api_secret=settings.api_secret)
    venue_insts = [i for i in instruments.values() if i.exchange == spec.venue]
    if not venue_insts:
        raise ConfigError(
            f"no instruments declared for venue {spec.venue!r}; "
            "add [[instruments]] entries with that exchange value",
            venue=spec.venue,
        )
    symbols = SymbolMapper(venue_insts)
    rest = BinanceRESTClient(config=cfg, credentials=creds, clock=clock)

    gw = BinanceOrderGateway(
        bus=bus, clock=clock, config=cfg,
        credentials=creds, symbols=symbols, rest_client=rest,
    )
    lkm = ListenKeyManager(rest=rest, config=cfg)
    uds = BinanceUserDataStream(
        bus=bus, clock=clock, config=cfg,
        listen_key_manager=lkm, symbols=symbols,
        strategy_id_lookup=oms.strategy_id_for_client_order,
    )
    reconciler = BalanceReconciler(
        bus=bus, clock=clock, config=cfg, rest=rest,
        position_engine=pos,
        tracked_instruments=venue_insts,
        mismatch_threshold=Decimal(spec.mismatch_threshold),
    )
    return gw, [lkm, uds, reconciler]


# --- Live wiring ------------------------------------------------------------


@dataclass(slots=True)
class LiveApp:
    """Wired-up live application. Caller manages start/stop."""

    clock: Clock
    bus: AbstractEventBus
    position_engine: PositionEngine
    risk_engine: RiskEngine
    oms_engine: OMSEngine
    strategy_registry: StrategyRegistry
    order_gateways: list[AbstractOrderGateway]
    # Venue-specific supporting services: ListenKeyManagers,
    # BinanceUserDataStreams, BalanceReconcilers.  Started after order_gateways
    # (which connect REST), stopped before order_gateways in reverse order.
    extra_services: list = field(default_factory=list)
    # Optional HTTP health/metrics server.  None means disabled.
    health_server: HealthServer | None = None

    async def start(self) -> None:
        await self.position_engine.start()
        await self.risk_engine.start()
        await self.oms_engine.start()
        # OrderGateways first: they open the REST connection that extra services need.
        for gw in self.order_gateways:
            await gw.start()
            # Immediately cancel any orders left over from a previous session.
            if isinstance(gw, BinanceOrderGateway):
                cancelled = await gw.cancel_stale_orders()
                if cancelled:
                    _log.warning(
                        "startup_cancelled_stale_orders",
                        num_cancelled=cancelled, venue=gw.venue,
                    )
        for svc in self.extra_services:
            await svc.start()
        await self.strategy_registry.start()
        # Bus last so subscribers exist before any traffic.
        if hasattr(self.bus, "start"):
            await self.bus.start()
        if self.health_server is not None:
            await self.health_server.start()

    async def stop(self) -> None:
        if self.health_server is not None:
            await self.health_server.stop()
        if hasattr(self.bus, "stop"):
            await self.bus.stop()
        await self.strategy_registry.stop()
        for svc in reversed(self.extra_services):
            await svc.stop()
        for gw in self.order_gateways:
            await gw.stop()
        await self.oms_engine.stop()
        await self.risk_engine.stop()
        await self.position_engine.stop()

    def metrics_snapshot(self) -> dict:
        """Collect a point-in-time metrics dict from all running engines."""
        snap: dict = {
            "oms": self.oms_engine.snapshot(),
            "risk": self.risk_engine.snapshot(),
            "position": self.position_engine.snapshot(),
            "order_gateways": [
                gw.snapshot() for gw in self.order_gateways if hasattr(gw, "snapshot")
            ],
        }
        return snap


def build_live_app(config: AppConfig) -> LiveApp:
    clock = LiveClock()
    bus = _build_bus(config.bus)
    instruments = _instruments_by_id(config)

    # Position engine.
    pos = PositionEngine(
        bus=bus, clock=clock,
        method=AccountingMethod(config.position.method),
    )

    # Risk engine + rules.
    risk = RiskEngine(bus=bus, clock=clock)
    risk.register_global_rules([_build_rule(r) for r in config.risk.global_rules])
    for strat_id, rules in config.risk.per_strategy.items():
        risk.register_rules(strat_id, [_build_rule(r) for r in rules])

    # OMS.
    oms = OMSEngine(
        bus=bus, clock=clock,
        signal_ttl_seconds=config.oms.signal_ttl_seconds,
    )

    # OrderGateways. One order_gateway per declared venue spec.
    order_gateways: list[AbstractOrderGateway] = []
    extra_services: list = []
    for gw_spec in config.order_gateways:
        if isinstance(gw_spec, SimOrderGatewaySpec):
            order_gateways.append(
                SimulationOrderGateway(
                    bus=bus, clock=clock,
                    config=_build_sim_order_gateway_config(gw_spec),
                )
            )
        elif isinstance(gw_spec, BinanceOrderGatewaySpec):
            binance_gw, services = _build_binance_components(
                gw_spec, bus=bus, clock=clock,
                instruments=instruments, oms=oms, pos=pos,
            )
            order_gateways.append(binance_gw)
            extra_services.extend(services)

    # Strategy registry — gets a portfolio view backed by the position engine.
    portfolio = EnginePortfolioView(pos)
    registry = StrategyRegistry(bus=bus, clock=clock, portfolio=portfolio)
    for s in config.strategies:
        if not s.enabled:
            continue
        registry.register(
            _build_strategy(s, instruments),
            parameters=dict(s.parameters),
        )

    return LiveApp(
        clock=clock, bus=bus,
        position_engine=pos, risk_engine=risk, oms_engine=oms,
        strategy_registry=registry, order_gateways=order_gateways,
        extra_services=extra_services,
    )


# --- Backtest wiring --------------------------------------------------------


@dataclass(slots=True)
class BacktestApp:
    """Wired-up backtest application."""

    clock: SimulatedClock
    bus: AsyncioBus
    position_engine: PositionEngine
    risk_engine: RiskEngine
    oms_engine: OMSEngine
    strategy_registry: StrategyRegistry
    order_gateway: BacktestOrderGateway
    engine: BacktestEngine

    async def run(self) -> Any:
        # Start everything in order; the engine takes it from there.
        await self.position_engine.start()
        await self.risk_engine.start()
        await self.oms_engine.start()
        await self.order_gateway.start()
        await self.strategy_registry.start()
        return await self.engine.run()


def build_backtest_app(config: AppConfig) -> BacktestApp:
    if config.backtest is None:
        raise ConfigError("backtest section required for backtest run")
    if len(config.order_gateways) != 1:
        raise ConfigError(
            "backtest currently supports exactly one order_gateway",
            num_order_gateways=len(config.order_gateways),
        )

    clock = SimulatedClock(start=0)
    bus = AsyncioBus(queue_size=config.bus.queue_size)
    instruments = _instruments_by_id(config)

    if config.backtest.instrument_id not in instruments:
        raise ConfigError(
            f"backtest instrument {config.backtest.instrument_id} "
            "not declared in [[instruments]]",
        )

    pos = PositionEngine(
        bus=bus, clock=clock,
        method=AccountingMethod(config.position.method),
    )
    risk = RiskEngine(bus=bus, clock=clock)
    risk.register_global_rules([_build_rule(r) for r in config.risk.global_rules])
    for strat_id, rules in config.risk.per_strategy.items():
        risk.register_rules(strat_id, [_build_rule(r) for r in rules])
    oms = OMSEngine(
        bus=bus, clock=clock,
        signal_ttl_seconds=config.oms.signal_ttl_seconds,
    )

    gw_spec = config.order_gateways[0]
    order_gateway = BacktestOrderGateway(
        bus=bus, clock=clock,
        config=_build_sim_order_gateway_config(gw_spec),
    )

    portfolio = EnginePortfolioView(pos)
    registry = StrategyRegistry(bus=bus, clock=clock, portfolio=portfolio)
    for s in config.strategies:
        if not s.enabled:
            continue
        registry.register(
            _build_strategy(s, instruments),
            parameters=dict(s.parameters),
        )

    # Data source.
    instrument = instruments[config.backtest.instrument_id]
    data_source = CSVDataSource(
        path=config.backtest.data_path,
        instrument=instrument,
        columns=CSVColumns(timestamp_unit=config.backtest.timestamp_unit),
    )

    engine = BacktestEngine(
        clock=clock, bus=bus,
        data_source=data_source, order_gateway=order_gateway,
        position_engine=pos,
        config=BacktestConfig(
            snapshot_interval_seconds=config.backtest.snapshot_interval_seconds,
            initial_equity=config.backtest.initial_equity,
            periods_per_year=config.backtest.periods_per_year,
        ),
    )

    return BacktestApp(
        clock=clock, bus=bus,
        position_engine=pos, risk_engine=risk, oms_engine=oms,
        strategy_registry=registry, order_gateway=order_gateway, engine=engine,
    )


__all__ = ["BacktestApp", "LiveApp", "build_backtest_app", "build_live_app"]
