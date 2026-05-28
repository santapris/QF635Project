"""Application builder.

Takes a validated :class:`AppConfig` and returns a wired-up application
ready to start. Component construction is delegated to the plugin
registries in :mod:`trading.plugins`; nothing in this file names a
specific exchange, strategy, or risk rule.
"""

from __future__ import annotations

import structlog
from dataclasses import dataclass, field
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
from ..order_gateways import AbstractOrderGateway
from ..order_gateways.simulation_plugin import sim_config_from_params
from ..oms import OMSEngine
from ..plugins import (
    BuildContext,
    gateway_registry,
    rule_registry,
    strategy_registry,
)
from ..position import AccountingMethod, EnginePortfolioView, PositionEngine
from ..risk import RiskEngine
from ..strategy import StrategyRegistry
from ..health import HealthServer
from ..monitoring import DashboardServer
from .schema import (
    AppConfig,
    BusBackend,
    BusConfig,
    GatewaySpec,
    RuleSpec,
    StrategySpec,
)

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


def _build_strategy(spec: StrategySpec, ctx: BuildContext) -> Any:
    plugin = strategy_registry.get(spec.type)
    try:
        params = plugin.Params.model_validate(spec.parameters)
    except Exception as e:
        raise ConfigError(
            f"invalid parameters for strategy {spec.strategy_id} ({spec.type}): {e}",
            strategy_id=spec.strategy_id,
        ) from e

    try:
        insts = [ctx.instruments[i] for i in spec.instruments]
    except KeyError as e:
        raise ConfigError(
            f"strategy {spec.strategy_id} references unknown instrument {e}",
            strategy_id=spec.strategy_id,
        ) from e

    return plugin.build(params, ctx, strategy_id=spec.strategy_id, instruments=insts)


def _build_rule(spec: RuleSpec) -> Any:
    plugin = rule_registry.get(spec.type)
    try:
        params = plugin.Params.model_validate(spec.params)
    except Exception as e:
        raise ConfigError(
            f"invalid parameters for rule {spec.type}: {e}",
        ) from e
    return plugin.build(params)


def _build_gateway(
    spec: GatewaySpec, ctx: BuildContext
) -> tuple[AbstractOrderGateway, list[Any]]:
    plugin = gateway_registry.get(spec.type)
    try:
        params = plugin.Params.model_validate(spec.params)
    except Exception as e:
        raise ConfigError(
            f"invalid parameters for gateway {spec.venue} ({spec.type}): {e}",
            venue=spec.venue,
        ) from e
    return plugin.build(params, ctx, venue=spec.venue)


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
    extra_services: list = field(default_factory=list)
    health_server: HealthServer | None = None
    dashboard_server: DashboardServer | None = None
    # Policy: adopt pre-existing venue orders at startup rather than wiping
    # them. Only cancel-on-start when explicitly configured (start-from-flat).
    cancel_stale_orders_on_start: bool = False

    async def start(self) -> None:
        await self.position_engine.start()
        await self.risk_engine.start()
        await self.oms_engine.start()
        for gw in self.order_gateways:
            await gw.start()
            if self.cancel_stale_orders_on_start:
                cancelled = await gw.cancel_stale_orders()
                if cancelled:
                    _log.warning(
                        "startup_cancelled_stale_orders",
                        num_cancelled=cancelled, venue=gw.venue,
                    )
        for svc in self.extra_services:
            await svc.start()
        await self.strategy_registry.start()
        if hasattr(self.bus, "start"):
            await self.bus.start()
        if self.health_server is not None:
            await self.health_server.start()
        if self.dashboard_server is not None:
            await self.dashboard_server.start()

    async def stop(self) -> None:
        if self.dashboard_server is not None:
            await self.dashboard_server.stop()
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
        return {
            "oms": self.oms_engine.snapshot(),
            "risk": self.risk_engine.snapshot(),
            "position": self.position_engine.snapshot(),
            "order_gateways": [
                gw.snapshot() for gw in self.order_gateways if hasattr(gw, "snapshot")
            ],
        }


def build_live_app(config: AppConfig) -> LiveApp:
    clock = LiveClock()
    bus = _build_bus(config.bus)
    instruments = _instruments_by_id(config)

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
        algo_driver_interval_seconds=config.oms.algo_driver_interval_seconds,
    )

    ctx = BuildContext(
        bus=bus, clock=clock, instruments=instruments, oms=oms, position=pos,
    )

    order_gateways: list[AbstractOrderGateway] = []
    extra_services: list = []
    for gw_spec in config.order_gateways:
        gw, services = _build_gateway(gw_spec, ctx)
        order_gateways.append(gw)
        extra_services.extend(services)

    portfolio = EnginePortfolioView(pos)
    registry = StrategyRegistry(bus=bus, clock=clock, portfolio=portfolio)
    for s in config.strategies:
        if not s.enabled:
            continue
        registry.register(
            _build_strategy(s, ctx),
            parameters=dict(s.parameters),
        )

    return LiveApp(
        clock=clock, bus=bus,
        position_engine=pos, risk_engine=risk, oms_engine=oms,
        strategy_registry=registry, order_gateways=order_gateways,
        extra_services=extra_services,
        cancel_stale_orders_on_start=config.oms.cancel_stale_orders_on_start,
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

    gw_spec = config.order_gateways[0]
    if gw_spec.type not in ("simulation", "backtest"):
        raise ConfigError(
            f"backtest order_gateway must be 'simulation' or 'backtest', got {gw_spec.type!r}",
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
        algo_driver_interval_seconds=config.oms.algo_driver_interval_seconds,
    )

    sim_cfg = sim_config_from_params(gw_spec.params, gw_spec.venue)
    order_gateway = BacktestOrderGateway(bus=bus, clock=clock, config=sim_cfg)

    ctx = BuildContext(
        bus=bus, clock=clock, instruments=instruments, oms=oms, position=pos,
    )

    portfolio = EnginePortfolioView(pos)
    registry = StrategyRegistry(bus=bus, clock=clock, portfolio=portfolio)
    for s in config.strategies:
        if not s.enabled:
            continue
        registry.register(
            _build_strategy(s, ctx),
            parameters=dict(s.parameters),
        )

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
