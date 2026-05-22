"""Application builder.

Takes a validated :class:`AppConfig` and returns a wired-up
:class:`Application` ready to start. Centralises the constructor calls
that would otherwise be duplicated across runners.

Two builders:

- :func:`build_live_app` — production wiring: real bus, simulation or real
  gateway, strategy registry, full risk/OMS/position pipeline. The default
  for paper trading and (with real exchange adapters in place) live trading.

- :func:`build_backtest_app` — same components, plus the backtest engine
  driving everything from a :class:`SimulatedClock`.

The builders are explicit about *which* gateway flavour they use. Live
needs :class:`SimulationGateway` (asyncio-sleep latency). Backtest needs
:class:`BacktestGateway` (time-jumping). They share configuration.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from ..backtest import (
    BacktestConfig,
    BacktestEngine,
    BacktestGateway,
    CSVColumns,
    CSVDataSource,
)
from ..core.clock import Clock, LiveClock, SimulatedClock
from ..core.exceptions import ConfigError
from ..core.instruments import Instrument
from ..event_bus import AsyncioBus, MemoryBus
from ..event_bus.base import AbstractEventBus
from ..gateways import (
    FeeModel,
    FillModel,
    LatencyModel,
    RejectModel,
    SimulationGateway,
    SimulationGatewayConfig,
)
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
from .schema import (
    AppConfig,
    BusBackend,
    BusConfig,
    GatewaySpec,
    RuleSpec,
    StrategySpec,
)


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


def _build_sim_gateway_config(spec: GatewaySpec) -> SimulationGatewayConfig:
    return SimulationGatewayConfig(
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
    gateways: list[SimulationGateway]

    async def start(self) -> None:
        await self.position_engine.start()
        await self.risk_engine.start()
        await self.oms_engine.start()
        for gw in self.gateways:
            await gw.start()
        await self.strategy_registry.start()
        # Bus last so subscribers exist before any traffic.
        if hasattr(self.bus, "start"):
            await self.bus.start()

    async def stop(self) -> None:
        if hasattr(self.bus, "stop"):
            await self.bus.stop()
        await self.strategy_registry.stop()
        for gw in self.gateways:
            await gw.stop()
        await self.oms_engine.stop()
        await self.risk_engine.stop()
        await self.position_engine.stop()


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

    # Gateways. One SimulationGateway per declared venue.
    gateways: list[SimulationGateway] = []
    for gw_spec in config.gateways:
        if gw_spec.type != "simulation":
            raise ConfigError(
                f"live app supports simulation gateways only here; got {gw_spec.type}",
                venue=gw_spec.venue,
            )
        gateways.append(
            SimulationGateway(
                bus=bus, clock=clock,
                config=_build_sim_gateway_config(gw_spec),
            )
        )

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
        strategy_registry=registry, gateways=gateways,
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
    gateway: BacktestGateway
    engine: BacktestEngine

    async def run(self) -> Any:
        # Start everything in order; the engine takes it from there.
        await self.position_engine.start()
        await self.risk_engine.start()
        await self.oms_engine.start()
        await self.gateway.start()
        await self.strategy_registry.start()
        return await self.engine.run()


def build_backtest_app(config: AppConfig) -> BacktestApp:
    if config.backtest is None:
        raise ConfigError("backtest section required for backtest run")
    if len(config.gateways) != 1:
        raise ConfigError(
            "backtest currently supports exactly one gateway",
            num_gateways=len(config.gateways),
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

    gw_spec = config.gateways[0]
    gateway = BacktestGateway(
        bus=bus, clock=clock,
        config=_build_sim_gateway_config(gw_spec),
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
        data_source=data_source, gateway=gateway,
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
        strategy_registry=registry, gateway=gateway, engine=engine,
    )


__all__ = ["BacktestApp", "LiveApp", "build_backtest_app", "build_live_app"]
