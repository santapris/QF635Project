"""Plugin registrations for the simulation and backtest order gateways."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from ..plugins import gateway_registry
from .sim_config import (
    FeeModel,
    FillModel,
    LatencyModel,
    RejectModel,
    SimulationOrderGatewayConfig,
)
from .simulation import SimulationOrderGateway


class _SimParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    maker_bps: float = 1.0
    taker_bps: float = 5.0
    submit_ack_ms: float = 5.0
    cancel_ack_ms: float = 5.0
    fill_ms: float = 10.0
    slippage_ticks: int = 0
    partial_fill_probability: float = 0.0
    seed: int | None = None


def _build_sim_config(params: _SimParams, venue: str) -> SimulationOrderGatewayConfig:
    return SimulationOrderGatewayConfig(
        venue=venue,
        fees=FeeModel(maker_bps=params.maker_bps, taker_bps=params.taker_bps),
        latency=LatencyModel(
            submit_ack_ms=params.submit_ack_ms,
            cancel_ack_ms=params.cancel_ack_ms,
            fill_ms=params.fill_ms,
        ),
        fills=FillModel(
            partial_fill_probability=params.partial_fill_probability,
            slippage_ticks=params.slippage_ticks,
        ),
        rejects=RejectModel(),
        seed=params.seed,
    )


class _SimulationPlugin:
    Params = _SimParams

    def build(self, params, ctx, *, venue):
        gw = SimulationOrderGateway(
            bus=ctx.bus, clock=ctx.clock,
            config=_build_sim_config(params, venue),
        )
        return gw, []


class _BacktestPlugin:
    """Backtest gateway shares the simulation config schema."""

    Params = _SimParams

    def build(self, params, ctx, *, venue):
        # Backtest builder wires its own BacktestOrderGateway; the live
        # builder uses SimulationOrderGateway. Returning the sim config
        # here means the live builder treats `type = "backtest"` as
        # simulation. The backtest builder picks up the spec separately
        # via gateway_registry and reads params off it.
        gw = SimulationOrderGateway(
            bus=ctx.bus, clock=ctx.clock,
            config=_build_sim_config(params, venue),
        )
        return gw, []


def sim_config_from_params(params_dict: dict, venue: str) -> SimulationOrderGatewayConfig:
    """Convenience for the backtest builder."""
    return _build_sim_config(_SimParams.model_validate(params_dict), venue)


def register() -> None:
    gateway_registry.register("simulation", _SimulationPlugin())
    gateway_registry.register("backtest", _BacktestPlugin())


register()
