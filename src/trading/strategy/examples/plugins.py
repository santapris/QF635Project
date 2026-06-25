"""Plugin registrations for the built-in example strategies."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict

from ...plugins import strategy_registry
from .avellaneda_stoikov import AvellanedaStoikovStrategy
from .glft import GLFTStrategy
from .grid import GridStrategy
from .market_making import MarketMakingStrategy
from .mean_reversion import MeanReversionStrategy
from .microprice_mm import MicropriceMMStrategy
from .momentum import MomentumStrategy
from .obi_alpha import OBIAlphaStrategy
from .ping_pong import PingPongStrategy


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MomentumParams(_Strict):
    fast_period: int = 20
    slow_period: int = 50
    # Read at runtime via ctx.get_param("target_quantity") in MomentumStrategy,
    # not passed to the constructor — declared here so strict validation of the
    # TOML [strategies.parameters] table accepts it.
    target_quantity: Decimal = Decimal("1")


class MeanReversionParams(_Strict):
    period: int = 20
    num_std: float = 2.0


class MarketMakingParams(_Strict):
    quote_size: Decimal = Decimal("0.01")
    target_spread_bps: float = 10.0
    max_position: Decimal = Decimal("0.5")
    inventory_skew_bps: float = 5.0


class PingPongParams(_Strict):
    interval_seconds: float = 10.0


class _MomentumPlugin:
    Params = MomentumParams

    def build(self, params, ctx, *, strategy_id, instruments):
        return MomentumStrategy(
            strategy_id=strategy_id, instruments=instruments,
            fast_period=params.fast_period, slow_period=params.slow_period,
        )


class _MeanReversionPlugin:
    Params = MeanReversionParams

    def build(self, params, ctx, *, strategy_id, instruments):
        return MeanReversionStrategy(
            strategy_id=strategy_id, instruments=instruments,
            period=params.period, num_std=params.num_std,
        )


class _MarketMakingPlugin:
    Params = MarketMakingParams

    def build(self, params, ctx, *, strategy_id, instruments):
        return MarketMakingStrategy(
            strategy_id=strategy_id, instruments=instruments,
            quote_size=params.quote_size,
            target_spread_bps=params.target_spread_bps,
            max_position=params.max_position,
            inventory_skew_bps=params.inventory_skew_bps,
        )


class _PingPongPlugin:
    Params = PingPongParams

    def build(self, params, ctx, *, strategy_id, instruments):
        return PingPongStrategy(
            strategy_id=strategy_id, instruments=instruments,
            interval_seconds=params.interval_seconds,
        )


class AvellanedaStoikovParams(_Strict):
    gamma: float = 0.3
    k: float = 1.5
    tau_seconds: float = 300.0
    half_life_seconds: float = 60.0
    ofi_window_seconds: float = 10.0
    ofi_alpha: float = 0.0
    vpin_bucket_volume: float = 1.0
    vpin_threshold: float = 0.7
    vpin_widen_factor: float = 3.0
    quote_size: Decimal = Decimal("0.01")
    max_position: Decimal = Decimal("0.5")
    min_vol: float = 0.5
    min_price_move_ticks: int = 1


class _AvellanedaStoikovPlugin:
    Params = AvellanedaStoikovParams

    def build(self, params, ctx, *, strategy_id, instruments):
        return AvellanedaStoikovStrategy(
            strategy_id=strategy_id, instruments=instruments,
            gamma=params.gamma, k=params.k,
            tau_seconds=params.tau_seconds,
            half_life_seconds=params.half_life_seconds,
            ofi_window_seconds=params.ofi_window_seconds,
            ofi_alpha=params.ofi_alpha,
            vpin_bucket_volume=params.vpin_bucket_volume,
            vpin_threshold=params.vpin_threshold,
            vpin_widen_factor=params.vpin_widen_factor,
            quote_size=params.quote_size,
            max_position=params.max_position,
            min_vol=params.min_vol,
            min_price_move_ticks=params.min_price_move_ticks,
        )


class GLFTParams(_Strict):
    gamma: float = 0.2
    k: float = 1.5
    A: float = 140.0
    half_life_seconds: float = 60.0
    ofi_window_seconds: float = 10.0
    ofi_alpha: float = 0.0
    quote_size: Decimal = Decimal("0.01")
    max_position: Decimal = Decimal("0.5")
    min_vol: float = 0.5
    min_price_move_ticks: int = 1
    n_levels: int = 1
    grid_step_bps: float = 5.0


class _GLFTPlugin:
    Params = GLFTParams

    def build(self, params, ctx, *, strategy_id, instruments):
        return GLFTStrategy(
            strategy_id=strategy_id, instruments=instruments,
            gamma=params.gamma, k=params.k, A=params.A,
            half_life_seconds=params.half_life_seconds,
            ofi_window_seconds=params.ofi_window_seconds,
            ofi_alpha=params.ofi_alpha,
            quote_size=params.quote_size,
            max_position=params.max_position,
            min_vol=params.min_vol,
            min_price_move_ticks=params.min_price_move_ticks,
            n_levels=params.n_levels,
            grid_step_bps=params.grid_step_bps,
        )


class OBIAlphaParams(_Strict):
    quote_size: Decimal = Decimal("0.01")
    target_spread_bps: float = 10.0
    max_position: Decimal = Decimal("0.5")
    inventory_skew_bps: float = 5.0
    obi_alpha: float = 0.0
    ofi_alpha: float = 0.0
    ofi_window_seconds: float = 10.0
    min_price_move_ticks: int = 1


class _OBIAlphaPlugin:
    Params = OBIAlphaParams

    def build(self, params, ctx, *, strategy_id, instruments):
        return OBIAlphaStrategy(
            strategy_id=strategy_id, instruments=instruments,
            quote_size=params.quote_size,
            target_spread_bps=params.target_spread_bps,
            max_position=params.max_position,
            inventory_skew_bps=params.inventory_skew_bps,
            obi_alpha=params.obi_alpha,
            ofi_alpha=params.ofi_alpha,
            ofi_window_seconds=params.ofi_window_seconds,
            min_price_move_ticks=params.min_price_move_ticks,
        )


class GridParams(_Strict):
    quote_size: Decimal = Decimal("0.01")
    n_levels: int = 3
    grid_step_bps: float = 5.0
    max_position: Decimal = Decimal("0.5")
    inventory_skew_bps: float = 5.0
    use_microprice: bool = False
    min_quote_interval_s: float = 1.0
    requote_threshold_bps: float = 2.0


class _GridPlugin:
    Params = GridParams

    def build(self, params, ctx, *, strategy_id, instruments):
        return GridStrategy(
            strategy_id=strategy_id, instruments=instruments,
            quote_size=params.quote_size,
            n_levels=params.n_levels,
            grid_step_bps=params.grid_step_bps,
            max_position=params.max_position,
            inventory_skew_bps=params.inventory_skew_bps,
            use_microprice=params.use_microprice,
            min_quote_interval_s=params.min_quote_interval_s,
            requote_threshold_bps=params.requote_threshold_bps,
        )


class MicropriceMMParams(_Strict):
    quote_size: Decimal = Decimal("0.01")
    target_spread_bps: float = 10.0
    max_position: Decimal = Decimal("0.5")
    inventory_skew_bps: float = 5.0
    min_quote_interval_s: float = 1.0
    requote_threshold_bps: float = 2.0


class _MicropriceMMPlugin:
    Params = MicropriceMMParams

    def build(self, params, ctx, *, strategy_id, instruments):
        return MicropriceMMStrategy(
            strategy_id=strategy_id, instruments=instruments,
            quote_size=params.quote_size,
            target_spread_bps=params.target_spread_bps,
            max_position=params.max_position,
            inventory_skew_bps=params.inventory_skew_bps,
            min_quote_interval_s=params.min_quote_interval_s,
            requote_threshold_bps=params.requote_threshold_bps,
        )


def register() -> None:
    strategy_registry.register("momentum", _MomentumPlugin())
    strategy_registry.register("mean_reversion", _MeanReversionPlugin())
    strategy_registry.register("market_making", _MarketMakingPlugin())
    strategy_registry.register("ping_pong", _PingPongPlugin())
    strategy_registry.register("avellaneda_stoikov", _AvellanedaStoikovPlugin())
    strategy_registry.register("glft", _GLFTPlugin())
    strategy_registry.register("obi_alpha", _OBIAlphaPlugin())
    strategy_registry.register("grid", _GridPlugin())
    strategy_registry.register("microprice_mm", _MicropriceMMPlugin())


register()
