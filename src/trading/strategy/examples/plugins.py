"""Plugin registrations for the built-in example strategies."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict

from ...plugins import strategy_registry
from .market_making import MarketMakingStrategy
from .mean_reversion import MeanReversionStrategy
from .momentum import MomentumStrategy
from .ping_pong import PingPongStrategy


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MomentumParams(_Strict):
    fast_period: int = 20
    slow_period: int = 50


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


def register() -> None:
    strategy_registry.register("momentum", _MomentumPlugin())
    strategy_registry.register("mean_reversion", _MeanReversionPlugin())
    strategy_registry.register("market_making", _MarketMakingPlugin())
    strategy_registry.register("ping_pong", _PingPongPlugin())


register()
