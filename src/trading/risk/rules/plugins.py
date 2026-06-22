"""Plugin registrations for the built-in risk rules."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from ...plugins import rule_registry
from .daily_loss_limit import DailyLossLimitRule
from .instrument_allowlist import InstrumentAllowlistRule
from .max_notional import MaxNotionalRule
from .max_order_size import MaxOrderSizeRule
from .max_position import MaxPositionRule
from .throttle import ThrottleRule
from .vpin_circuit_breaker import VPINCircuitBreakerRule

class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MaxPositionParams(_Strict):
    max_long: Decimal
    max_short: Decimal


class MaxOrderSizeParams(_Strict):
    max_quantity: Decimal


class MaxNotionalParams(_Strict):
    max_notional: Decimal


class ThrottleParams(_Strict):
    max_signals: int
    window_seconds: float = 60.0


class DailyLossLimitParams(_Strict):
    max_loss: Decimal

class VPINCircuitBreakerParams(_Strict):
    vpin_threshold: float = 0.8
    sustained_ticks: int = 5

class InstrumentAllowlistParams(_Strict):
    # Accept comma-separated string (legacy TOML) or list.
    allowed_instrument_ids: str | list[str]


class _MaxPositionPlugin:
    Params = MaxPositionParams

    def build(self, params: MaxPositionParams):
        return MaxPositionRule(max_long=params.max_long, max_short=params.max_short)


class _MaxOrderSizePlugin:
    Params = MaxOrderSizeParams

    def build(self, params: MaxOrderSizeParams):
        return MaxOrderSizeRule(max_quantity=params.max_quantity)


class _MaxNotionalPlugin:
    Params = MaxNotionalParams

    def build(self, params: MaxNotionalParams):
        return MaxNotionalRule(max_notional=params.max_notional)


class _ThrottlePlugin:
    Params = ThrottleParams

    def build(self, params: ThrottleParams):
        return ThrottleRule(
            max_signals=params.max_signals,
            window_seconds=params.window_seconds,
        )


class _DailyLossLimitPlugin:
    Params = DailyLossLimitParams

    def build(self, params: DailyLossLimitParams):
        return DailyLossLimitRule(max_loss=params.max_loss)


class _VPINCircuitBreakerPlugin:
    Params = VPINCircuitBreakerParams

    def build(self, params: VPINCircuitBreakerParams):
        return VPINCircuitBreakerRule(
            vpin_threshold=params.vpin_threshold,
            sustained_ticks=params.sustained_ticks,
        )

class _InstrumentAllowlistPlugin:
    Params = InstrumentAllowlistParams

    def build(self, params: InstrumentAllowlistParams):
        ids = params.allowed_instrument_ids
        if isinstance(ids, str):
            ids = [i.strip() for i in ids.split(",") if i.strip()]
        return InstrumentAllowlistRule(allowed_instrument_ids=ids)


def register() -> None:
    rule_registry.register("max_position", _MaxPositionPlugin())
    rule_registry.register("max_order_size", _MaxOrderSizePlugin())
    rule_registry.register("max_notional", _MaxNotionalPlugin())
    rule_registry.register("throttle", _ThrottlePlugin())
    rule_registry.register("daily_loss_limit", _DailyLossLimitPlugin())
    rule_registry.register("instrument_allowlist", _InstrumentAllowlistPlugin())
    rule.registry.register("vpin_circuit_breaker", _VPINCircuitBreakerPlugin())


register()
