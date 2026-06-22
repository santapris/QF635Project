"""Top-level configuration schema.

Generic by design: nothing in this file names a specific exchange,
strategy, or risk rule. The ``type`` field on each component spec is an
open string; the matching plugin lives next to its implementation and
registers itself with ``trading.plugins``.
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..core.instruments import InstrumentSpec
from ..core.types import StrategyId


class BusBackend(str, Enum):
    MEMORY = "memory"
    ASYNCIO = "asyncio"
    KAFKA = "kafka"


class BusConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend: BusBackend = BusBackend.ASYNCIO
    queue_size: int = Field(default=10_000, gt=0)

    bootstrap_servers: str | None = None
    client_id: str | None = None
    topic_prefix: str = "trading"

    @field_validator("bootstrap_servers", "client_id")
    @classmethod
    def _normalize_empty(cls, v: str | None, info) -> str | None:
        return v or None


class FeedHandlerSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Stable id for this feed handler.")
    venue: str
    instruments: list[str] = Field(..., description="Symbols on the venue.")
    stale_threshold_seconds: float = 30.0
    max_reconnect_attempts: int = 10
    backoff_initial_seconds: float = 1.0
    backoff_max_seconds: float = 60.0


def _collect_extras_into(field_name: str, *reserved: str):
    """Build a pre-validator that scoops unknown keys into ``field_name``.

    Lets TOML stay flat (``maker_bps = 1.0`` at the gateway top level)
    while the internal model holds a tidy ``params: dict`` underneath.
    """

    reserved_set = set(reserved) | {field_name}

    def _validator(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        bucket: dict[str, Any] = dict(data.get(field_name) or {})
        out: dict[str, Any] = {}
        for k, v in data.items():
            if k in reserved_set:
                out[k] = v
            else:
                bucket[k] = v
        out[field_name] = bucket
        return out

    return _validator


class StrategySpec(BaseModel):
    """One strategy. ``type`` selects the plugin; ``parameters`` are passed verbatim."""

    model_config = ConfigDict(extra="forbid")

    strategy_id: StrategyId
    type: str
    instruments: list[str] = Field(..., description="Instrument IDs (venue:symbol).")
    enabled: bool = True
    parameters: dict[str, Any] = Field(default_factory=dict)


class RuleSpec(BaseModel):
    """One risk rule. ``type`` selects the plugin; ``params`` are passed verbatim."""

    model_config = ConfigDict(extra="forbid")

    type: str
    params: dict[str, Any] = Field(default_factory=dict)


class RiskSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    global_rules: list[RuleSpec] = Field(default_factory=list)
    per_strategy: dict[StrategyId, list[RuleSpec]] = Field(default_factory=dict)


class OMSSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    signal_ttl_seconds: float = 300.0
    # Cadence at which the OMS wakes execution algos to emit slices. Tune to
    # the strategies' execution horizon — tighter for fast TWAPs, looser for
    # slow unwinds.
    algo_driver_interval_seconds: float = 0.1
    # If True, cancel ALL pre-existing venue orders at startup. Default False
    # because the policy is to *adopt* pre-existing state (recover mid-trade),
    # not wipe it. Only enable for environments that must start from flat.
    cancel_stale_orders_on_start: bool = False
    # Internal netting / self-trade prevention. When True (default), the OMS
    # holds back any leg that would cross a *different* strategy's resting order
    # on the same instrument, so the firm never trades with itself. Disable only
    # for single-strategy setups or to measure its effect.
    self_trade_prevention: bool = True


class PositionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    method: Literal["WAVG", "FIFO", "LIFO"] = "WAVG"
    mark_to_market_interval_seconds: float = 5.0


class GatewaySpec(BaseModel):
    """One order gateway. ``type`` selects the plugin; ``params`` are passed verbatim.

    Top-level keys that aren't reserved (``type``, ``venue``, ``params``) are
    rolled into ``params`` so TOML can stay flat:

    ::

        [[order_gateways]]
        type = "binance"
        venue = "BINANCE"
        testnet = true        # becomes params.testnet
    """

    model_config = ConfigDict(extra="forbid")

    type: str
    venue: str
    params: dict[str, Any] = Field(default_factory=dict)

    _collect = model_validator(mode="before")(
        _collect_extras_into("params", "type", "venue")
    )


class BacktestSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data_path: Path
    instrument_id: str
    snapshot_interval_seconds: float = 60.0
    initial_equity: float = 100_000.0
    periods_per_year: float = 252
    timestamp_unit: Literal["s", "ms", "us", "ns"] = "ms"


class AppConfig(BaseModel):
    """Root configuration object."""

    model_config = ConfigDict(extra="forbid")

    instruments: list[InstrumentSpec] = Field(default_factory=list)
    bus: BusConfig = Field(default_factory=BusConfig)
    feed_handlers: list[FeedHandlerSpec] = Field(default_factory=list)
    strategies: list[StrategySpec] = Field(default_factory=list)
    risk: RiskSpec = Field(default_factory=RiskSpec)
    oms: OMSSpec = Field(default_factory=OMSSpec)
    position: PositionSpec = Field(default_factory=PositionSpec)
    order_gateways: list[GatewaySpec] = Field(default_factory=list)
    backtest: BacktestSpec | None = None


__all__ = [
    "AppConfig",
    "BacktestSpec",
    "BusBackend",
    "BusConfig",
    "FeedHandlerSpec",
    "GatewaySpec",
    "OMSSpec",
    "PositionSpec",
    "RiskSpec",
    "RuleSpec",
    "StrategySpec",
]
