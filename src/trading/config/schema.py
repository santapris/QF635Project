"""Top-level configuration schema.

All app configuration is one Pydantic model tree. Loaders (TOML, env) hydrate
this tree; runners consume it. Validation happens once at load time — by the
time a runner is wiring components, every field is known-good.

The schema is intentionally explicit. No magic defaults that vary by environment;
no "we'll figure out a sensible value." If a knob matters it's spelled out.
"""

from __future__ import annotations

from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..core.instruments import InstrumentSpec
from ..core.types import StrategyId


class BusBackend(str, Enum):
    MEMORY = "memory"      # for tests
    ASYNCIO = "asyncio"    # single-process production
    KAFKA = "kafka"        # multi-process production


class BusConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend: BusBackend = BusBackend.ASYNCIO
    queue_size: int = Field(default=10_000, gt=0)

    # Kafka-only fields. Validated below to require them when backend=kafka.
    bootstrap_servers: str | None = None
    client_id: str | None = None
    topic_prefix: str = "trading"

    @field_validator("bootstrap_servers", "client_id")
    @classmethod
    def _require_for_kafka(cls, v: str | None, info) -> str | None:
        # We can't see ``backend`` here in a per-field validator without
        # cross-field info. The runner does the final cross-field check;
        # this validator just normalises empty strings to None.
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


class StrategySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy_id: StrategyId
    type: Literal["momentum", "mean_reversion", "market_making"]
    instruments: list[str] = Field(..., description="Instrument IDs (venue:symbol).")
    enabled: bool = True
    parameters: dict[str, str] = Field(default_factory=dict)


class RuleSpec(BaseModel):
    """One risk rule. ``type`` discriminates; ``params`` are passed verbatim."""

    model_config = ConfigDict(extra="forbid")

    type: Literal[
        "max_position",
        "max_order_size",
        "max_notional",
        "throttle",
        "daily_loss_limit",
        "instrument_allowlist",
    ]
    params: dict[str, str] = Field(default_factory=dict)


class RiskSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    global_rules: list[RuleSpec] = Field(default_factory=list)
    per_strategy: dict[StrategyId, list[RuleSpec]] = Field(default_factory=dict)


class OMSSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    signal_ttl_seconds: float = 300.0


class PositionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    method: Literal["WAVG", "FIFO", "LIFO"] = "WAVG"
    mark_to_market_interval_seconds: float = 5.0


class SimOrderGatewaySpec(BaseModel):
    """Simulation or backtest order_gateway (no real exchange connection)."""

    model_config = ConfigDict(extra="forbid")

    venue: str
    type: Literal["simulation", "backtest"] = "simulation"

    # Simulation/Backtest config — all optional with sensible defaults.
    maker_bps: float = 1.0
    taker_bps: float = 5.0
    submit_ack_ms: float = 5.0
    cancel_ack_ms: float = 5.0
    fill_ms: float = 10.0
    slippage_ticks: int = 0
    partial_fill_probability: float = 0.0
    seed: int | None = None


class BinanceOrderGatewaySpec(BaseModel):
    """Binance order_gateway config.

    ``credentials_env`` names the env-var prefix; the builder reads
    ``{credentials_env}_API_KEY`` and ``{credentials_env}_API_SECRET``.
    URL fields default to testnet or live endpoints based on ``testnet``.
    """

    model_config = ConfigDict(extra="forbid")

    venue: str = "BINANCE"
    type: Literal["binance"]
    testnet: bool = True
    credentials_env: str = "BINANCE"
    reconcile_interval_seconds: float = 60.0
    mismatch_threshold: str = "0.0001"

    spot_rest_base: str | None = None
    spot_ws_base: str | None = None
    futures_rest_base: str | None = None
    futures_ws_base: str | None = None


# Discriminated union — Pydantic selects the right model via the ``type`` field.
OrderGatewaySpec = Annotated[
    Union[SimOrderGatewaySpec, BinanceOrderGatewaySpec],
    Field(discriminator="type"),
]


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
    order_gateways: list[OrderGatewaySpec] = Field(default_factory=list)
    backtest: BacktestSpec | None = None


__all__ = [
    "AppConfig",
    "BacktestSpec",
    "BinanceOrderGatewaySpec",
    "BusBackend",
    "BusConfig",
    "FeedHandlerSpec",
    "OrderGatewaySpec",
    "OMSSpec",
    "PositionSpec",
    "RiskSpec",
    "RuleSpec",
    "SimOrderGatewaySpec",
    "StrategySpec",
]
