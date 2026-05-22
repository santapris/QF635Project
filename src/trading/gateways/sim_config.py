"""Configuration types for the simulation gateway.

Kept separate from the gateway code so they're easy to discover, easy
to override per-test, and not buried in a long file.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class FeeModel:
    """Fee schedule for a venue.

    Crypto exchanges typically charge maker/taker bps; equity venues
    charge flat per-share. The model below covers the bps case; flat
    fees can be modeled by setting ``maker_bps=0`` and applying a
    per-share fee elsewhere (out of scope for this MVP).
    """

    maker_bps: float = 1.0   # 1 bp = 0.01%
    taker_bps: float = 5.0
    fee_currency: str = ""    # If empty, inherits from instrument.quote_currency

    def fee_for(self, *, notional: Decimal, is_maker: bool) -> Decimal:
        bps = self.maker_bps if is_maker else self.taker_bps
        return notional * Decimal(str(bps)) / Decimal("10000")


@dataclass(frozen=True, slots=True)
class LatencyModel:
    """Network latency between OMS and venue.

    Two numbers because real exchanges have one-way latency that
    differs from round-trip and shows non-trivial variance. Defaults
    are reasonable for co-located crypto in 2024-2025.

    Latency applies to:
    - submit -> ack
    - cancel -> ack
    - market fill response

    All values in milliseconds. Zero is allowed and produces an
    instantaneous gateway (useful for unit tests).
    """

    submit_ack_ms: float = 5.0
    cancel_ack_ms: float = 5.0
    fill_ms: float = 10.0


@dataclass(frozen=True, slots=True)
class FillModel:
    """How the simulator decides to fill resting limit orders.

    Two flavours are supported. The simulator picks the first one whose
    inputs are available:

    - **AGGRESSIVE_MARKET**: market and IOC orders fill instantly at
      the current best opposite-side price (or last trade if no book).
    - **PASSIVE_LIMIT**: limit orders fill when the opposite-side
      market trades through their price. This requires the simulator
      to subscribe to :class:`TradeEvent` on the market data topic;
      otherwise resting limit orders never fill.

    ``partial_fill_probability`` introduces partial-fill realism: with
    this probability a fill that *could* be complete is instead
    delivered in two pieces. Set to 0 for deterministic tests.
    """

    partial_fill_probability: float = 0.0
    """0.0 = always full fill; 0.5 = half of fills are split into two."""

    partial_fill_min_fraction: float = 0.1
    """Floor on the first-half fraction (default: never below 10%)."""

    slippage_ticks: int = 0
    """Apply this many ticks of slippage against the order on market fills."""


@dataclass(frozen=True, slots=True)
class RejectModel:
    """Configurable reject conditions for testing.

    Used in tests that need to exercise the reject-handling path of
    OMS and risk-aware strategies. Defaults to no rejects.
    """

    reject_probability: float = 0.0
    reject_reason: str = "simulated reject"


@dataclass(frozen=True, slots=True)
class SimulationGatewayConfig:
    venue: str
    fees: FeeModel = field(default_factory=FeeModel)
    latency: LatencyModel = field(default_factory=LatencyModel)
    fills: FillModel = field(default_factory=FillModel)
    rejects: RejectModel = field(default_factory=RejectModel)
    seed: int | None = None
    """Random seed for deterministic partial-fill / reject behaviour."""


__all__ = [
    "FeeModel",
    "FillModel",
    "LatencyModel",
    "RejectModel",
    "SimulationGatewayConfig",
]
