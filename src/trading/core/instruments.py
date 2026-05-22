"""Instrument definitions.

An :class:`Instrument` is a tradable thing — a symbol on a venue with
specific price and quantity granularity rules. Strategies should never
hardcode tick sizes; they ask the instrument.

We use Pydantic models (``frozen=True``) instead of plain dataclasses so
instruments serialize cleanly into events and config.
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from .types import AssetType, Price, Quantity, Symbol


class Instrument(BaseModel):
    """A tradable instrument on a specific venue."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    symbol: Symbol = Field(..., description="Venue-specific symbol, e.g. 'BTC-USDT'.")
    exchange: str = Field(..., description="Exchange identifier, e.g. 'BINANCE'.")
    asset_type: AssetType = Field(..., description="Spot, perp, future, etc.")

    base_currency: str = Field(..., description="The asset being bought/sold.")
    quote_currency: str = Field(..., description="The currency used to price it.")

    tick_size: Price = Field(..., description="Minimum price increment.")
    lot_size: Quantity = Field(..., description="Minimum quantity increment.")
    min_notional: Price | None = Field(
        default=None,
        description="Minimum order notional (some venues enforce this).",
    )
    contract_multiplier: Decimal = Field(
        default=Decimal("1"),
        description="For futures/options: how many units of base per contract.",
    )

    @property
    def instrument_id(self) -> str:
        """Globally-unique id across exchanges: 'BINANCE:BTC-USDT'."""
        return f"{self.exchange}:{self.symbol}"

    # --- Rounding helpers --------------------------------------------------
    # These are the canonical place to enforce venue rules. Strategies and
    # the OMS should always round through these methods rather than rolling
    # their own quantization, otherwise rounding errors leak into orders
    # and the exchange rejects them.

    def round_price(self, price: Price) -> Price:
        """Snap a price to the nearest tick (towards zero, i.e. truncated)."""
        return (price // self.tick_size) * self.tick_size

    def round_quantity(self, qty: Quantity) -> Quantity:
        """Snap a quantity down to the nearest valid lot."""
        return (qty // self.lot_size) * self.lot_size

    def is_valid_price(self, price: Price) -> bool:
        return price > 0 and (price % self.tick_size) == 0

    def is_valid_quantity(self, qty: Quantity) -> bool:
        return qty > 0 and (qty % self.lot_size) == 0


class InstrumentSpec(BaseModel):
    """Configuration form of :class:`Instrument`, loaded from TOML/YAML.

    Distinguished from ``Instrument`` so we can layer additional metadata
    (display names, fee tiers, etc.) at config time without polluting the
    runtime object.
    """

    model_config = ConfigDict(extra="forbid")

    symbol: Symbol
    exchange: str
    asset_type: AssetType
    base_currency: str
    quote_currency: str
    tick_size: Price
    lot_size: Quantity
    min_notional: Price | None = None
    contract_multiplier: Decimal = Decimal("1")
    display_name: str | None = None
    enabled: bool = True

    def to_instrument(self) -> Instrument:
        return Instrument(
            symbol=self.symbol,
            exchange=self.exchange,
            asset_type=self.asset_type,
            base_currency=self.base_currency,
            quote_currency=self.quote_currency,
            tick_size=self.tick_size,
            lot_size=self.lot_size,
            min_notional=self.min_notional,
            contract_multiplier=self.contract_multiplier,
        )


__all__ = ["Instrument", "InstrumentSpec"]
