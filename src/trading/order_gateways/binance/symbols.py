"""Symbol mapping between Binance wire format and our canonical form.

Binance uses concatenated symbols like ``BTCUSDT``. Our canonical form
includes a separator (``BTC-USDT``) to keep base/quote unambiguous —
without it, you can't parse ``ETHBTC`` (ETH/BTC) from ``ETHUSDT`` (ETH/USDT)
without already knowing the universe of quote currencies.

We handle this via a registry built from :class:`Instrument` declarations.
The :class:`Instrument` carries the canonical symbol; the mapper holds the
Binance wire symbol alongside.
"""

from __future__ import annotations

from collections.abc import Iterable

from ...core.instruments import Instrument


class SymbolMapper:
    """Bidirectional lookup between Instrument and Binance wire symbol.

    The Binance wire symbol is derived as ``base + quote`` with both
    upper-cased — a venue-specific convention. If the convention ever
    fails for a particular instrument (some pairs have unusual base/quote
    splits), override at construction.
    """

    def __init__(self, instruments: Iterable[Instrument]) -> None:
        self._by_canonical: dict[str, Instrument] = {}
        self._by_wire: dict[str, Instrument] = {}
        self._wire_for_instrument: dict[str, str] = {}
        for inst in instruments:
            if inst.exchange != "BINANCE":
                continue
            wire = self.canonical_to_wire(inst)
            self._by_canonical[inst.symbol] = inst
            self._by_wire[wire] = inst
            self._wire_for_instrument[inst.symbol] = wire

    @staticmethod
    def canonical_to_wire(instrument: Instrument) -> str:
        """``BTC`` + ``USDT`` -> ``BTCUSDT``."""
        return f"{instrument.base_currency}{instrument.quote_currency}".upper()

    def wire_symbol(self, instrument: Instrument) -> str:
        try:
            return self._wire_for_instrument[instrument.symbol]
        except KeyError:
            # Fall back to derivation — supports symbols not pre-registered.
            return self.canonical_to_wire(instrument)

    def by_wire(self, wire_symbol: str) -> Instrument | None:
        return self._by_wire.get(wire_symbol.upper())

    def by_canonical(self, canonical: str) -> Instrument | None:
        return self._by_canonical.get(canonical)

    def all_wire_symbols(self) -> list[str]:
        """Return every Binance wire symbol registered with this mapper."""
        return list(self._by_wire.keys())


__all__ = ["SymbolMapper"]
