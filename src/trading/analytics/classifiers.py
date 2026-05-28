"""Trade classifiers for VPIN bucket filling.

BVCClassifier — Bulk-Volume Classification (Easley et al. 2012).
    Classifies volume into buy/sell fractions using the normal CDF of
    standardized price changes. Requires an EWMA vol estimate.

TickRuleClassifier — classic tick-rule fallback.
    Trade is a buy if price rose from last trade, sell if it fell.
    Unchanged price inherits the prior classification.
"""

from __future__ import annotations

import math
from typing import Protocol, runtime_checkable


@runtime_checkable
class TradeClassifier(Protocol):
    """Protocol for VPIN trade classifiers."""

    def classify(self, price: float, volume: float) -> tuple[float, float]:
        """Return (buy_volume, sell_volume) for this trade."""
        ...


class BVCClassifier:
    """Bulk-Volume Classification using normal CDF.

    buy_frac  = Φ(ΔS / σ_ΔS)
    sell_frac = 1 - buy_frac

    Where σ_ΔS is a rolling std of price changes (EWMA approximation).
    On the first call (no prior), defaults to 50/50 split.
    """

    __slots__ = ("_vol_seconds", "_ewma_var", "_prev_price", "_lam")

    def __init__(self, half_life_ticks: int = 50) -> None:
        if half_life_ticks <= 0:
            raise ValueError("half_life_ticks must be positive")
        self._lam = math.exp(-1.0 / half_life_ticks)
        self._ewma_var: float = 0.0
        self._prev_price: float | None = None

    def classify(self, price: float, volume: float) -> tuple[float, float]:
        if self._prev_price is None or self._prev_price <= 0:
            self._prev_price = price
            return (volume * 0.5, volume * 0.5)

        dp = price - self._prev_price
        self._ewma_var = self._lam * self._ewma_var + (1.0 - self._lam) * dp * dp
        self._prev_price = price

        if self._ewma_var <= 0.0:
            return (volume * 0.5, volume * 0.5)

        z = dp / math.sqrt(self._ewma_var)
        buy_frac = _norm_cdf(z)
        return (volume * buy_frac, volume * (1.0 - buy_frac))


class TickRuleClassifier:
    """Simple tick-rule classifier. No parameters."""

    __slots__ = ("_prev_price", "_last_side")

    def __init__(self) -> None:
        self._prev_price: float | None = None
        self._last_side: float = 0.5  # neutral start

    def classify(self, price: float, volume: float) -> tuple[float, float]:
        if self._prev_price is None:
            self._prev_price = price
            return (volume * 0.5, volume * 0.5)

        if price > self._prev_price:
            self._last_side = 1.0  # uptick → buy
        elif price < self._prev_price:
            self._last_side = 0.0  # downtick → sell
        # unchanged → inherit last side

        self._prev_price = price
        buy_frac = self._last_side
        return (volume * buy_frac, volume * (1.0 - buy_frac))


# --- Helper ----------------------------------------------------------------


def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erf."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


__all__ = ["BVCClassifier", "TickRuleClassifier", "TradeClassifier"]
