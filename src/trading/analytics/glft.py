"""Guéant-Lehalle-Fernandez-Tapia (GLFT) market-making quote calculator.

Pure-function calculator — no state, no clock, no I/O. Caller passes
current inputs on each tick. The GLFT model (Guéant, Lehalle &
Fernandez-Tapia, 2013) is the closed-form, large-horizon evolution of
Avellaneda-Stoikov: instead of an explicit terminal time ``tau`` it uses
the asymptotic optimal quotes for a market maker facing exponential order
arrival intensity ``lambda(delta) = A * exp(-k * delta)``.

Formulae (asymptotic / stationary regime):
    c1 = (1 / gamma) * ln(1 + gamma / k)
    c2 = sqrt( (gamma / (2 * A * k)) * (1 + gamma / k) ** (1 + k / gamma) )

    half_spread   = c1 + 0.5 * c2 * sigma
    skew_per_unit = c2 * sigma                  # price shift per unit inventory
    reservation   = mid - inventory * skew_per_unit
    bid           = reservation - half_spread
    ask           = reservation + half_spread

Compared to A-S (``analytics/avellaneda_stoikov.py``):
  - No ``tau``; the horizon is asymptotic. ``A`` (intensity scale) appears
    explicitly and controls the base spread together with ``k``.
  - The inventory skew (``c2 * sigma``) grows linearly with vol, so the maker
    leans harder against inventory in volatile regimes.

Parameters:
    gamma : inventory risk-aversion coefficient (> 0)
    k     : order-arrival intensity decay (> 0)
    A     : order-arrival intensity scale (> 0)

``sigma`` is passed per call in the same absolute price-volatility units the
caller uses for ``mid`` (the GLFT strategy converts EWMA annualized-relative
vol to absolute per-second vol, exactly as the A-S strategy does).
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class GLFTQuotes:
    reservation: float
    bid: float
    ask: float
    half_spread: float
    skew_per_unit: float


class GLFT:
    """Stateless GLFT quote calculator."""

    __slots__ = ("_gamma", "_k", "_A", "_c1", "_c2_base")

    def __init__(self, gamma: float, k: float, A: float) -> None:
        if gamma <= 0:
            raise ValueError("gamma must be positive")
        if k <= 0:
            raise ValueError("k must be positive")
        if A <= 0:
            raise ValueError("A must be positive")
        self._gamma = gamma
        self._k = k
        self._A = A
        # Precompute the vol-independent constants.
        self._c1 = (1.0 / gamma) * math.log(1.0 + gamma / k)
        self._c2_base = math.sqrt(
            (gamma / (2.0 * A * k)) * (1.0 + gamma / k) ** (1.0 + k / gamma)
        )

    def quotes(self, mid: float, inventory: float, sigma: float) -> GLFTQuotes:
        """Compute GLFT bid/ask quotes.

        Args:
            mid:       current microprice or arithmetic mid
            inventory: signed position in base units (+long, -short)
            sigma:     absolute price volatility (same units as ``mid``)
        """
        skew_per_unit = self._c2_base * sigma
        half_spread = max(0.0, self._c1 + 0.5 * skew_per_unit)
        reservation = mid - inventory * skew_per_unit
        return GLFTQuotes(
            reservation=reservation,
            bid=reservation - half_spread,
            ask=reservation + half_spread,
            half_spread=half_spread,
            skew_per_unit=skew_per_unit,
        )


__all__ = ["GLFT", "GLFTQuotes"]
