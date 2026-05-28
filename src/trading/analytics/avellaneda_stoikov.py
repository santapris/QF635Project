"""Avellaneda-Stoikov optimal market-making quote calculator.

Pure-function calculator — no state, no clock, no I/O. Caller passes
current inputs on each tick. Suitable for any inventory-carrying
market-making strategy.

Formulae (Avellaneda & Stoikov, 2008):
    reservation = mid - inventory * gamma * sigma² * tau
    half_spread  = (gamma * sigma² * tau) / 2 + (1/gamma) * ln(1 + gamma/k)
    bid          = reservation - half_spread
    ask          = reservation + half_spread

Parameters:
    gamma : inventory risk-aversion coefficient (0.01–1.0, calibrate)
    k     : order arrival intensity (calibrate from observed fill rate)
    tau   : time horizon in seconds
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class ASQuotes:
    reservation: float
    bid: float
    ask: float
    half_spread: float


class AvellanedaStoikov:
    """Stateless A-S quote calculator."""

    __slots__ = ("_gamma", "_k", "_tau")

    def __init__(self, gamma: float, k: float, tau_seconds: float) -> None:
        if gamma <= 0:
            raise ValueError("gamma must be positive")
        if k <= 0:
            raise ValueError("k must be positive")
        if tau_seconds <= 0:
            raise ValueError("tau_seconds must be positive")
        self._gamma = gamma
        self._k = k
        self._tau = tau_seconds

    def quotes(self, mid: float, inventory: float, sigma: float) -> ASQuotes:
        """Compute optimal bid/ask quotes.

        Args:
            mid:       current microprice or arithmetic mid
            inventory: signed position in base units (+long, -short)
            sigma:     annualized vol (or per-tau vol, consistent with tau)
        """
        sigma2 = sigma * sigma
        reservation = mid - inventory * self._gamma * sigma2 * self._tau
        half_spread = (
            (self._gamma * sigma2 * self._tau) / 2.0
            + (1.0 / self._gamma) * math.log(1.0 + self._gamma / self._k)
        )
        # Clamp half_spread to non-negative (numerical edge on tiny vol)
        half_spread = max(0.0, half_spread)
        return ASQuotes(
            reservation=reservation,
            bid=reservation - half_spread,
            ask=reservation + half_spread,
            half_spread=half_spread,
        )


__all__ = ["ASQuotes", "AvellanedaStoikov"]
