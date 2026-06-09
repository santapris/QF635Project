"""
market_making.py
================
Market-Making Inventory Control (Avellaneda-Stoikov model)

Market makers post BOTH a bid (buy) and an ask (sell) quote and earn the
spread between them. Their main risk is *inventory risk*: if the price moves
while they are holding a lopsided position, they lose money.

The Avellaneda-Stoikov (2008) model gives two simple formulas to manage this:

  1. Reservation price  -- a fair price shifted by how much inventory you hold:
         r = S - q * gamma * sigma^2 * (T - t)
     If you are long (q > 0) the reservation price drops below mid, so you
     quote lower and are more likely to SELL and reduce inventory.

  2. Optimal half-spread -- how far from the reservation price to place quotes:
         delta = (1 / gamma) * ln(1 + gamma / k)

     bid = r - delta,   ask = r + delta

Symbols
-------
  S      : current mid / fair price
  q      : inventory (positive = long, negative = short)
  gamma  : risk aversion (bigger = more cautious, skews/widens more)
  sigma  : volatility of the asset
  T - t  : time remaining in the trading session (as a fraction, e.g. 0.5)
  k      : order-book liquidity (bigger k = deeper book = tighter spread)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class Quote:
    """A pair of bid/ask prices plus the reservation price they sit around."""
    bid: float
    ask: float
    reservation_price: float
    half_spread: float

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    @property
    def skew(self) -> float:
        """How far the reservation price sits from the mid we were given."""
        return self.reservation_price


@dataclass
class AvellanedaStoikovParams:
    """Tunable parameters for the model."""
    gamma: float = 0.1        # risk aversion
    k: float = 1.5            # order-book liquidity / arrival intensity
    sigma: float = 0.02       # asset volatility (per unit time)
    horizon: float = 1.0      # T : full session length (same units as elapsed)


class AvellanedaStoikovQuoter:
    """
    Produces inventory-aware bid/ask quotes.

    Typical usage inside a market-making loop:

        quoter = AvellanedaStoikovQuoter(AvellanedaStoikovParams(gamma=0.1, k=1.5))
        quote = quoter.quote(mid_price=100.0, inventory=5, time_elapsed=0.3)
        print(quote.bid, quote.ask)
    """

    def __init__(self, params: Optional[AvellanedaStoikovParams] = None,
                 max_inventory: float = 100.0):
        self.params = params or AvellanedaStoikovParams()
        self.max_inventory = max_inventory

    # ---- core formulas -------------------------------------------------

    def reservation_price(self, mid_price: float, inventory: float,
                          time_elapsed: float) -> float:
        """r = S - q * gamma * sigma^2 * (T - t)"""
        p = self.params
        time_remaining = max(0.0, p.horizon - time_elapsed)
        return mid_price - inventory * p.gamma * (p.sigma ** 2) * time_remaining

    def optimal_half_spread(self, time_elapsed: float = 0.0) -> float:
        """
        delta = (1 / gamma) * ln(1 + gamma / k)

        (This is the form used in the lecture notes. It does not depend on
        inventory; the inventory effect comes through the reservation price.)
        """
        p = self.params
        return (1.0 / p.gamma) * math.log(1.0 + p.gamma / p.k)

    def total_spread(self, time_elapsed: float = 0.0) -> float:
        """
        Full Avellaneda-Stoikov total spread (alternative form):
            2*delta = gamma*sigma^2*(T-t) + (2/k)*ln(1 + gamma/k)
        Provided for reference / comparison.
        """
        p = self.params
        time_remaining = max(0.0, p.horizon - time_elapsed)
        return (p.gamma * (p.sigma ** 2) * time_remaining
                + (2.0 / p.k) * math.log(1.0 + p.gamma / p.k))

    # ---- the thing you actually call ----------------------------------

    def quote(self, mid_price: float, inventory: float,
              time_elapsed: float = 0.0) -> Quote:
        """Return inventory-skewed bid/ask quotes around the reservation price."""
        r = self.reservation_price(mid_price, inventory, time_elapsed)
        delta = self.optimal_half_spread(time_elapsed)
        return Quote(
            bid=r - delta,
            ask=r + delta,
            reservation_price=r,
            half_spread=delta,
        )

    def quote_with_widening(self, mid_price: float, inventory: float,
                            time_elapsed: float = 0.0,
                            volatility_multiplier: float = 1.0) -> Quote:
        """
        Same as quote() but widens the spread in volatile conditions
        (the "spread widening" technique from the notes). Pass a
        volatility_multiplier > 1 when markets are choppy.
        """
        r = self.reservation_price(mid_price, inventory, time_elapsed)
        delta = self.optimal_half_spread(time_elapsed) * volatility_multiplier
        return Quote(bid=r - delta, ask=r + delta,
                     reservation_price=r, half_spread=delta)

    def inventory_utilisation(self, inventory: float) -> float:
        """Fraction of the inventory limit currently used (0..1+)."""
        if self.max_inventory <= 0:
            return 0.0
        return abs(inventory) / self.max_inventory


# ════════════════════════════════════════════════════════════════════
# Smoke test
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    quoter = AvellanedaStoikovQuoter(
        AvellanedaStoikovParams(gamma=0.1, k=1.5, sigma=0.02, horizon=1.0),
        max_inventory=100,
    )

    print("Flat inventory:")
    q = quoter.quote(mid_price=100.0, inventory=0, time_elapsed=0.0)
    print(f"  bid={q.bid:.4f}  ask={q.ask:.4f}  r={q.reservation_price:.4f}  spread={q.spread:.4f}")

    print("Long 50 units (should skew quotes DOWN to sell):")
    q = quoter.quote(mid_price=100.0, inventory=50, time_elapsed=0.0)
    print(f"  bid={q.bid:.4f}  ask={q.ask:.4f}  r={q.reservation_price:.4f}")

    print("Short 50 units (should skew quotes UP to buy):")
    q = quoter.quote(mid_price=100.0, inventory=-50, time_elapsed=0.0)
    print(f"  bid={q.bid:.4f}  ask={q.ask:.4f}  r={q.reservation_price:.4f}")
