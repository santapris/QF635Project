"""
position_sizing.py
==================
Position Sizing & Inventory Management
(maps to "Inventory Management" in the lecture notes)

This module answers the question: *given a signal and my risk budget, how big
should the trade be?*  It collects the common sizing techniques from the notes:

  * Fixed position size
  * Fixed-fractional (risk a fixed % of capital)
  * ATR / volatility-based sizing            (the "ATR Risk-Based" slide)
  * Volatility targeting                      (scale to a target annual vol)
  * Kelly fraction                            (optimal growth sizing)
  * Signal scaling: linear / tanh / cubic     (the nonlinear scaling slide)

Each function is small and independent, so you can mix and match. They all
return a number of shares/contracts/units (rounded down to be safe).

Beginner note: "ATR" = Average True Range, a common measure of how much a
price typically moves in one bar. Bigger ATR = more volatile = smaller size.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal, Optional

import numpy as np


# ════════════════════════════════════════════════════════════════════
# 1. Fixed and fixed-fractional sizing
# ════════════════════════════════════════════════════════════════════

def fixed_size(units: float) -> float:
    """Always trade the same number of units. The simplest possible rule."""
    return math.floor(units)


def fixed_fractional_size(capital: float, risk_fraction: float,
                          entry_price: float, stop_price: float) -> float:
    """
    Risk a fixed fraction of capital per trade, defined by your stop distance.

    Example: risk 1% of $1,000,000 = $10,000. If your stop is $2 away from
    entry, you can hold $10,000 / $2 = 5,000 units.

    Parameters
    ----------
    capital        : account capital
    risk_fraction  : fraction of capital to risk (0.01 = 1%)
    entry_price    : price you enter at
    stop_price     : price your stop-loss sits at
    """
    risk_amount = capital * risk_fraction
    stop_distance = abs(entry_price - stop_price)
    if stop_distance <= 0:
        return 0.0
    return math.floor(risk_amount / stop_distance)


# ════════════════════════════════════════════════════════════════════
# 2. ATR / volatility-based sizing  (the lecture "ATR Risk-Based" example)
# ════════════════════════════════════════════════════════════════════

def atr_position_size(account_capital: float, risk_per_trade_pct: float,
                      atr: float, atr_multiplier: float = 2.0) -> float:
    """
    ATR-based position sizing.

        Position Size = (Account Capital x Risk Per Trade %) / (ATR x Multiplier)

    This keeps the *dollar* risk per trade constant regardless of how volatile
    the instrument is. More volatile (bigger ATR) -> smaller position.

    Worked example from the notes:
        capital = 1,000,000, risk = 1%, ATR = 500, multiplier = 2
        max risk        = 1,000,000 x 0.01     = 10,000
        stop distance   = 500 x 2              = 1,000
        position size   = 10,000 / 1,000       = 10 contracts
    """
    max_risk_amount = account_capital * risk_per_trade_pct
    stop_distance = atr * atr_multiplier
    if stop_distance <= 0:
        return 0.0
    return math.floor(max_risk_amount / stop_distance)


def compute_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
                period: int = 14) -> float:
    """
    Compute the Average True Range from arrays of high/low/close prices.

    True Range = max of:
        (high - low),
        |high - previous close|,
        |low  - previous close|
    ATR = simple moving average of True Range over `period` bars.
    """
    highs = np.asarray(highs, dtype=float)
    lows = np.asarray(lows, dtype=float)
    closes = np.asarray(closes, dtype=float)
    if len(closes) < 2:
        return 0.0

    prev_close = closes[:-1]
    tr = np.maximum.reduce([
        highs[1:] - lows[1:],
        np.abs(highs[1:] - prev_close),
        np.abs(lows[1:] - prev_close),
    ])
    if len(tr) < period:
        return float(np.mean(tr)) if len(tr) else 0.0
    return float(np.mean(tr[-period:]))


# ════════════════════════════════════════════════════════════════════
# 3. Volatility targeting
# ════════════════════════════════════════════════════════════════════

def volatility_target_size(capital: float, target_annual_vol: float,
                           asset_annual_vol: float, price: float,
                           max_leverage: float = 4.0) -> float:
    """
    Scale exposure so the position contributes a target annualised volatility.

        leverage = target_vol / asset_vol      (capped at max_leverage)
        notional = capital x leverage
        units    = notional / price

    Example: target 10% vol, asset vol 25% -> leverage 0.4 -> hold 40% of capital.
    """
    if asset_annual_vol <= 0:
        return 0.0
    leverage = min(target_annual_vol / asset_annual_vol, max_leverage)
    notional = capital * leverage
    if price <= 0:
        return 0.0
    return math.floor(notional / price)


# ════════════════════════════════════════════════════════════════════
# 4. Kelly fraction
# ════════════════════════════════════════════════════════════════════

def kelly_fraction(win_prob: float, win_loss_ratio: float) -> float:
    """
    Kelly criterion optimal fraction of capital to bet.

        f* = p - (1 - p) / b

    where p = probability of winning, b = ratio of win size to loss size.
    Returns 0 if the edge is negative. In practice traders use a fraction of
    this ("half-Kelly") because full Kelly is very aggressive.
    """
    if win_loss_ratio <= 0:
        return 0.0
    f = win_prob - (1 - win_prob) / win_loss_ratio
    return max(0.0, f)


def kelly_position_size(capital: float, price: float, win_prob: float,
                        win_loss_ratio: float, kelly_scale: float = 0.5) -> float:
    """
    Convert a Kelly fraction into a number of units.
    `kelly_scale` lets you use e.g. half-Kelly (0.5) for safety.
    """
    f = kelly_fraction(win_prob, win_loss_ratio) * kelly_scale
    notional = capital * f
    if price <= 0:
        return 0.0
    return math.floor(notional / price)


# ════════════════════════════════════════════════════════════════════
# 5. Signal-based scaling  (linear / tanh / cubic)  -- from the notes
# ════════════════════════════════════════════════════════════════════

ScalingMethod = Literal["linear", "tanh", "cubic"]


def signal_scaled_size(max_position: float, signal: float,
                       method: ScalingMethod = "linear",
                       tanh_k: float = 2.0) -> float:
    """
    Turn a normalised signal in [-1, +1] into a position size.

        linear : Position = max_position x signal
        tanh   : Position = max_position x tanh(k x signal)   (smooth saturation)
        cubic  : Position = max_position x signal**3          (suppress weak signals)

    +1 = strongest long, -1 = strongest short, 0 = flat.

    Worked example (max_position = 100, signal = 0.5):
        linear -> 50,  tanh(k=2) -> ~76,  cubic -> 12.5
    """
    signal = max(-1.0, min(1.0, signal))   # clamp to [-1, 1]

    if method == "linear":
        raw = max_position * signal
    elif method == "tanh":
        raw = max_position * math.tanh(tanh_k * signal)
    elif method == "cubic":
        raw = max_position * (signal ** 3)
    else:
        raise ValueError(f"Unknown scaling method: {method}")

    # round toward zero so we never round a tiny signal up into a position
    return float(math.trunc(raw))


# ════════════════════════════════════════════════════════════════════
# 6. Inventory skewing (market-making helper)
# ════════════════════════════════════════════════════════════════════

def inventory_skew_adjustment(current_inventory: float, max_inventory: float,
                              max_skew_ticks: float = 5.0,
                              tick_size: float = 0.01) -> float:
    """
    How far to shift quotes (in price units) to nudge inventory back to zero.

    If we are long, we lower both quotes so we are more likely to sell.
    If we are short, we raise them so we are more likely to buy.
    Returns a price offset (negative when long, positive when short).
    """
    if max_inventory <= 0:
        return 0.0
    ratio = max(-1.0, min(1.0, current_inventory / max_inventory))
    return -ratio * max_skew_ticks * tick_size


# ════════════════════════════════════════════════════════════════════
# 7. Convenience wrapper that applies a hard cap
# ════════════════════════════════════════════════════════════════════

@dataclass
class SizingResult:
    units: float
    notional: float
    method: str
    capped: bool = False


def cap_to_limits(units: float, price: float, max_notional: float,
                  method: str = "") -> SizingResult:
    """
    Take a desired size and shrink it if it would exceed a notional cap.
    Always call this after a sizing function to respect your per-symbol limit.
    """
    desired_notional = abs(units) * price
    capped = False
    if max_notional > 0 and desired_notional > max_notional:
        units = math.copysign(math.floor(max_notional / price), units)
        capped = True
    return SizingResult(units=units, notional=abs(units) * price,
                        method=method, capped=capped)


# ════════════════════════════════════════════════════════════════════
# Smoke test
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Fixed fractional (1% risk, $2 stop):",
          fixed_fractional_size(1_000_000, 0.01, 100, 98))

    print("ATR size (cap 1M, 1%, ATR 500, x2):",
          atr_position_size(1_000_000, 0.01, 500, 2))

    print("Vol target (10% target, 25% asset, $100):",
          volatility_target_size(1_000_000, 0.10, 0.25, 100))

    print("Half-Kelly (p=0.55, b=1.5, $50):",
          kelly_position_size(1_000_000, 50, 0.55, 1.5, 0.5))

    for m in ("linear", "tanh", "cubic"):
        print(f"Signal scaled ({m}, max=100, s=0.5):",
              signal_scaled_size(100, 0.5, m))

    print("Inventory skew (long 80 of 100 max):",
          round(inventory_skew_adjustment(80, 100, 5, 0.01), 4))
