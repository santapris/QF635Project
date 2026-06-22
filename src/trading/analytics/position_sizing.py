"""Position sizing algorithms.

Stateless utility functions for computing trade sizes from risk budgets.
No I/O, no async, no bus dependency — safe to import anywhere.

Techniques:
- Fixed and fixed-fractional sizing
- ATR / volatility-based sizing  (ATR Risk-Based slide)
- Volatility targeting
- Kelly criterion (full and fractional)
- Signal-based scaling: linear / tanh / cubic
- Inventory skew adjustment (market-making helper)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

import numpy as np


ScalingMethod = Literal["linear", "tanh", "cubic"]


@dataclass
class SizingResult:
    """Result of cap_to_limits — desired size after applying a notional cap."""
    units: float
    notional: float
    method: str
    capped: bool = False


def fixed_size(units: float) -> float:
    """Always trade the same number of units. Rounds down for safety."""
    return math.floor(units)


def fixed_fractional_size(
    capital: float,
    risk_fraction: float,
    entry_price: float,
    stop_price: float,
) -> float:
    """Risk a fixed fraction of capital per trade, defined by the stop distance.

    Position = (capital × risk_fraction) / |entry − stop|

    Example: risk 1% of $1 000 000, stop $2 away → 5 000 units.
    """
    risk_amount = capital * risk_fraction
    stop_distance = abs(entry_price - stop_price)
    if stop_distance <= 0:
        return 0.0
    return math.floor(risk_amount / stop_distance)


def atr_position_size(
    account_capital: float,
    risk_per_trade_pct: float,
    atr: float,
    atr_multiplier: float = 2.0,
) -> float:
    """ATR-based position sizing — keeps dollar risk per trade constant.

    Position = (capital × risk%) / (ATR × multiplier)

    Example: capital $1M, risk 1%, ATR 500, multiplier 2 → 10 contracts.
    """
    max_risk = account_capital * risk_per_trade_pct
    stop_distance = atr * atr_multiplier
    if stop_distance <= 0:
        return 0.0
    return math.floor(max_risk / stop_distance)


def compute_atr(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    period: int = 14,
) -> float:
    """Compute Average True Range from price arrays.

    True Range = max(high−low, |high−prev_close|, |low−prev_close|)
    ATR = SMA(TR, period)
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


def volatility_target_size(
    capital: float,
    target_annual_vol: float,
    asset_annual_vol: float,
    price: float,
    max_leverage: float = 4.0,
) -> float:
    """Scale exposure so the position contributes a target annualised vol.

    leverage = min(target_vol / asset_vol, max_leverage)
    units    = (capital × leverage) / price
    """
    if asset_annual_vol <= 0 or price <= 0:
        return 0.0
    leverage = min(target_annual_vol / asset_annual_vol, max_leverage)
    return math.floor(capital * leverage / price)


def kelly_fraction(win_prob: float, win_loss_ratio: float) -> float:
    """Kelly criterion: optimal fraction of capital to bet.

    f* = p − (1−p) / b

    Returns 0 when the edge is negative or win_loss_ratio is non-positive.
    """
    if win_loss_ratio <= 0:
        return 0.0
    return max(0.0, win_prob - (1.0 - win_prob) / win_loss_ratio)


def kelly_position_size(
    capital: float,
    price: float,
    win_prob: float,
    win_loss_ratio: float,
    kelly_scale: float = 0.5,
) -> float:
    """Convert a Kelly fraction into a number of units.

    kelly_scale=0.5 gives half-Kelly, which is common in practice.
    """
    if price <= 0:
        return 0.0
    f = kelly_fraction(win_prob, win_loss_ratio) * kelly_scale
    return math.floor(capital * f / price)


def signal_scaled_size(
    max_position: float,
    signal: float,
    method: ScalingMethod = "linear",
    tanh_k: float = 2.0,
) -> float:
    """Map a normalised signal in [−1, +1] to a position size.

    linear: position = max_position × signal
    tanh:   position = max_position × tanh(k × signal)   — smooth saturation
    cubic:  position = max_position × signal³            — suppresses weak signals

    Truncates toward zero so weak signals never round up into a position.
    """
    signal = max(-1.0, min(1.0, signal))
    if method == "linear":
        raw = max_position * signal
    elif method == "tanh":
        raw = max_position * math.tanh(tanh_k * signal)
    elif method == "cubic":
        raw = max_position * (signal ** 3)
    else:
        raise ValueError(f"Unknown scaling method: {method!r}")
    return float(math.trunc(raw))


def inventory_skew_adjustment(
    current_inventory: float,
    max_inventory: float,
    max_skew_ticks: float = 5.0,
    tick_size: float = 0.01,
) -> float:
    """Price offset to skew quotes and nudge inventory back toward zero.

    Returns a negative offset when long (lower quotes → more likely to sell),
    positive when short. Zero when max_inventory is non-positive.
    """
    if max_inventory <= 0:
        return 0.0
    ratio = max(-1.0, min(1.0, current_inventory / max_inventory))
    return -ratio * max_skew_ticks * tick_size


def cap_to_limits(
    units: float,
    price: float,
    max_notional: float,
    method: str = "",
) -> SizingResult:
    """Shrink a desired size if it would exceed a notional cap.

    Always call this after a sizing function to enforce per-symbol limits.
    """
    desired_notional = abs(units) * price
    capped = False
    if max_notional > 0 and desired_notional > max_notional:
        units = math.copysign(math.floor(max_notional / price), units)
        capped = True
    return SizingResult(
        units=units,
        notional=abs(units) * price,
        method=method,
        capped=capped,
    )


__all__ = [
    "ScalingMethod",
    "SizingResult",
    "atr_position_size",
    "cap_to_limits",
    "compute_atr",
    "fixed_fractional_size",
    "fixed_size",
    "inventory_skew_adjustment",
    "kelly_fraction",
    "kelly_position_size",
    "signal_scaled_size",
    "volatility_target_size",
]
