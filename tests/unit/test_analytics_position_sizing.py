"""Unit tests for analytics.position_sizing."""

from __future__ import annotations

import numpy as np
import pytest

from trading.analytics.position_sizing import (
    atr_position_size,
    cap_to_limits,
    compute_atr,
    fixed_fractional_size,
    fixed_size,
    inventory_skew_adjustment,
    kelly_fraction,
    kelly_position_size,
    signal_scaled_size,
    volatility_target_size,
)


def test_fixed_size_floors():
    assert fixed_size(5.9) == 5
    assert fixed_size(5.0) == 5


def test_fixed_fractional_size_basic():
    # $10 000 risk / $2 stop = 5 000 units
    result = fixed_fractional_size(1_000_000, 0.01, 100.0, 98.0)
    assert result == 5_000.0


def test_fixed_fractional_size_zero_stop():
    assert fixed_fractional_size(1_000_000, 0.01, 100.0, 100.0) == 0.0


def test_atr_position_size_basic():
    # $10 000 risk / (500 ATR × 2 mult) = 10 contracts
    result = atr_position_size(1_000_000, 0.01, 500.0, 2.0)
    assert result == 10.0


def test_atr_position_size_zero_atr():
    assert atr_position_size(1_000_000, 0.01, 0.0) == 0.0


def test_compute_atr_simple():
    highs  = np.array([11.0, 12.0, 11.5, 13.0, 12.5])
    lows   = np.array([ 9.0, 10.0,  9.5, 11.0, 10.5])
    closes = np.array([10.0, 11.0, 10.5, 12.0, 11.5])
    atr = compute_atr(highs, lows, closes, period=3)
    assert atr > 0.0


def test_compute_atr_too_few_bars():
    assert compute_atr(np.array([10.0]), np.array([9.0]), np.array([9.5])) == 0.0


def test_volatility_target_size_basic():
    # target 10%, asset 25% → leverage 0.4 → $400 000 notional / $100 = 4 000 units
    result = volatility_target_size(1_000_000, 0.10, 0.25, 100.0)
    assert result == 4_000.0


def test_volatility_target_size_zero_vol():
    assert volatility_target_size(1_000_000, 0.10, 0.0, 100.0) == 0.0


def test_kelly_fraction_positive_edge():
    f = kelly_fraction(0.6, 2.0)
    assert f == pytest.approx(0.6 - 0.4 / 2.0)


def test_kelly_fraction_negative_edge():
    assert kelly_fraction(0.3, 0.5) == 0.0


def test_kelly_fraction_zero_ratio():
    assert kelly_fraction(0.6, 0.0) == 0.0


def test_kelly_position_size_basic():
    # f* = 0.6 - 0.4/1.5 = 0.333; half-Kelly = 0.167; $167 000 / $50 = 3 340 units
    result = kelly_position_size(1_000_000, 50.0, 0.6, 1.5, kelly_scale=0.5)
    assert result > 0


def test_signal_scaled_size_linear():
    assert signal_scaled_size(100, 0.5, "linear") == 50.0
    assert signal_scaled_size(100, -0.5, "linear") == -50.0


def test_signal_scaled_size_tanh():
    result = signal_scaled_size(100, 0.5, "tanh", tanh_k=2.0)
    assert 70 < result < 80  # tanh(1) ≈ 0.76


def test_signal_scaled_size_cubic():
    # 0.5^3 = 0.125 → trunc(100 × 0.125) = 12
    assert signal_scaled_size(100, 0.5, "cubic") == 12.0


def test_signal_scaled_size_clamps_signal():
    assert signal_scaled_size(100, 5.0, "linear") == 100.0   # clamps to 1.0


def test_signal_scaled_size_unknown_method():
    with pytest.raises(ValueError):
        signal_scaled_size(100, 0.5, "unknown")  # type: ignore[arg-type]


def test_inventory_skew_zero_inventory():
    assert inventory_skew_adjustment(0.0, 100.0) == 0.0


def test_inventory_skew_long():
    skew = inventory_skew_adjustment(80.0, 100.0, max_skew_ticks=5.0, tick_size=0.01)
    assert skew < 0  # long → lower quotes


def test_inventory_skew_short():
    skew = inventory_skew_adjustment(-80.0, 100.0, max_skew_ticks=5.0, tick_size=0.01)
    assert skew > 0  # short → raise quotes


def test_cap_to_limits_no_cap_needed():
    result = cap_to_limits(10.0, 100.0, 2_000.0)
    assert result.units == 10.0
    assert not result.capped


def test_cap_to_limits_caps_down():
    # 20 units × $100 = $2 000 notional, cap $1 500 → floor(1500/100) = 15 units
    result = cap_to_limits(20.0, 100.0, 1_500.0)
    assert result.units == 15.0
    assert result.capped
    assert result.notional == 1_500.0


def test_cap_to_limits_preserves_sign():
    result = cap_to_limits(-20.0, 100.0, 1_500.0)
    assert result.units == -15.0
    assert result.capped
