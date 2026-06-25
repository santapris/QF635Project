"""Unit tests: GLFT quote calculator (analytics/glft.py).

Pure quote math — deterministic, no strategy/bus/clock.
"""

from __future__ import annotations

import math

import pytest

from trading.analytics.glft import GLFT, GLFTQuotes


def _expected(gamma: float, k: float, A: float, mid: float, inv: float, sigma: float):
    c1 = (1.0 / gamma) * math.log(1.0 + gamma / k)
    c2 = math.sqrt((gamma / (2.0 * A * k)) * (1.0 + gamma / k) ** (1.0 + k / gamma))
    skew = c2 * sigma
    half_spread = max(0.0, c1 + 0.5 * skew)
    reservation = mid - inv * skew
    return reservation, half_spread, skew


def test_returns_glft_quotes_dataclass() -> None:
    q = GLFT(gamma=0.2, k=1.5, A=140.0).quotes(mid=100.0, inventory=0.0, sigma=1.0)
    assert isinstance(q, GLFTQuotes)


def test_exact_values_flat_inventory() -> None:
    calc = GLFT(gamma=0.2, k=1.5, A=140.0)
    q = calc.quotes(mid=100.0, inventory=0.0, sigma=1.0)
    res, hs, skew = _expected(0.2, 1.5, 140.0, 100.0, 0.0, 1.0)
    assert q.reservation == pytest.approx(res)
    assert q.half_spread == pytest.approx(hs)
    assert q.skew_per_unit == pytest.approx(skew)
    assert q.bid == pytest.approx(res - hs)
    assert q.ask == pytest.approx(res + hs)


def test_reservation_equals_mid_when_flat() -> None:
    q = GLFT(gamma=0.2, k=1.5, A=140.0).quotes(mid=100.0, inventory=0.0, sigma=1.0)
    assert q.reservation == pytest.approx(100.0)


def test_bid_below_ask() -> None:
    q = GLFT(gamma=0.3, k=1.2, A=120.0).quotes(mid=50000.0, inventory=0.1, sigma=2.0)
    assert q.bid < q.ask


def test_half_spread_inventory_independent() -> None:
    calc = GLFT(gamma=0.2, k=1.5, A=140.0)
    flat = calc.quotes(mid=100.0, inventory=0.0, sigma=1.0)
    long = calc.quotes(mid=100.0, inventory=0.5, sigma=1.0)
    short = calc.quotes(mid=100.0, inventory=-0.5, sigma=1.0)
    assert flat.half_spread == pytest.approx(long.half_spread)
    assert flat.half_spread == pytest.approx(short.half_spread)


def test_reservation_skews_down_when_long() -> None:
    calc = GLFT(gamma=0.2, k=1.5, A=140.0)
    flat = calc.quotes(mid=100.0, inventory=0.0, sigma=1.0)
    long = calc.quotes(mid=100.0, inventory=1.0, sigma=1.0)
    assert long.reservation < flat.reservation


def test_reservation_skews_up_when_short() -> None:
    calc = GLFT(gamma=0.2, k=1.5, A=140.0)
    flat = calc.quotes(mid=100.0, inventory=0.0, sigma=1.0)
    short = calc.quotes(mid=100.0, inventory=-1.0, sigma=1.0)
    assert short.reservation > flat.reservation


def test_skew_scales_with_vol() -> None:
    calc = GLFT(gamma=0.2, k=1.5, A=140.0)
    lo = calc.quotes(mid=100.0, inventory=1.0, sigma=1.0)
    hi = calc.quotes(mid=100.0, inventory=1.0, sigma=4.0)
    # Larger vol -> larger skew -> reservation pulled further from mid.
    assert (100.0 - hi.reservation) > (100.0 - lo.reservation)


@pytest.mark.parametrize("bad", [
    {"gamma": 0.0, "k": 1.5, "A": 140.0},
    {"gamma": -1.0, "k": 1.5, "A": 140.0},
    {"gamma": 0.2, "k": 0.0, "A": 140.0},
    {"gamma": 0.2, "k": 1.5, "A": 0.0},
])
def test_invalid_params_raise(bad: dict) -> None:
    with pytest.raises(ValueError):
        GLFT(**bad)
