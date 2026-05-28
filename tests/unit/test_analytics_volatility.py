"""Unit tests: EWMAVolatility and ParkinsonVolatility."""

from __future__ import annotations

import math

import pytest

from trading.analytics.volatility import EWMAVolatility, ParkinsonVolatility

_T0 = 1_700_000_000_000_000_000


def _ns(seconds: float, base: int = _T0) -> int:
    return base + int(seconds * 1_000_000_000)


# --- EWMAVolatility --------------------------------------------------------


def test_ewma_constant_price_converges_zero() -> None:
    vol = EWMAVolatility(half_life_seconds=60.0)
    price = 100.0
    ts = _T0
    result = None
    for i in range(200):
        ts = _ns(float(i))
        result = vol.update(price=price, ts_ns=ts)
    # Constant price → log return = 0 → variance → 0
    assert result is not None
    assert result == pytest.approx(0.0, abs=1e-10)


def test_ewma_first_call_returns_none() -> None:
    vol = EWMAVolatility(half_life_seconds=60.0)
    assert vol.update(price=100.0, ts_ns=_T0) is None


def test_ewma_step_change_jumps_then_decays() -> None:
    vol = EWMAVolatility(half_life_seconds=10.0)
    # Warm up with constant price
    ts = _T0
    for i in range(100):
        vol.update(price=100.0, ts_ns=_ns(float(i)))

    assert vol.value == pytest.approx(0.0, abs=1e-9)

    # Step change: price jumps to 110
    ts = _ns(100.0)
    v_jump = vol.update(price=110.0, ts_ns=ts)
    assert v_jump is not None and v_jump > 0.0

    # After 3 half-lives (30s) of constant price, vol should decay significantly
    prev = v_jump
    for i in range(30):
        ts = _ns(100.0 + float(i + 1))
        curr = vol.update(price=110.0, ts_ns=ts)
        assert curr is not None
    assert curr < prev  # vol decayed


def test_ewma_zero_dt_skipped() -> None:
    vol = EWMAVolatility(half_life_seconds=60.0)
    vol.update(price=100.0, ts_ns=_T0)
    before = vol.value
    vol.update(price=105.0, ts_ns=_T0)  # same ts → skip
    assert vol.value == before


def test_ewma_invalid_price_skipped() -> None:
    vol = EWMAVolatility(half_life_seconds=60.0)
    vol.update(price=100.0, ts_ns=_T0)
    vol.update(price=0.0, ts_ns=_ns(1.0))  # zero price → skip
    assert vol.value is None


def test_ewma_serialize_restore() -> None:
    vol = EWMAVolatility(half_life_seconds=60.0)
    for i in range(10):
        vol.update(price=100.0 + i * 0.1, ts_ns=_ns(float(i)))
    state = vol.serialize()

    vol2 = EWMAVolatility(half_life_seconds=60.0)
    vol2.restore(state)
    assert vol2.value == vol.value


# --- ParkinsonVolatility ---------------------------------------------------


def test_parkinson_not_ready_before_window() -> None:
    pv = ParkinsonVolatility(window_bars=5)
    result = pv.update(high=102.0, low=98.0)
    assert result is None
    assert not pv.is_ready


def test_parkinson_balanced_flow_near_zero() -> None:
    pv = ParkinsonVolatility(window_bars=100)
    # Zero-range bars → vol = 0
    for _ in range(100):
        pv.update(high=100.0, low=100.0)
    assert pv.value == pytest.approx(0.0)


def test_parkinson_non_zero_range() -> None:
    pv = ParkinsonVolatility(window_bars=10)
    for _ in range(10):
        pv.update(high=102.0, low=98.0)
    assert pv.value is not None
    assert pv.value > 0.0


def test_parkinson_vs_realized_on_gbm() -> None:
    """Parkinson within 30% of realized vol on 1000 bars of synthetic GBM."""
    import random
    random.seed(42)
    true_vol_per_bar = 0.01  # 1% per bar
    price = 100.0
    bars = []
    for _ in range(1000):
        ret = random.gauss(0, true_vol_per_bar)
        price_end = price * math.exp(ret)
        # Approximate high/low using GBM range formula
        z = abs(random.gauss(0, 1))
        half_range = price * true_vol_per_bar * z
        bars.append((price_end + half_range, price_end - half_range))
        price = price_end

    pv = ParkinsonVolatility(window_bars=100)
    result = None
    for high, low in bars:
        if low < high:
            result = pv.update(high=high, low=low)

    assert result is not None
    # Annualization not applied → per-bar vol; compare to true_vol_per_bar
    assert abs(result - true_vol_per_bar) / true_vol_per_bar < 0.30


def test_parkinson_serialize_restore() -> None:
    pv = ParkinsonVolatility(window_bars=5)
    for i in range(5):
        pv.update(high=100.0 + i, low=98.0 + i)
    state = pv.serialize()
    pv2 = ParkinsonVolatility(window_bars=5)
    pv2.restore(state)
    assert pv2.value == pv.value
