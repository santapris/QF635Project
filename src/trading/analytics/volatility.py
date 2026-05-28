"""Volatility estimators: EWMA (tick-based) and Parkinson (range-based).

EWMAVolatility — exponentially weighted volatility on log-returns.
    λ = exp(-Δt / half_life). Single state variable, no fixed window.
    Returns annualized vol (or per-second with annualization_seconds=1).

ParkinsonVolatility — range-based estimator using high/low per bar.
    σ² = (1/(4 ln 2)) × mean[(ln H/L)²]
    More efficient than close-to-close; caller is responsible for
    bar aggregation and feeding (high, low) per completed bar.
"""

from __future__ import annotations

import math
from collections import deque


_LN2 = math.log(2.0)
_PARKINSON_CONST = 1.0 / (4.0 * _LN2)


class EWMAVolatility:
    """EWMA vol on log-returns, annualized by default.

    Updates on every tick; no fixed window. Suitable for HFT contexts
    where bar aggregation would introduce lag.
    """

    __slots__ = (
        "_half_life_ns",
        "_annualization_factor",
        "_ewma_var",
        "_prev_price",
        "_prev_ts_ns",
        "_last",
    )

    def __init__(
        self,
        half_life_seconds: float,
        annualization_seconds: float = 365 * 24 * 3600.0,
    ) -> None:
        if half_life_seconds <= 0:
            raise ValueError("half_life_seconds must be positive")
        self._half_life_ns: float = half_life_seconds * 1_000_000_000.0
        self._annualization_factor: float = annualization_seconds
        self._ewma_var: float = 0.0
        self._prev_price: float | None = None
        self._prev_ts_ns: int | None = None
        self._last: float | None = None

    @property
    def value(self) -> float | None:
        return self._last

    def update(self, price: float, ts_ns: int) -> float | None:
        if price <= 0.0:
            return self._last

        if self._prev_price is None or self._prev_ts_ns is None:
            self._prev_price = price
            self._prev_ts_ns = ts_ns
            return None

        dt_ns = ts_ns - self._prev_ts_ns
        if dt_ns <= 0:
            return self._last

        log_ret = math.log(price / self._prev_price)
        # Annualize the squared return: scale by (annualization / dt_seconds)
        dt_seconds = dt_ns / 1_000_000_000.0
        ann_sq_ret = (log_ret ** 2) * (self._annualization_factor / dt_seconds)

        lam = math.exp(-dt_ns / self._half_life_ns)
        self._ewma_var = lam * self._ewma_var + (1.0 - lam) * ann_sq_ret

        self._prev_price = price
        self._prev_ts_ns = ts_ns
        self._last = math.sqrt(max(0.0, self._ewma_var))
        return self._last

    def serialize(self) -> dict:
        return {
            "ewma_var": self._ewma_var,
            "prev_price": self._prev_price,
            "prev_ts_ns": self._prev_ts_ns,
            "last": self._last,
        }

    def restore(self, d: dict) -> None:
        self._ewma_var = d.get("ewma_var", 0.0)
        self._prev_price = d.get("prev_price")
        self._prev_ts_ns = d.get("prev_ts_ns")
        self._last = d.get("last")


class ParkinsonVolatility:
    """Range-based vol estimator. Caller feeds one (high, low) per bar.

    Returns annualized vol once ``window_bars`` bars have been seen.
    Caller must pass annualization_factor = bars_per_year to annualize;
    default 1.0 returns the raw per-bar vol.
    """

    __slots__ = ("_window", "_buffer", "_annualization_factor", "_last")

    def __init__(
        self,
        window_bars: int,
        annualization_factor: float = 1.0,
    ) -> None:
        if window_bars < 1:
            raise ValueError("window_bars must be at least 1")
        self._window = window_bars
        self._buffer: deque[float] = deque(maxlen=window_bars)
        self._annualization_factor = annualization_factor
        self._last: float | None = None

    @property
    def is_ready(self) -> bool:
        return len(self._buffer) == self._window

    @property
    def value(self) -> float | None:
        return self._last

    def update(self, high: float, low: float) -> float | None:
        if high <= 0.0 or low <= 0.0 or high < low:
            return self._last
        if high == low:
            # Zero-range bar — contributes 0 to the mean but still counts.
            self._buffer.append(0.0)
        else:
            log_hl = math.log(high / low)
            self._buffer.append(log_hl ** 2)

        if not self.is_ready:
            return None

        mean_sq = sum(self._buffer) / self._window
        var = _PARKINSON_CONST * mean_sq * self._annualization_factor
        self._last = math.sqrt(max(0.0, var))
        return self._last

    def serialize(self) -> dict:
        return {
            "buffer": list(self._buffer),
            "last": self._last,
        }

    def restore(self, d: dict) -> None:
        self._buffer = deque(d.get("buffer", []), maxlen=self._window)
        self._last = d.get("last")


__all__ = ["EWMAVolatility", "ParkinsonVolatility"]
