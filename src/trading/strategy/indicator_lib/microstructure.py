"""Microstructure and volatility indicators."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


class RollingStdDev:
    """Rolling standard deviation over a fixed window.

    Uses a windowed implementation with running sums. We do *not* use
    Welford's algorithm here because Welford is for streams without
    eviction — once we drop the oldest sample, Welford's ``M2`` is
    invalidated. The naive sum/sum-of-squares approach is fine for the
    window sizes typical in trading (10s–thousands).

    Returns the *sample* standard deviation (n-1 denominator) once the
    window has at least 2 observations.
    """

    __slots__ = ("_period", "_buffer", "_sum", "_sum_sq")

    def __init__(self, period: int) -> None:
        if period < 2:
            raise ValueError("period must be at least 2")
        self._period = period
        self._buffer: deque[float] = deque(maxlen=period)
        self._sum = 0.0
        self._sum_sq = 0.0

    @property
    def is_ready(self) -> bool:
        return len(self._buffer) == self._period

    @property
    def mean(self) -> float | None:
        if not self._buffer:
            return None
        return self._sum / len(self._buffer)

    @property
    def value(self) -> float | None:
        n = len(self._buffer)
        if n < 2:
            return None
        mean = self._sum / n
        # Bessel's correction: n-1 denominator.
        var = max(0.0, (self._sum_sq - n * mean * mean) / (n - 1))
        return var ** 0.5

    def update(self, x: float) -> float | None:
        if len(self._buffer) == self._period:
            old = self._buffer[0]
            self._sum -= old
            self._sum_sq -= old * old
        self._buffer.append(x)
        self._sum += x
        self._sum_sq += x * x
        return self.value


@dataclass(slots=True)
class BollingerOutput:
    upper: float
    middle: float  # SMA
    lower: float


class Bollinger:
    """Bollinger Bands: SMA(period) ± num_std * stddev(period)."""

    __slots__ = ("_period", "_num_std", "_stddev")

    def __init__(self, period: int = 20, num_std: float = 2.0) -> None:
        self._period = period
        self._num_std = num_std
        self._stddev = RollingStdDev(period)

    @property
    def is_ready(self) -> bool:
        return self._stddev.is_ready

    @property
    def value(self) -> BollingerOutput | None:
        if not self._stddev.is_ready:
            return None
        mean = self._stddev.mean
        std = self._stddev.value
        if mean is None or std is None:
            return None
        return BollingerOutput(
            upper=mean + self._num_std * std,
            middle=mean,
            lower=mean - self._num_std * std,
        )

    def update(self, x: float) -> BollingerOutput | None:
        self._stddev.update(x)
        return self.value


class VWAP:
    """Volume-weighted average price over a rolling window of trades.

    For session VWAP (resets daily), construct a fresh instance at the
    session boundary; this class does not own session logic.
    """

    __slots__ = ("_window", "_buffer", "_pv_sum", "_v_sum")

    def __init__(self, window: int) -> None:
        if window <= 0:
            raise ValueError("window must be positive")
        self._window = window
        # Each entry: (price * volume, volume)
        self._buffer: deque[tuple[float, float]] = deque(maxlen=window)
        self._pv_sum = 0.0
        self._v_sum = 0.0

    @property
    def is_ready(self) -> bool:
        # Useful even at one observation; we still report is_ready when
        # the buffer is full so consumers know the window has saturated.
        return len(self._buffer) == self._window

    @property
    def value(self) -> float | None:
        if self._v_sum == 0:
            return None
        return self._pv_sum / self._v_sum

    def update(self, price: float, volume: float) -> float | None:
        if volume <= 0:
            # Zero-volume trades pollute VWAP; ignore them.
            return self.value
        if len(self._buffer) == self._window:
            old_pv, old_v = self._buffer[0]
            self._pv_sum -= old_pv
            self._v_sum -= old_v
        pv = price * volume
        self._buffer.append((pv, volume))
        self._pv_sum += pv
        self._v_sum += volume
        return self.value


__all__ = ["Bollinger", "BollingerOutput", "RollingStdDev", "VWAP"]
