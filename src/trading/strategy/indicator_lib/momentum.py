"""Momentum indicators."""

from __future__ import annotations

from .moving_averages import EMA


class RSI:
    """Relative Strength Index using Wilder's smoothing.

    Wilder's smoothing is an EMA with ``alpha = 1 / period``, applied
    separately to gains and losses. The classical RSI period is 14.
    """

    __slots__ = ("_period", "_alpha", "_avg_gain", "_avg_loss", "_prev", "_count")

    def __init__(self, period: int = 14) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self._period = period
        self._alpha = 1.0 / period
        self._avg_gain = 0.0
        self._avg_loss = 0.0
        self._prev: float | None = None
        self._count = 0

    @property
    def is_ready(self) -> bool:
        return self._count > self._period

    @property
    def value(self) -> float | None:
        if self._prev is None or self._count <= 1:
            return None
        if self._avg_loss == 0:
            return 100.0
        rs = self._avg_gain / self._avg_loss
        return 100.0 - 100.0 / (1.0 + rs)

    def update(self, x: float) -> float | None:
        if self._prev is None:
            self._prev = x
            self._count = 1
            return None

        change = x - self._prev
        gain = max(change, 0.0)
        loss = max(-change, 0.0)

        if self._count <= self._period:
            # Warm-up: simple averaging until we have ``period`` deltas.
            n = self._count
            self._avg_gain = (self._avg_gain * (n - 1) + gain) / n
            self._avg_loss = (self._avg_loss * (n - 1) + loss) / n
        else:
            self._avg_gain = self._alpha * gain + (1.0 - self._alpha) * self._avg_gain
            self._avg_loss = self._alpha * loss + (1.0 - self._alpha) * self._avg_loss

        self._prev = x
        self._count += 1
        return self.value


class MACD:
    """Moving Average Convergence Divergence.

    Returns ``(macd, signal, histogram)``. Classical params: 12, 26, 9.
    """

    __slots__ = ("_fast", "_slow", "_signal_ema", "_macd")

    def __init__(
        self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9
    ) -> None:
        if fast_period >= slow_period:
            raise ValueError("fast_period must be < slow_period")
        self._fast = EMA(fast_period)
        self._slow = EMA(slow_period)
        self._signal_ema = EMA(signal_period)
        self._macd: float | None = None

    @property
    def is_ready(self) -> bool:
        return (
            self._fast.is_ready
            and self._slow.is_ready
            and self._signal_ema.is_ready
        )

    @property
    def value(self) -> tuple[float, float, float] | None:
        if not self.is_ready or self._macd is None:
            return None
        signal = self._signal_ema.value
        if signal is None:
            return None
        return self._macd, signal, self._macd - signal

    def update(self, x: float) -> tuple[float, float, float] | None:
        fast = self._fast.update(x)
        slow = self._slow.update(x)
        if fast is None or slow is None:
            return None
        self._macd = fast - slow
        self._signal_ema.update(self._macd)
        return self.value


__all__ = ["MACD", "RSI"]
