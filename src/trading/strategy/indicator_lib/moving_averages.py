"""Moving-average indicators.

All indicators are designed for online use: feed them one observation
at a time via :meth:`update`. They expose :attr:`value` (the current
indicator output) and :attr:`is_ready` (True once enough samples have
been observed for the value to be meaningful).

Inputs and outputs are ``float`` for speed. Strategies typically convert
``Decimal`` prices to ``float`` once at the indicator boundary.
"""

from __future__ import annotations

from collections import deque


class SMA:
    """Simple moving average over a fixed window."""

    __slots__ = ("_window", "_buffer", "_sum")

    def __init__(self, period: int) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self._window = period
        self._buffer: deque[float] = deque(maxlen=period)
        self._sum = 0.0

    @property
    def is_ready(self) -> bool:
        return len(self._buffer) == self._window

    @property
    def value(self) -> float | None:
        if not self._buffer:
            return None
        return self._sum / len(self._buffer)

    def update(self, x: float) -> float | None:
        if len(self._buffer) == self._window:
            self._sum -= self._buffer[0]
        self._buffer.append(x)
        self._sum += x
        return self.value


class EMA:
    """Exponential moving average. ``alpha = 2 / (period + 1)``."""

    __slots__ = ("_alpha", "_period", "_count", "_value")

    def __init__(self, period: int) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self._period = period
        self._alpha = 2.0 / (period + 1)
        self._count = 0
        self._value: float | None = None

    @property
    def is_ready(self) -> bool:
        # Conventional rule: trust the EMA after at least ``period``
        # observations so the warm-up bias has dissipated.
        return self._count >= self._period

    @property
    def value(self) -> float | None:
        return self._value

    def update(self, x: float) -> float:
        if self._value is None:
            self._value = x
        else:
            self._value = self._alpha * x + (1.0 - self._alpha) * self._value
        self._count += 1
        return self._value


class WMA:
    """Linearly-weighted moving average. Most recent observation has weight ``period``."""

    __slots__ = ("_period", "_buffer", "_weights")

    def __init__(self, period: int) -> None:
        if period <= 0:
            raise ValueError("period must be positive")
        self._period = period
        self._buffer: deque[float] = deque(maxlen=period)
        self._weights = list(range(1, period + 1))

    @property
    def is_ready(self) -> bool:
        return len(self._buffer) == self._period

    @property
    def value(self) -> float | None:
        if not self._buffer:
            return None
        n = len(self._buffer)
        weights = self._weights[-n:]
        weighted_sum = sum(x * w for x, w in zip(self._buffer, weights))
        return weighted_sum / sum(weights)

    def update(self, x: float) -> float | None:
        self._buffer.append(x)
        return self.value


__all__ = ["EMA", "SMA", "WMA"]
