"""VPIN — Volume-Synchronized Probability of Informed Trading.

Easley, Lopez de Prado & O'Hara (2012).

Algorithm:
1. Accumulate trades into equal-volume buckets of size ``bucket_volume``.
2. For each bucket, classify trade volume as buy or sell via ``classifier``.
3. Bucket toxicity = |V_buy - V_sell| / bucket_volume  ∈ [0, 1].
4. Report rolling mean of toxicity over ``rolling_buckets`` buckets.

VPIN near 0 → balanced flow (safe to quote).
VPIN near 1 → one-sided informed flow (pull or widen quotes).
"""

from __future__ import annotations

from collections import deque

from .classifiers import BVCClassifier, TradeClassifier


class VPIN:
    """Rolling VPIN estimator."""

    __slots__ = (
        "_bucket_volume",
        "_rolling_buckets",
        "_classifier",
        "_bucket_buy",
        "_bucket_sell",
        "_bucket_filled",
        "_bucket_history",
        "_last",
    )

    def __init__(
        self,
        bucket_volume: float,
        rolling_buckets: int = 50,
        classifier: TradeClassifier | None = None,
    ) -> None:
        if bucket_volume <= 0:
            raise ValueError("bucket_volume must be positive")
        if rolling_buckets < 1:
            raise ValueError("rolling_buckets must be at least 1")
        self._bucket_volume = bucket_volume
        self._rolling_buckets = rolling_buckets
        self._classifier: TradeClassifier = classifier or BVCClassifier()
        self._bucket_buy: float = 0.0
        self._bucket_sell: float = 0.0
        self._bucket_filled: float = 0.0
        self._bucket_history: deque[float] = deque(maxlen=rolling_buckets)
        self._last: float | None = None

    @property
    def value(self) -> float | None:
        return self._last

    @property
    def is_ready(self) -> bool:
        return len(self._bucket_history) == self._rolling_buckets

    def update(self, price: float, volume: float) -> float | None:
        remaining = volume
        while remaining > 0.0:
            space = self._bucket_volume - self._bucket_filled
            chunk = min(remaining, space)
            buy_vol, sell_vol = self._classifier.classify(price, chunk)
            self._bucket_buy += buy_vol
            self._bucket_sell += sell_vol
            self._bucket_filled += chunk
            remaining -= chunk

            if self._bucket_filled >= self._bucket_volume:
                toxicity = abs(self._bucket_buy - self._bucket_sell) / self._bucket_volume
                self._bucket_history.append(toxicity)
                self._bucket_buy = 0.0
                self._bucket_sell = 0.0
                self._bucket_filled = 0.0

        if self._bucket_history:
            self._last = sum(self._bucket_history) / len(self._bucket_history)
        return self._last

    def serialize(self) -> dict:
        return {
            "bucket_buy": self._bucket_buy,
            "bucket_sell": self._bucket_sell,
            "bucket_filled": self._bucket_filled,
            "bucket_history": list(self._bucket_history),
            "last": self._last,
        }

    def restore(self, d: dict) -> None:
        self._bucket_buy = d.get("bucket_buy", 0.0)
        self._bucket_sell = d.get("bucket_sell", 0.0)
        self._bucket_filled = d.get("bucket_filled", 0.0)
        self._bucket_history = deque(
            d.get("bucket_history", []), maxlen=self._rolling_buckets
        )
        self._last = d.get("last")


__all__ = ["VPIN"]
