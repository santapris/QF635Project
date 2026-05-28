"""Order book imbalance indicators: OBI and OFI.

OBI — Order Book Imbalance (top-of-book snapshot):
    OBI = (bid_size - ask_size) / (bid_size + ask_size)  ∈ [-1, +1]
    Instantaneous measure of quote-level pressure.

OFI — Order Flow Imbalance (rolling time-window):
    Cont & Kukanov (2013) definition. Tracks net directional pressure
    at the best bid/ask over a rolling window. Stronger predictor of
    short-term price moves than OBI; needs clock injection for windowing.
"""

from __future__ import annotations

import math
from collections import deque

from ..core.clock import Clock


class OBI:
    """Order Book Imbalance at the top of book."""

    __slots__ = ("_last",)

    def __init__(self) -> None:
        self._last: float | None = None

    @property
    def value(self) -> float | None:
        return self._last

    def update(self, bid_size: float, ask_size: float) -> float | None:
        total = bid_size + ask_size
        if total <= 0.0:
            return self._last
        self._last = (bid_size - ask_size) / total
        return self._last

    def serialize(self) -> dict:
        return {"last": self._last}

    def restore(self, d: dict) -> None:
        self._last = d.get("last")


class OFI:
    """Order Flow Imbalance over a rolling time window.

    For each top-of-book update computes the signed delta:
        e = Δbid_component - Δask_component
    and maintains a rolling sum over ``window_seconds``.

    Bid component:  +bid_size if bid price rose, -bid_size if fell, 0 if same.
    Ask component:  -ask_size if ask price rose, +ask_size if fell, 0 if same.

    Positive OFI → net buying pressure → price likely to rise.
    """

    __slots__ = (
        "_window_ns",
        "_clock",
        "_events",
        "_running_sum",
        "_prev_bid",
        "_prev_bid_size",
        "_prev_ask",
        "_prev_ask_size",
        "_last",
    )

    def __init__(self, window_seconds: float, clock: Clock) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._window_ns: int = int(window_seconds * 1_000_000_000)
        self._clock = clock
        # Each entry: (ts_ns, delta)
        self._events: deque[tuple[int, float]] = deque()
        self._running_sum: float = 0.0
        self._prev_bid: float | None = None
        self._prev_bid_size: float | None = None
        self._prev_ask: float | None = None
        self._prev_ask_size: float | None = None
        self._last: float | None = None

    @property
    def value(self) -> float | None:
        return self._last

    def update(
        self,
        bid: float,
        bid_size: float,
        ask: float,
        ask_size: float,
        ts_ns: int,
    ) -> float | None:
        delta = 0.0
        if self._prev_bid is not None:
            if bid > self._prev_bid:
                delta += bid_size
            elif bid < self._prev_bid:
                delta -= bid_size
            # bid unchanged → 0

            if ask > self._prev_ask:  # type: ignore[operator]
                delta -= ask_size
            elif ask < self._prev_ask:  # type: ignore[operator]
                delta += ask_size
            # ask unchanged → 0

            self._events.append((ts_ns, delta))
            self._running_sum += delta

        self._prev_bid = bid
        self._prev_bid_size = bid_size
        self._prev_ask = ask
        self._prev_ask_size = ask_size

        # Evict expired events
        cutoff = ts_ns - self._window_ns
        while self._events and self._events[0][0] < cutoff:
            _, old_delta = self._events.popleft()
            self._running_sum -= old_delta

        if self._events:
            self._last = self._running_sum
        return self._last

    def serialize(self) -> dict:
        return {
            "events": list(self._events),
            "running_sum": self._running_sum,
            "prev_bid": self._prev_bid,
            "prev_bid_size": self._prev_bid_size,
            "prev_ask": self._prev_ask,
            "prev_ask_size": self._prev_ask_size,
            "last": self._last,
        }

    def restore(self, d: dict) -> None:
        self._events = deque(tuple(e) for e in d.get("events", []))
        self._running_sum = d.get("running_sum", 0.0)
        self._prev_bid = d.get("prev_bid")
        self._prev_bid_size = d.get("prev_bid_size")
        self._prev_ask = d.get("prev_ask")
        self._prev_ask_size = d.get("prev_ask_size")
        self._last = d.get("last")


__all__ = ["OBI", "OFI"]
