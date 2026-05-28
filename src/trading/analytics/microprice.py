"""Microprice — imbalance-weighted mid.

Microprice = (bid * ask_size + ask * bid_size) / (bid_size + ask_size)

When bid_size == ask_size, collapses to arithmetic mid. When book is
imbalanced (e.g. ask_size >> bid_size), price leans toward the ask —
reflecting where a marginal buyer would transact. Superior to naive mid
for adverse-selection avoidance in market-making.
"""

from __future__ import annotations


class Microprice:
    """Stateless tick-by-tick microprice calculator."""

    __slots__ = ("_last",)

    def __init__(self) -> None:
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
    ) -> float | None:
        total = bid_size + ask_size
        if total <= 0.0:
            return self._last
        self._last = (bid * ask_size + ask * bid_size) / total
        return self._last

    def serialize(self) -> dict:
        return {"last": self._last}

    def restore(self, d: dict) -> None:
        self._last = d.get("last")


__all__ = ["Microprice"]
