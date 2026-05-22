"""Lot — one open position fragment at a single price.

Used by FIFO/LIFO accounting books to track which slice of inventory
gets closed first. ``quantity`` is always positive; the book itself
tracks whether the lots represent a long or short position (one book
never mixes sides).
"""

from __future__ import annotations

from dataclasses import dataclass

from ..core.types import Price, Quantity


@dataclass(slots=True)
class Lot:
    quantity: Quantity  # always positive
    price: Price


__all__ = ["Lot"]
