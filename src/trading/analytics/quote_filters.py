"""Quote filters — final guards before order emission.

All functions operate on Decimal to preserve price precision.
"""

from __future__ import annotations

from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal

from ..core.types import Side


def round_to_tick(price: Decimal, tick_size: Decimal) -> Decimal:
    """Round price to the nearest valid tick (ROUND_HALF_UP)."""
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    return (price / tick_size).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick_size


def round_to_lot(qty: Decimal, lot_size: Decimal) -> Decimal:
    """Round quantity down to the nearest valid lot (truncate, never overshoot)."""
    if lot_size <= 0:
        raise ValueError("lot_size must be positive")
    return (qty / lot_size).quantize(Decimal("1"), rounding=ROUND_DOWN) * lot_size


def passes_min_notional(
    price: Decimal,
    qty: Decimal,
    min_notional: Decimal,
) -> bool:
    """True if price * qty >= min_notional."""
    return price * qty >= min_notional


def post_only_guard(
    side: Side,
    our_price: Decimal,
    best_bid: Decimal,
    best_ask: Decimal,
) -> bool:
    """True if the quote will rest as a maker (will not cross the book).

    BUY : our_price must be strictly below the best ask.
    SELL: our_price must be strictly above the best bid.
    """
    if side is Side.BUY:
        return our_price < best_ask
    return our_price > best_bid


__all__ = [
    "passes_min_notional",
    "post_only_guard",
    "round_to_lot",
    "round_to_tick",
]
