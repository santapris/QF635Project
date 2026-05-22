"""Order-type and side translation between our schema and Binance.

Our :class:`OrderType` enum uses generic names; Binance uses its own
strings. Keep the translation here so the gateway code doesn't sprinkle
``if order_type is ...`` chains everywhere.

Binance Spot order types: ``LIMIT``, ``MARKET``, ``STOP_LOSS``,
``STOP_LOSS_LIMIT``, ``TAKE_PROFIT``, ``TAKE_PROFIT_LIMIT``, ``LIMIT_MAKER``.

Notable mappings:
- ``POST_ONLY`` -> ``LIMIT_MAKER`` (Binance's name for post-only LIMIT)
- ``IOC`` / ``FOK`` aren't standalone types on Binance — they're TIF flags
  on a LIMIT order. We translate by emitting ``LIMIT`` with the right TIF.
"""

from __future__ import annotations

from ...core.exceptions import OrderError
from ...core.types import OrderType, Side, TimeInForce


_OUR_SIDE_TO_BINANCE: dict[Side, str] = {
    Side.BUY: "BUY",
    Side.SELL: "SELL",
}

_BINANCE_TO_OUR_SIDE: dict[str, Side] = {v: k for k, v in _OUR_SIDE_TO_BINANCE.items()}


def side_to_binance(side: Side) -> str:
    return _OUR_SIDE_TO_BINANCE[side]


def side_from_binance(s: str) -> Side:
    try:
        return _BINANCE_TO_OUR_SIDE[s.upper()]
    except KeyError as e:
        raise OrderError(f"unknown binance side: {s!r}") from e


def order_type_to_binance(
    order_type: OrderType, time_in_force: TimeInForce
) -> tuple[str, TimeInForce]:
    """Translate ``(order_type, tif)`` to Binance's ``(type, timeInForce)``.

    Returns the pair to embed in the request. For MARKET orders, TIF is
    omitted (Binance rejects MARKET with TIF set); we return ``GTC`` as
    a sentinel that the caller should skip including.
    """
    if order_type is OrderType.MARKET:
        return "MARKET", time_in_force  # caller omits TIF for MARKET
    if order_type is OrderType.LIMIT:
        return "LIMIT", time_in_force
    if order_type is OrderType.IOC:
        # IOC is a TIF on a LIMIT order at Binance.
        return "LIMIT", TimeInForce.IOC
    if order_type is OrderType.FOK:
        return "LIMIT", TimeInForce.FOK
    if order_type is OrderType.POST_ONLY:
        # LIMIT_MAKER is post-only; TIF is implicit (rejected if would cross).
        return "LIMIT_MAKER", time_in_force
    if order_type is OrderType.STOP:
        return "STOP_LOSS", time_in_force
    if order_type is OrderType.STOP_LIMIT:
        return "STOP_LOSS_LIMIT", time_in_force
    raise OrderError(f"unsupported order type for Binance: {order_type}")


def tif_to_binance(tif: TimeInForce) -> str:
    if tif is TimeInForce.GTC:
        return "GTC"
    if tif is TimeInForce.IOC:
        return "IOC"
    if tif is TimeInForce.FOK:
        return "FOK"
    raise OrderError(f"unsupported time-in-force for Binance: {tif}")


__all__ = [
    "order_type_to_binance",
    "side_from_binance",
    "side_to_binance",
    "tif_to_binance",
]
