"""Binance WebSocket stream name helpers.

Binance's stream naming follows ``<symbol>@<kind>`` where symbol is
lower-cased. The helpers centralise the format so a typo in any one
place won't cause silent subscription failure (Binance accepts any
stream name and just sends nothing for unknown ones — the worst kind
of feedback).
"""

from __future__ import annotations


def book_ticker(symbol: str) -> str:
    """Best bid/ask updates. Pushed on every tick of best level."""
    return f"{symbol.lower()}@bookTicker"


def agg_trade(symbol: str) -> str:
    """Aggregated trades: one event per public trade print."""
    return f"{symbol.lower()}@aggTrade"


def trade(symbol: str) -> str:
    """Individual trades (less common; aggTrade is usually preferred)."""
    return f"{symbol.lower()}@trade"


def depth_diff(symbol: str, *, update_speed_ms: int = 100) -> str:
    """Differential depth stream.

    ``update_speed_ms`` is either 100 or 1000. 100 ms is finer but bursty.

    To maintain a correct local order book you must combine this stream
    with a REST snapshot from ``{api_prefix}/depth`` and apply the documented
    interleave rule — see :class:`DepthBookManager` (batch 4).
    """
    if update_speed_ms == 100:
        return f"{symbol.lower()}@depth@100ms"
    if update_speed_ms == 1000:
        return f"{symbol.lower()}@depth"
    raise ValueError("update_speed_ms must be 100 or 1000")


def kline(symbol: str, interval: str = "1m") -> str:
    """Candlestick stream. Useful for strategies that operate on bars."""
    return f"{symbol.lower()}@kline_{interval}"


__all__ = ["agg_trade", "book_ticker", "depth_diff", "kline", "trade"]
