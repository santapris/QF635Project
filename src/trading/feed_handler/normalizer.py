from __future__ import annotations

from typing import Dict, List, Tuple

from trading.core.events import OrderBookEvent, TradeEvent


def normalize_agg_trade(msg: Dict) -> TradeEvent:
    """Normalize Binance aggTrade WS message to TradeEvent.

    Example msg fields:
      e: 'aggTrade'
      E: 123456789,  # Event time
      s: 'BTCUSDT',  # Symbol
      p: '123.45',   # Price
      q: '0.10',     # Quantity
      m: true        # Is buyer the market maker? (True => seller aggressive)
      a: 12345       # Aggregate tradeId
    """
    side = "sell" if msg.get("m") else "buy"
    return TradeEvent(
        instrument_id=msg.get("s", "").replace("_", "-"),
        price=float(msg.get("p", 0.0)),
        quantity=float(msg.get("q", 0.0)),
        side=side,
        trade_id=str(msg.get("a")),
        exchange="binance",
    )


def normalize_depth5(msg: Dict) -> OrderBookEvent:
    """Normalize Binance @depth5@100ms WS message to OrderBookEvent.

    Example msg fields:
      e: 'depthUpdate' or null (depth5 may omit e)
      E: 123456789   # Event time
      s: 'BTCUSDT'   # Symbol
      b: [[price, qty], ...]  # Bids
      a: [[price, qty], ...]  # Asks
    """
    bids: List[Tuple[float, float]] = [
        (float(p), float(q)) for p, q in msg.get("b", [])
    ]
    asks: List[Tuple[float, float]] = [
        (float(p), float(q)) for p, q in msg.get("a", [])
    ]
    return OrderBookEvent(
        instrument_id=msg.get("s", "").replace("_", "-"),
        exchange="binance",
        bids=bids,
        asks=asks,
        is_snapshot=True,
    )

