from __future__ import annotations

import asyncio
import json
from typing import Any

from trading.config import load_settings
from trading.event_bus.asyncio_bus import AsyncioBus
from trading.gateways.binance.ws import BinanceWS
from trading.feed_handler.normalizer import normalize_agg_trade, normalize_depth5


async def main() -> None:
    settings = load_settings()
    ws = BinanceWS(settings.ws_base)
    bus = AsyncioBus()

    async def log_event(evt):
        print("BUS:", evt.event_type, getattr(evt, "instrument_id", None))

    await bus.subscribe("market-data", log_event)

    async def pump_aggtrade():
        # Read a handful of trades then stop
        count = 0
        stream = f"{settings.symbol}@aggTrade"
        url = f"{settings.ws_base}/public/ws/{stream}"
        async with (await __import__("websockets").connect(url)) as sock:  # type: ignore
            while count < 5:
                raw = await sock.recv()
                msg = json.loads(raw)
                evt = normalize_agg_trade(msg)
                await bus.publish("market-data", evt)
                count += 1

    async def pump_depth5():
        # Read one snapshot
        stream = f"{settings.symbol}@depth5@100ms"
        url = f"{settings.ws_base}/public/ws/{stream}"
        async with (await __import__("websockets").connect(url)) as sock:  # type: ignore
            raw = await sock.recv()
            msg = json.loads(raw)
            evt = normalize_depth5(msg)
            await bus.publish("market-data", evt)

    await asyncio.gather(pump_depth5(), pump_aggtrade())


if __name__ == "__main__":
    asyncio.run(main())

