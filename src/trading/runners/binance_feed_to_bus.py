from __future__ import annotations

import asyncio
import json

import structlog

from trading.config import load_settings
from trading.event_bus.asyncio_bus import AsyncioBus
from trading.gateways.binance.ws import BinanceWS
from trading.feed_handler.normalizer import normalize_agg_trade, normalize_depth5

log = structlog.get_logger(__name__)


async def main() -> None:
    settings = load_settings()
    ws = BinanceWS(settings.ws_base)
    bus = AsyncioBus()

    async def log_event(evt):
        log.debug("bus_event", event_type=evt.event_type, instrument_id=getattr(evt, "instrument_id", None))

    await bus.subscribe("market-data", log_event)

    async def pump_aggtrade():
        count = 0
        async for raw in ws.agg_trade(settings.symbol):
            evt = normalize_agg_trade(json.loads(raw))
            await bus.publish("market-data", evt)
            count += 1
            if count >= 5:
                break

    async def pump_depth5():
        async for raw in ws.depth5(settings.symbol):
            evt = normalize_depth5(json.loads(raw))
            await bus.publish("market-data", evt)
            break

    await asyncio.gather(pump_depth5(), pump_aggtrade())


if __name__ == "__main__":
    asyncio.run(main())

