from __future__ import annotations

import asyncio
import json

import structlog

from trading.config import load_settings
from trading.order_gateways.binance.rest import BinanceREST
from trading.order_gateways.binance.ws import BinanceWS

log = structlog.get_logger(__name__)


async def main() -> None:
    settings = load_settings()
    log.info("settings_loaded", environment=settings.environment, symbol=settings.symbol)

    rest = BinanceREST(settings.futures_rest_base, settings.api_key, settings.api_secret)
    log.info("rest_ping", result=await rest.ping())
    log.info("rest_time", result=await rest.time())

    exi = await rest.exchange_info(settings.symbol)
    if "symbols" in exi and exi["symbols"]:
        sym = exi["symbols"][0]
        log.info(
            "rest_exchange_info",
            symbol=sym.get("symbol"),
            price_filter=next((f for f in sym.get("filters", []) if f.get("filterType") == "PRICE_FILTER"), None),
            lot_size=next((f for f in sym.get("filters", []) if f.get("filterType") == "LOT_SIZE"), None),
            min_notional=next((f for f in sym.get("filters", []) if f.get("filterType") == "MIN_NOTIONAL"), None),
        )

    ws = BinanceWS(settings.futures_ws_base)
    log.info("ws_connecting", stream="aggTrade", mode="one-shot")
    raw = await ws.read_one_agg_trade(settings.symbol)
    msg = json.loads(raw)
    log.info("ws_agg_trade_sample", **{k: msg.get(k) for k in ("e", "E", "s", "p", "q", "m")})

    if settings.api_key:
        log.info("rest_creating_listen_key")
        lk = await rest.user_stream_start()
        listen_key = lk.get("listenKey")
        log.info("ws_user_stream_connecting", listen_key=listen_key)
        try:
            raw = await asyncio.wait_for(ws.read_one_user(listen_key), timeout=10.0)
            evt = json.loads(raw)
            log.info("ws_user_data_event", event_type=evt.get("e") or evt.get("event"))
        except asyncio.TimeoutError:
            log.info("ws_user_data_timeout", note="no account activity within 10s — connectivity OK")
    else:
        log.warning("ws_user_stream_skipped", reason="BINANCE_API_KEY not set")


if __name__ == "__main__":
    asyncio.run(main())
