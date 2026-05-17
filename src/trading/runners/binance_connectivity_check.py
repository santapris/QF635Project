from __future__ import annotations

import asyncio
import json
from typing import Any

from trading.config import load_settings
from trading.gateways.binance.rest import BinanceREST
from trading.gateways.binance.ws import BinanceWS


async def main() -> None:
    settings = load_settings()
    print("Loaded settings:", settings)

    # REST checks
    rest = BinanceREST(settings.rest_base, settings.api_key, settings.api_secret)
    print("REST: /ping ...", await rest.ping())
    print("REST: /time ...", await rest.time())
    exi = await rest.exchange_info(settings.symbol)
    print("REST: /exchangeInfo symbol filters found:")
    if "symbols" in exi and exi["symbols"]:
        sym = exi["symbols"][0]
        print({
            "symbol": sym.get("symbol"),
            "priceFilter": next((f for f in sym.get("filters", []) if f.get("filterType") == "PRICE_FILTER"), None),
            "lotSize": next((f for f in sym.get("filters", []) if f.get("filterType") == "LOT_SIZE"), None),
            "minNotional": next((f for f in sym.get("filters", []) if f.get("filterType") == "MIN_NOTIONAL"), None),
        })

    # Public WS check: aggTrade (one message then close cleanly)
    ws = BinanceWS(settings.ws_base)
    print("WS public: connecting to aggTrade (one-shot)...")
    raw = await ws.read_one_agg_trade(settings.symbol)
    msg = json.loads(raw)
    print("WS aggTrade sample:", {k: msg.get(k) for k in ("e", "E", "s", "p", "q", "m")})

    # User data stream (requires API key)
    if settings.api_key:
        print("REST: creating listenKey ...")
        lk = await rest.user_stream_start()
        listen_key = lk.get("listenKey")
        print("listenKey:", listen_key)
        print("WS private: connecting to user data stream (one-shot or 10s timeout) ...")
        try:
            raw = await asyncio.wait_for(ws.read_one_user(listen_key), timeout=10.0)
            evt = json.loads(raw)
            etype = evt.get("e") or evt.get("event")
            print("User data event:", etype)
        except asyncio.TimeoutError:
            print("No user data events within 10s (expected if no account activity). Connectivity OK.")
    else:
        print("BINANCE_API_KEY not set — skipping private user data stream check.")


if __name__ == "__main__":
    asyncio.run(main())
