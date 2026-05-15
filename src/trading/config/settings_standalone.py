import os
from dataclasses import dataclass

from trading.config.settings import _as_bool, _maybe_load_from_vault, Settings


def load_settings() -> Settings:
    market = os.getenv("BINANCE_MARKET", "futures").strip().lower()
    symbol = os.getenv("BINANCE_SYMBOL", "btcusdt").strip().lower()
    testnet = _as_bool(os.getenv("BINANCE_TESTNET"), True)
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        vk, vs = _maybe_load_from_vault()
        api_key = api_key or vk
        api_secret = api_secret or vs

    if market != "futures":
        raise ValueError("Only BINANCE_MARKET=futures is supported in this scaffold")

    if testnet:
        rest_base = "https://testnet.binancefuture.com"
        ws_public_base = "wss://stream.binancefuture.com"
        ws_user_base = "wss://stream.binancefuture.com/ws"
    else:
        rest_base = "https://fapi.binance.com"
        ws_public_base = "wss://fstream.binance.com"
        ws_user_base = "wss://fstream.binance.com/ws"

    return Settings(
        market=market,
        symbol=symbol,
        testnet=testnet,
        api_key=api_key,
        api_secret=api_secret,
        rest_base=rest_base,
        ws_public_base=ws_public_base,
        ws_user_base=ws_user_base,
    )

