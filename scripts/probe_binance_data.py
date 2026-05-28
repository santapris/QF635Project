"""Probe Binance Futures testnet: capture sample market data + REST account/orders.

Two phases:
  1. Open public WS for ~10s, dump first N raw frames per stream type.
  2. Hit REST: futures account, open orders, position risk, recent trades.

Prints structured JSON so you can eyeball field shapes and sanity-check
that the data is what the strategy expects.

Usage: .venv/bin/python -m scripts.probe_binance_data
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict
from decimal import Decimal

from trading.config import load_settings
from trading.core import LiveClock
from trading.order_gateways.binance import (
    BinanceConfig,
    BinanceCredentials,
    BinancePublicWSConnector,
    BinanceRESTClient,
)
from trading.order_gateways.binance import stream_names


SAMPLES_PER_STREAM = 3
CAPTURE_SECONDS = 8.0


async def capture_ws(config: BinanceConfig, clock: LiveClock) -> dict[str, list[dict]]:
    streams = [stream_names.book_ticker("btcusdt"), stream_names.agg_trade("btcusdt")]
    samples: dict[str, list[dict]] = defaultdict(list)
    print(f"\n=== WS capture: {streams} for {CAPTURE_SECONDS}s ===\n")

    conn = BinancePublicWSConnector(
        config=config, streams=streams, clock=clock, source="probe",
    )
    deadline = time.monotonic() + CAPTURE_SECONDS

    async def _run() -> None:
        async for raw in conn.connect():
            try:
                msg = json.loads(raw.payload)
            except Exception:
                continue
            stream = msg.get("stream", "unknown")
            data = msg.get("data", msg)
            if len(samples[stream]) < SAMPLES_PER_STREAM:
                samples[stream].append(data)
            if all(len(samples[s]) >= SAMPLES_PER_STREAM for s in streams):
                break

    try:
        await asyncio.wait_for(_run(), timeout=CAPTURE_SECONDS)
    except asyncio.TimeoutError:
        pass
    finally:
        try:
            await conn.close()
        except Exception:
            pass

    for stream, msgs in samples.items():
        print(f"\n--- {stream} ({len(msgs)} samples) ---")
        for i, m in enumerate(msgs):
            print(f"[{i}] {json.dumps(m, indent=2)}")
    return samples


async def probe_rest(rest: BinanceRESTClient, config: BinanceConfig) -> None:
    print("\n=== REST probes ===\n")

    async def call(label: str, method: str, path: str, params: dict | None = None,
                   weight: float = 1.0, signed: bool = True):
        try:
            res = await rest.request(method, path, params=params, signed=signed, weight=weight)
            txt = json.dumps(res, indent=2, default=str)
            if len(txt) > 2000:
                txt = txt[:2000] + f"\n... [truncated, total {len(txt)} chars]"
            print(f"\n--- {label}  ({method} {path}) ---\n{txt}\n")
        except Exception as e:
            print(f"\n--- {label}  ({method} {path}) ---\nERROR: {type(e).__name__}: {e}\n")

    # Futures account (USD-M)
    await call("account", "GET", config.account_path, weight=5.0)
    # Position risk
    await call("positionRisk", "GET", "/fapi/v2/positionRisk",
               params={"symbol": "BTCUSDT"}, weight=5.0)
    # Open orders (BTCUSDT)
    await call("openOrders", "GET", "/fapi/v1/openOrders",
               params={"symbol": "BTCUSDT"}, weight=1.0)
    # All orders (recent)
    await call("allOrders (last 5)", "GET", "/fapi/v1/allOrders",
               params={"symbol": "BTCUSDT", "limit": 5}, weight=5.0)
    # User trades (recent)
    await call("userTrades (last 5)", "GET", "/fapi/v1/userTrades",
               params={"symbol": "BTCUSDT", "limit": 5}, weight=5.0)
    # Commission rate (helps verify maker/taker fee assumptions)
    await call("commissionRate", "GET", "/fapi/v1/commissionRate",
               params={"symbol": "BTCUSDT"}, weight=20.0)
    # Public: orderbook depth (top 5)
    await call("depth (top 5)", "GET", "/fapi/v1/depth",
               params={"symbol": "BTCUSDT", "limit": 5}, weight=2.0, signed=False)
    # Public: exchange info filters (tick size, lot size, min notional)
    await call("exchangeInfo filters (BTCUSDT)", "GET", "/fapi/v1/exchangeInfo",
               weight=1.0, signed=False)


async def amain() -> int:
    settings = load_settings()
    config = BinanceConfig.from_settings(settings)
    print(f"futures={config.futures}  rest_base={config.rest_base_url}  ws_base={config.ws_base_url}")
    print(f"account_path={config.account_path}")
    creds = BinanceCredentials(api_key=settings.api_key, api_secret=settings.api_secret)
    clock = LiveClock()

    rest = BinanceRESTClient(config=config, credentials=creds, clock=clock)
    await rest.connect()
    try:
        await probe_rest(rest, config)
        await capture_ws(config, clock)
    finally:
        await rest.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(amain()))
