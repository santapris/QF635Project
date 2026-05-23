"""Batch 5: BinanceOrderGateway.cancel_stale_orders() startup sync.

Tests use a stub REST client that records calls rather than making real
network requests.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from trading.core import AssetType, Instrument, LiveClock
from trading.order_gateways.binance.config import BinanceConfig, BinanceCredentials
from trading.order_gateways.binance.order_gateway import BinanceOrderGateway
from trading.order_gateways.binance.symbols import SymbolMapper


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def btc_inst() -> Instrument:
    return Instrument(
        symbol="BTC-USDT", exchange="BINANCE", asset_type=AssetType.SPOT,
        base_currency="BTC", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.00001"),
    )


class _NullBus:
    async def publish(self, topic, event): pass
    async def subscribe(self, topic, handler): pass
    async def start(self): pass
    async def stop(self): pass


def _make_gw(btc_inst, rest_client) -> BinanceOrderGateway:
    cfg = BinanceConfig(
        spot_rest_base="https://testnet.binance.vision",
        spot_ws_base="wss://testnet.binance.vision",
        futures_rest_base="https://demo-fapi.binance.com",
        futures_ws_base="wss://fstream.binancefuture.com",
    )
    creds = BinanceCredentials(api_key="k", api_secret="s")
    symbols = SymbolMapper([btc_inst])
    return BinanceOrderGateway(
        bus=_NullBus(), clock=LiveClock(),
        config=cfg, credentials=creds, symbols=symbols,
        rest_client=rest_client,
    )


# ---------------------------------------------------------------------------
# 5.6  cancel_stale_orders returns 0 when no open orders
# ---------------------------------------------------------------------------

async def test_cancel_stale_orders_none_open(btc_inst) -> None:
    class _EmptyREST:
        calls: list = []
        async def connect(self): pass
        async def close(self): pass
        async def request(self, method, path, **kwargs):
            self.calls.append((method, path, kwargs))
            if method == "GET":
                return []  # no open orders
            return {}

    rest = _EmptyREST()
    gw = _make_gw(btc_inst, rest)
    count = await gw.cancel_stale_orders()
    assert count == 0
    # Should have made exactly one GET call (for BTCUSDT)
    get_calls = [(m, p) for m, p, _ in rest.calls if m == "GET"]
    assert len(get_calls) == 1
    assert get_calls[0][1] == "/api/v3/openOrders"


# ---------------------------------------------------------------------------
# 5.7  cancel_stale_orders cancels all returned orders
# ---------------------------------------------------------------------------

async def test_cancel_stale_orders_cancels_found(btc_inst) -> None:
    open_orders = [
        {"orderId": 101, "symbol": "BTCUSDT"},
        {"orderId": 102, "symbol": "BTCUSDT"},
    ]

    class _TwoOrderREST:
        calls: list = []
        async def connect(self): pass
        async def close(self): pass
        async def request(self, method, path, **kwargs):
            self.calls.append((method, path, kwargs.get("params", {})))
            if method == "GET":
                return open_orders
            return {}  # successful cancel

    rest = _TwoOrderREST()
    gw = _make_gw(btc_inst, rest)
    count = await gw.cancel_stale_orders()
    assert count == 2

    delete_calls = [(m, p, params) for m, p, params in rest.calls if m == "DELETE"]
    assert len(delete_calls) == 2
    cancelled_ids = {params["orderId"] for _, _, params in delete_calls}
    assert cancelled_ids == {101, 102}


# ---------------------------------------------------------------------------
# 5.8  cancel_stale_orders survives REST failure on list
# ---------------------------------------------------------------------------

async def test_cancel_stale_orders_survives_list_error(btc_inst) -> None:
    class _ErrorREST:
        async def connect(self): pass
        async def close(self): pass
        async def request(self, method, path, **kwargs):
            raise RuntimeError("network error")

    gw = _make_gw(btc_inst, _ErrorREST())
    # Should not raise; returns 0 (couldn't enumerate orders)
    count = await gw.cancel_stale_orders()
    assert count == 0


# ---------------------------------------------------------------------------
# 5.9  cancel_stale_orders survives cancel failure on individual order
# ---------------------------------------------------------------------------

async def test_cancel_stale_orders_survives_cancel_error(btc_inst) -> None:
    open_orders = [{"orderId": 55, "symbol": "BTCUSDT"}]

    class _CancelErrorREST:
        async def connect(self): pass
        async def close(self): pass
        async def request(self, method, path, **kwargs):
            if method == "GET":
                return open_orders
            raise RuntimeError("cancel failed")

    gw = _make_gw(btc_inst, _CancelErrorREST())
    # cancel failed, but we swallow the error and return 0 for that order
    count = await gw.cancel_stale_orders()
    assert count == 0


# ---------------------------------------------------------------------------
# 5.10  SymbolMapper.all_wire_symbols() returns registered symbols
# ---------------------------------------------------------------------------

def test_symbol_mapper_all_wire_symbols(btc_inst) -> None:
    eth = Instrument(
        symbol="ETH-USDT", exchange="BINANCE", asset_type=AssetType.SPOT,
        base_currency="ETH", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.001"),
    )
    mapper = SymbolMapper([btc_inst, eth])
    wires = mapper.all_wire_symbols()
    assert set(wires) == {"BTCUSDT", "ETHUSDT"}


def test_symbol_mapper_all_wire_symbols_empty() -> None:
    mapper = SymbolMapper([])
    assert mapper.all_wire_symbols() == []
