"""Unit tests for the Binance order_gateway: order translation, symbol map, end-to-end
event publishing using a fake REST client."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from trading.core import (
    AssetType,
    CancelRequest,
    ClientOrderId,
    Instrument,
    LiveClock,
    OrderAcknowledged,
    OrderId,
    OrderRejected,
    OrderRequest,
    OrderType,
    Side,
    StrategyId,
    TimeInForce,
)
from trading.core.exceptions import OrderGatewayError, OrderError
from trading.event_bus import MemoryBus, Topic
from trading.order_gateways.binance import (
    BinanceConfig,
    BinanceCredentials,
    BinanceOrderGateway,
    SymbolMapper,
)
from trading.order_gateways.binance.order_translation import (
    order_type_to_binance,
    side_from_binance,
    side_to_binance,
    tif_to_binance,
)


# --- Symbol mapper ---------------------------------------------------------

@pytest.fixture
def btc_binance() -> Instrument:
    return Instrument(
        symbol="BTC-USDT", exchange="BINANCE", asset_type=AssetType.SPOT,
        base_currency="BTC", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.00001"),
    )


@pytest.fixture
def eth_binance() -> Instrument:
    return Instrument(
        symbol="ETH-USDT", exchange="BINANCE", asset_type=AssetType.SPOT,
        base_currency="ETH", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.0001"),
    )


def test_symbol_mapper_derives_wire(btc_binance):
    mapper = SymbolMapper([btc_binance])
    assert mapper.wire_symbol(btc_binance) == "BTCUSDT"


def test_symbol_mapper_round_trip(btc_binance, eth_binance):
    mapper = SymbolMapper([btc_binance, eth_binance])
    assert mapper.by_wire("BTCUSDT") is btc_binance
    assert mapper.by_wire("ETHUSDT") is eth_binance
    assert mapper.by_wire("btcusdt") is btc_binance  # case-insensitive
    assert mapper.by_wire("UNKNOWN") is None


def test_symbol_mapper_ignores_non_binance():
    non_binance = Instrument(
        symbol="BTC-USDT", exchange="OTHER", asset_type=AssetType.SPOT,
        base_currency="BTC", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.0001"),
    )
    mapper = SymbolMapper([non_binance])
    assert mapper.by_wire("BTCUSDT") is None


# --- Order translation ----------------------------------------------------

def test_translate_market_omits_tif():
    binance_type, tif = order_type_to_binance(OrderType.MARKET, TimeInForce.IOC)
    assert binance_type == "MARKET"
    # The caller is expected to not include TIF for MARKET.


def test_translate_limit_passes_tif():
    binance_type, tif = order_type_to_binance(OrderType.LIMIT, TimeInForce.GTC)
    assert binance_type == "LIMIT"
    assert tif is TimeInForce.GTC


def test_translate_post_only_to_limit_maker():
    binance_type, _ = order_type_to_binance(OrderType.POST_ONLY, TimeInForce.GTC)
    assert binance_type == "LIMIT_MAKER"


def test_translate_ioc_becomes_limit_with_ioc_tif():
    binance_type, tif = order_type_to_binance(OrderType.IOC, TimeInForce.GTC)
    assert binance_type == "LIMIT"
    assert tif is TimeInForce.IOC


def test_translate_fok_becomes_limit_with_fok_tif():
    binance_type, tif = order_type_to_binance(OrderType.FOK, TimeInForce.GTC)
    assert binance_type == "LIMIT"
    assert tif is TimeInForce.FOK


def test_side_round_trip():
    assert side_to_binance(Side.BUY) == "BUY"
    assert side_to_binance(Side.SELL) == "SELL"
    assert side_from_binance("BUY") is Side.BUY
    assert side_from_binance("sell") is Side.SELL


def test_side_from_binance_unknown_raises():
    with pytest.raises(OrderError):
        side_from_binance("SIDEWAYS")


def test_tif_translation():
    assert tif_to_binance(TimeInForce.GTC) == "GTC"
    assert tif_to_binance(TimeInForce.IOC) == "IOC"
    assert tif_to_binance(TimeInForce.FOK) == "FOK"


# --- OrderGateway with fake REST client ---------------------------------------

class _FakeREST:
    """Records every call. Configurable responses or exceptions."""

    def __init__(self):
        self.calls: list[tuple[str, str, dict | None]] = []
        self.responses: list[Any] = []  # values: dict | Exception

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def request(self, method, path, *, params=None, signed=False, weight=1.0, user_data=False):
        self.calls.append((method, path, dict(params) if params else None))
        if not self.responses:
            raise AssertionError(f"unexpected REST call: {method} {path}")
        resp = self.responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp


def _gw_with_fake(rest, instruments):
    bus = MemoryBus()
    cfg = BinanceConfig(
        spot_rest_base="https://testnet.binance.vision",
        spot_ws_base="wss://testnet.binance.vision",
        futures_rest_base="https://demo-fapi.binance.com",
        futures_ws_base="wss://fstream.binancefuture.com",
    )
    creds = BinanceCredentials(api_key="k", api_secret="s")
    mapper = SymbolMapper(instruments)
    gw = BinanceOrderGateway(
        bus=bus, clock=LiveClock(), config=cfg, credentials=creds,
        symbols=mapper, rest_client=rest,
    )
    return bus, gw


def _order_request(instrument, **overrides) -> OrderRequest:
    defaults = dict(
        ts_event=0, ts_ingest=0, source="oms",
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId(f"test-{uuid4().hex[:8]}"),
        strategy_id=StrategyId("s1"),
        instrument=instrument,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.001"),
        price=None,
        stop_price=None,
        time_in_force=TimeInForce.IOC,
    )
    defaults.update(overrides)
    return OrderRequest(**defaults)


async def test_order_gateway_sends_market_order(btc_binance):
    rest = _FakeREST()
    rest.responses.append({"orderId": 12345, "status": "NEW"})
    bus, gw = _gw_with_fake(rest, [btc_binance])
    await gw.start()

    req = _order_request(btc_binance)
    await bus.publish(Topic.ORDERS, req)

    # Verify the REST call shape
    assert len(rest.calls) == 1
    method, path, params = rest.calls[0]
    assert method == "POST" and path == "/api/v3/order"
    assert params["symbol"] == "BTCUSDT"
    assert params["side"] == "BUY"
    assert params["type"] == "MARKET"
    assert params["quantity"] == "0.001"
    assert params["newClientOrderId"] == req.client_order_id
    # MARKET must NOT include TIF
    assert "timeInForce" not in params

    # Verify an ack was published
    acks = [e for e in bus.published_on(Topic.ORDERS) if isinstance(e, OrderAcknowledged)]
    assert len(acks) == 1
    assert acks[0].exchange_order_id == "12345"


async def test_order_gateway_sends_limit_order(btc_binance):
    rest = _FakeREST()
    rest.responses.append({"orderId": 99, "status": "NEW"})
    bus, gw = _gw_with_fake(rest, [btc_binance])
    await gw.start()

    req = _order_request(
        btc_binance,
        order_type=OrderType.LIMIT, time_in_force=TimeInForce.GTC,
        price=Decimal("50000.50"),
    )
    await bus.publish(Topic.ORDERS, req)
    _, _, params = rest.calls[0]
    assert params["type"] == "LIMIT"
    assert params["timeInForce"] == "GTC"
    assert params["price"] == "50000.5"  # normalized


async def test_order_gateway_post_only_becomes_limit_maker(btc_binance):
    rest = _FakeREST()
    rest.responses.append({"orderId": 1, "status": "NEW"})
    bus, gw = _gw_with_fake(rest, [btc_binance])
    await gw.start()
    req = _order_request(
        btc_binance,
        order_type=OrderType.POST_ONLY, time_in_force=TimeInForce.GTC,
        price=Decimal("50000"),
    )
    await bus.publish(Topic.ORDERS, req)
    _, _, params = rest.calls[0]
    assert params["type"] == "LIMIT_MAKER"


async def test_order_gateway_publishes_rejection_on_logical_error(btc_binance):
    """Insufficient balance should produce an OrderRejected event."""
    rest = _FakeREST()
    rest.responses.append(OrderError(
        "binance rejected order: Account has insufficient balance.",
        code=-2010, logical_reject=True,
    ))
    bus, gw = _gw_with_fake(rest, [btc_binance])
    await gw.start()
    req = _order_request(btc_binance)
    await bus.publish(Topic.ORDERS, req)
    rejs = [e for e in bus.published_on(Topic.ORDERS) if isinstance(e, OrderRejected)]
    assert len(rejs) == 1
    assert "insufficient balance" in rejs[0].reason.lower()


async def test_order_gateway_publishes_rejection_on_bad_symbol(btc_binance):
    rest = _FakeREST()
    rest.responses.append(OrderError("binance error -1121: Invalid symbol.", code=-1121))
    bus, gw = _gw_with_fake(rest, [btc_binance])
    await gw.start()
    await bus.publish(Topic.ORDERS, _order_request(btc_binance))
    rejs = [e for e in bus.published_on(Topic.ORDERS) if isinstance(e, OrderRejected)]
    assert len(rejs) == 1


async def test_order_gateway_publishes_rejection_on_transport_error(btc_binance):
    """A transport error has unknown order status; we reject and surface."""
    rest = _FakeREST()
    rest.responses.append(OrderGatewayError("connection lost"))
    bus, gw = _gw_with_fake(rest, [btc_binance])
    await gw.start()
    await bus.publish(Topic.ORDERS, _order_request(btc_binance))
    rejs = [e for e in bus.published_on(Topic.ORDERS) if isinstance(e, OrderRejected)]
    assert len(rejs) == 1
    assert "order_gateway error" in rejs[0].reason.lower()


async def test_order_gateway_ignores_non_binance_orders():
    """If a non-Binance instrument leaks through, order_gateway must not call REST."""
    rest = _FakeREST()
    bus, gw = _gw_with_fake(rest, [])  # no Binance instruments
    await gw.start()
    other = Instrument(
        symbol="BTC-USDT", exchange="OTHER", asset_type=AssetType.SPOT,
        base_currency="BTC", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.0001"),
    )
    req = _order_request(other)
    await bus.publish(Topic.ORDERS, req)
    assert rest.calls == []


async def test_order_gateway_sends_cancel(btc_binance):
    rest = _FakeREST()
    rest.responses.append({"orderId": 99, "status": "CANCELED"})
    bus, gw = _gw_with_fake(rest, [btc_binance])
    await gw.start()
    cancel = CancelRequest(
        ts_event=0, ts_ingest=0, source="oms",
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId("cli-abc"),
        instrument=btc_binance,
    )
    await bus.publish(Topic.ORDERS, cancel)
    method, path, params = rest.calls[0]
    assert method == "DELETE" and path == "/api/v3/order"
    assert params["symbol"] == "BTCUSDT"
    assert params["origClientOrderId"] == "cli-abc"


async def test_order_gateway_format_decimal_no_exponent(btc_binance):
    """Verify quantities like 0.00001 don't get serialized as 1E-5."""
    rest = _FakeREST()
    rest.responses.append({"orderId": 1, "status": "NEW"})
    bus, gw = _gw_with_fake(rest, [btc_binance])
    await gw.start()
    req = _order_request(btc_binance, quantity=Decimal("0.00001"))
    await bus.publish(Topic.ORDERS, req)
    _, _, params = rest.calls[0]
    # Must be plain decimal, no scientific notation.
    assert "e" not in params["quantity"].lower()
    assert params["quantity"] == "0.00001"
