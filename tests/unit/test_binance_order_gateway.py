"""Unit tests for the Binance order_gateway: order translation, symbol map, end-to-end
event publishing using a fake REST client."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from trading.core import (
    AmendRequest,
    AssetType,
    CancelRequest,
    ClientOrderId,
    Instrument,
    LiveClock,
    OrderAcknowledged,
    OrderAmended,
    OrderId,
    OrderRejected,
    OrderRequest,
    OrderType,
    Price,
    Quantity,
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


def _futures_gw_with_fake(rest, instruments):
    bus = MemoryBus()
    cfg = BinanceConfig(
        spot_rest_base="https://testnet.binance.vision",
        spot_ws_base="wss://testnet.binance.vision",
        futures_rest_base="https://demo-fapi.binance.com",
        futures_ws_base="wss://fstream.binancefuture.com",
        futures=True,
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


@pytest.mark.parametrize("gone_code", [-2011, -2013])
async def test_cancel_of_gone_order_publishes_cancelled_not_rejected(
    btc_binance, gone_code
):
    """A cancel that fails because the order is already gone (-2011 unknown
    order / -2013 does not exist) must publish OrderCancelled, not
    CancelRejected. CancelRejected would roll the OMS order back to ACKNOWLEDGED
    and the reconciler would re-cancel it forever against an order Binance no
    longer has — the tracked-but-gone desync (orders stuck PENDING_CANCEL while
    the venue shows none)."""
    from trading.core.events import CancelRejected, OrderCancelled

    rest = _FakeREST()
    rest.responses.append(OrderError("Unknown order sent.", code=gone_code))
    bus, gw = _gw_with_fake(rest, [btc_binance])
    await gw.start()
    cancel = CancelRequest(
        ts_event=0, ts_ingest=0, source="oms",
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId("cli-gone"),
        instrument=btc_binance,
    )
    await bus.publish(Topic.ORDERS, cancel)

    published = bus.published_on(Topic.ORDERS)
    assert not [e for e in published if isinstance(e, CancelRejected)]
    cancels = [e for e in published if isinstance(e, OrderCancelled)]
    assert len(cancels) == 1
    assert str(cancels[0].client_order_id) == "cli-gone"


async def test_cancel_rejected_for_other_errors_still_rejects(btc_binance):
    """A cancel failing for a reason other than order-gone leaves the order
    genuinely live, so we publish CancelRejected and let the OMS retry."""
    from trading.core.events import CancelRejected, OrderCancelled

    rest = _FakeREST()
    rest.responses.append(OrderError("some other failure", code=-1234))
    bus, gw = _gw_with_fake(rest, [btc_binance])
    await gw.start()
    cancel = CancelRequest(
        ts_event=0, ts_ingest=0, source="oms",
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId("cli-live"),
        instrument=btc_binance,
    )
    await bus.publish(Topic.ORDERS, cancel)

    published = bus.published_on(Topic.ORDERS)
    assert not [e for e in published if isinstance(e, OrderCancelled)]
    rejs = [e for e in published if isinstance(e, CancelRejected)]
    assert len(rejs) == 1
    assert str(rejs[0].client_order_id) == "cli-live"


async def test_futures_amend_publishes_venue_resulting_values(btc_binance):
    """The futures PUT amend publishes OrderAmended carrying the price/qty the
    venue actually rested at (from the PUT response), not the requested values.

    Guards against silent local/venue divergence: if Binance clamps the amend,
    trusting the request would orphan the resting order and accumulate ladders.
    """
    rest = _FakeREST()
    # We request 50001 / 0.20 but the venue reports it rested at 50000.5 / 0.20.
    rest.responses.append(
        {"orderId": 4242, "status": "NEW", "price": "50000.5", "origQty": "0.20"}
    )
    bus, gw = _futures_gw_with_fake(rest, [btc_binance])
    await gw.start()
    amend = AmendRequest(
        ts_event=0, ts_ingest=0, source="oms",
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId("test-amend-1"),
        instrument=btc_binance,
        side=Side.BUY,
        new_price=Price(Decimal("50001")),
        new_quantity=Quantity(Decimal("0.20")),
    )
    await bus.publish(Topic.ORDERS, amend)

    method, path, _ = rest.calls[0]
    assert method == "PUT" and path == "/fapi/v1/order"
    amended = [e for e in bus.published_on(Topic.ORDERS) if isinstance(e, OrderAmended)]
    assert len(amended) == 1
    assert amended[0].new_price == Decimal("50000.5")   # venue value, not 50001
    assert amended[0].new_quantity == Decimal("0.20")
    assert amended[0].new_exchange_order_id == "4242"


async def test_futures_amend_cancelled_order_publishes_cancel_not_amend(btc_binance):
    """A GTX amend that would cross is CANCELED by Binance: the PUT returns
    HTTP 200 with status=CANCELED (not an HTTP error). The gateway must publish
    OrderCancelled, not OrderAmended — otherwise the OMS tracks a dead order as
    live at the new price."""
    from trading.core.events import OrderCancelled

    rest = _FakeREST()
    rest.responses.append(
        {"orderId": 4242, "status": "CANCELED", "price": "50001", "origQty": "0.20"}
    )
    bus, gw = _futures_gw_with_fake(rest, [btc_binance])
    await gw.start()
    amend = AmendRequest(
        ts_event=0, ts_ingest=0, source="oms",
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId("test-amend-x"),
        instrument=btc_binance,
        side=Side.BUY,
        new_price=Price(Decimal("50001")),
        new_quantity=Quantity(Decimal("0.20")),
    )
    await bus.publish(Topic.ORDERS, amend)

    published = bus.published_on(Topic.ORDERS)
    assert not [e for e in published if isinstance(e, OrderAmended)]
    cancels = [e for e in published if isinstance(e, OrderCancelled)]
    assert len(cancels) == 1
    assert str(cancels[0].client_order_id) == "test-amend-x"


async def test_futures_amend_5027_noop_leaves_order_live(btc_binance):
    """Binance -5027 ('No need to modify the order') means the order is already
    at the requested price/qty — the amend is a no-op, not a failure. The gateway
    must publish nothing (no OrderRejected, no OrderAmended) so the OMS keeps the
    order in its current live state rather than terminalizing it."""
    rest = _FakeREST()
    rest.responses.append(
        OrderError("binance error -5027: No need to modify the order.", code=-5027)
    )
    bus, gw = _futures_gw_with_fake(rest, [btc_binance])
    await gw.start()
    amend = AmendRequest(
        ts_event=0, ts_ingest=0, source="oms",
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId("test-amend-noop"),
        instrument=btc_binance,
        side=Side.BUY,
        new_price=Price(Decimal("50000")),
        new_quantity=Quantity(Decimal("0.10")),
    )
    await bus.publish(Topic.ORDERS, amend)

    published = bus.published_on(Topic.ORDERS)
    assert not [e for e in published if isinstance(e, OrderRejected)]
    assert not [e for e in published if isinstance(e, OrderAmended)]


async def test_futures_amend_5026_modify_limit_cancels_on_venue_then_publishes_cancel(btc_binance):
    """Binance -5026 ('Exceed maximum modify order limit') means the order can
    never be amended again, but it is STILL RESTING on the venue. The gateway
    must actually DELETE it on the venue *before* publishing OrderCancelled —
    otherwise the OMS drops it and re-places a fresh order while the original
    keeps resting (and can still fill) on Binance, producing a 2-on-venue /
    1-tracked desync. It must publish OrderCancelled (not AmendRejected) so the
    OMS terminalizes and re-places, rather than looping the doomed amend."""
    from trading.core.events import AmendRejected, OrderCancelled

    rest = _FakeREST()
    # First call: the PUT amend fails with -5026.
    rest.responses.append(
        OrderError(
            "binance error -5026: Exceed maximum modify order limit.", code=-5026
        )
    )
    # Second call: the DELETE we now issue to actually cancel the resting order.
    rest.responses.append({"status": "CANCELED"})
    bus, gw = _futures_gw_with_fake(rest, [btc_binance])
    await gw.start()
    amend = AmendRequest(
        ts_event=0, ts_ingest=0, source="oms",
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId("test-amend-5026"),
        instrument=btc_binance,
        side=Side.BUY,
        new_price=Price(Decimal("50000")),
        new_quantity=Quantity(Decimal("0.10")),
    )
    await bus.publish(Topic.ORDERS, amend)

    # The gateway must have actually issued a DELETE for this order.
    delete_calls = [c for c in rest.calls if c[0] == "DELETE"]
    assert len(delete_calls) == 1
    assert delete_calls[0][2]["origClientOrderId"] == "test-amend-5026"

    published = bus.published_on(Topic.ORDERS)
    cancels = [e for e in published if isinstance(e, OrderCancelled)]
    assert len(cancels) == 1
    assert cancels[0].order_id == amend.order_id
    # Must NOT publish an amend-reject — that is the path that loops.
    assert not [e for e in published if isinstance(e, AmendRejected)]


async def test_futures_amend_5026_delete_finds_order_already_gone(btc_binance):
    """If the -5026 follow-up DELETE finds the order already gone (-2013), it is
    not resting either way, so the gateway still publishes OrderCancelled and
    does not roll the amend back."""
    from trading.core.events import AmendRejected, OrderCancelled

    rest = _FakeREST()
    rest.responses.append(
        OrderError("binance error -5026: Exceed maximum modify order limit.", code=-5026)
    )
    rest.responses.append(OrderError("Order does not exist.", code=-2013))
    bus, gw = _futures_gw_with_fake(rest, [btc_binance])
    await gw.start()
    amend = AmendRequest(
        ts_event=0, ts_ingest=0, source="oms",
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId("test-amend-5026-gone"),
        instrument=btc_binance,
        side=Side.BUY,
        new_price=Price(Decimal("50000")),
        new_quantity=Quantity(Decimal("0.10")),
    )
    await bus.publish(Topic.ORDERS, amend)

    published = bus.published_on(Topic.ORDERS)
    assert len([e for e in published if isinstance(e, OrderCancelled)]) == 1
    assert not [e for e in published if isinstance(e, AmendRejected)]


async def test_futures_amend_5026_delete_fails_rolls_back_no_leak(btc_binance):
    """If the -5026 follow-up DELETE fails for a non-'gone' reason, the order is
    genuinely still live. The gateway must NOT publish OrderCancelled (which would
    make the OMS drop a still-resting order = leak); it rolls the amend back via
    AmendRejected so the reconciler retries the cancel next tick."""
    from trading.core.events import AmendRejected, OrderCancelled

    rest = _FakeREST()
    rest.responses.append(
        OrderError("binance error -5026: Exceed maximum modify order limit.", code=-5026)
    )
    # DELETE fails with some other order error — order is still resting.
    rest.responses.append(OrderError("Some other error.", code=-1234))
    bus, gw = _futures_gw_with_fake(rest, [btc_binance])
    await gw.start()
    amend = AmendRequest(
        ts_event=0, ts_ingest=0, source="oms",
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId("test-amend-5026-stuck"),
        instrument=btc_binance,
        side=Side.BUY,
        new_price=Price(Decimal("50000")),
        new_quantity=Quantity(Decimal("0.10")),
    )
    await bus.publish(Topic.ORDERS, amend)

    published = bus.published_on(Topic.ORDERS)
    # No cancel — the order is still resting, dropping it would leak it.
    assert not [e for e in published if isinstance(e, OrderCancelled)]
    assert len([e for e in published if isinstance(e, AmendRejected)]) == 1


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
