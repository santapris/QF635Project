"""Unit tests for Binance WebSocket plumbing: stream names, listen-key
manager, user data stream parsing.

We do not test the live WebSocket connection itself — that requires
network access to Binance. The connector is structurally identical to
the simulated connector tested in batch 3 of the core platform; the
piece worth testing carefully is the executionReport parser.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from trading.core import (
    AssetType,
    ClientOrderId,
    ExchangeOrderId,
    FillEvent,
    Instrument,
    LiveClock,
    OrderAcknowledged,
    OrderCancelled,
    OrderId,
    OrderRejected,
    Side,
    StrategyId,
)
from trading.event_bus import MemoryBus, Topic
from trading.gateways.binance import (
    BinanceConfig,
    ListenKeyManager,
    SymbolMapper,
)
from trading.gateways.binance import stream_names
from trading.gateways.binance.user_data import BinanceUserDataStream


# --- Stream names --------------------------------------------------------

def test_book_ticker_lowercases():
    assert stream_names.book_ticker("BTCUSDT") == "btcusdt@bookTicker"


def test_agg_trade():
    assert stream_names.agg_trade("ETHUSDT") == "ethusdt@aggTrade"


def test_depth_diff_speed_validation():
    assert stream_names.depth_diff("BTCUSDT", update_speed_ms=100) == "btcusdt@depth@100ms"
    assert stream_names.depth_diff("BTCUSDT", update_speed_ms=1000) == "btcusdt@depth"
    with pytest.raises(ValueError):
        stream_names.depth_diff("BTCUSDT", update_speed_ms=500)


def test_kline_interval():
    assert stream_names.kline("btcusdt", "1m") == "btcusdt@kline_1m"
    assert stream_names.kline("BTCUSDT", "5m") == "btcusdt@kline_5m"


# --- Listen key manager (with fake REST) ---------------------------------

class _FakeREST:
    """Records calls; returns canned responses."""

    def __init__(self):
        self.calls: list[tuple[str, str]] = []
        self.next_key = "lk-001"
        self.fail_keepalive_after: int = -1  # -1 = never
        self.keepalive_count = 0

    async def connect(self) -> None: pass
    async def close(self) -> None: pass

    async def request(self, method, path, *, params=None, signed=False, user_data=False, weight=1.0):
        self.calls.append((method, path))
        if path == "/api/v3/userDataStream":
            if method == "POST":
                return {"listenKey": self.next_key}
            if method == "PUT":
                self.keepalive_count += 1
                if (self.fail_keepalive_after >= 0
                        and self.keepalive_count > self.fail_keepalive_after):
                    raise Exception("simulated keepalive failure")
                return {}
            if method == "DELETE":
                return {}
        raise AssertionError(f"unexpected REST call: {method} {path}")


async def test_listen_key_manager_obtains_initial():
    cfg = BinanceConfig(
        spot_rest_base="https://testnet.binance.vision",
        spot_ws_base="wss://testnet.binance.vision",
        futures_rest_base="",
        futures_ws_base="",
        listen_key_keepalive_seconds=0.01,
    )
    rest = _FakeREST()
    rest.next_key = "lk-initial"
    mgr = ListenKeyManager(rest=rest, config=cfg)
    await mgr.start()
    try:
        assert mgr.current_key == "lk-initial"
        # Initial set: the recreation event is signalled.
        key = await asyncio.wait_for(mgr.wait_for_recreation(), timeout=0.5)
        assert key == "lk-initial"
    finally:
        await mgr.stop()


async def test_listen_key_manager_keepalive_fires():
    cfg = BinanceConfig(
        spot_rest_base="https://testnet.binance.vision",
        spot_ws_base="wss://testnet.binance.vision",
        futures_rest_base="",
        futures_ws_base="",
        listen_key_keepalive_seconds=0.05,
    )
    rest = _FakeREST()
    mgr = ListenKeyManager(rest=rest, config=cfg)
    await mgr.start()
    try:
        # Wait long enough for at least one keepalive cycle.
        await asyncio.sleep(0.15)
        puts = [c for c in rest.calls if c[0] == "PUT"]
        assert len(puts) >= 1
    finally:
        await mgr.stop()


async def test_listen_key_manager_reissues_on_keepalive_failure():
    cfg = BinanceConfig(
        spot_rest_base="https://testnet.binance.vision",
        spot_ws_base="wss://testnet.binance.vision",
        futures_rest_base="",
        futures_ws_base="",
        listen_key_keepalive_seconds=0.02,
    )
    rest = _FakeREST()
    rest.next_key = "lk-first"
    rest.fail_keepalive_after = 0  # fail the very next keepalive
    mgr = ListenKeyManager(rest=rest, config=cfg)

    await mgr.start()
    try:
        # First recreation: initial fetch.
        first = await asyncio.wait_for(mgr.wait_for_recreation(), timeout=0.5)
        assert first == "lk-first"
        # Now arm for reissue and wait for the next recreation.
        rest.next_key = "lk-second"
        second = await asyncio.wait_for(mgr.wait_for_recreation(), timeout=1.0)
        assert second == "lk-second"
    finally:
        await mgr.stop()


# --- User data stream — executionReport parsing -------------------------

@pytest.fixture
def btc_binance() -> Instrument:
    return Instrument(
        symbol="BTC-USDT", exchange="BINANCE", asset_type=AssetType.SPOT,
        base_currency="BTC", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.00001"),
    )


@pytest.fixture
def mapper(btc_binance) -> SymbolMapper:
    return SymbolMapper([btc_binance])


def _execution_report_trade(client_id: str, qty: str, price: str, cum: str, leaves: str) -> dict:
    """Build a TRADE executionReport approximating Binance's real shape."""
    return {
        "e": "executionReport",
        "E": 1700000000000,
        "s": "BTCUSDT",
        "c": client_id,
        "S": "BUY",
        "o": "LIMIT",
        "f": "GTC",
        "q": str(Decimal(cum) + Decimal(leaves)),
        "p": "50000",
        "x": "TRADE",
        "X": "PARTIALLY_FILLED" if Decimal(leaves) > 0 else "FILLED",
        "i": 12345678,
        "l": qty,    # last filled qty
        "z": cum,    # cumulative filled
        "L": price,  # last filled price
        "n": "0.1",  # commission
        "N": "USDT",
        "T": 1700000000001,
        "t": 99,
        "m": False,
    }


def _execution_report_cancel(client_id: str) -> dict:
    return {
        "e": "executionReport", "E": 1700000000000, "s": "BTCUSDT",
        "c": client_id, "S": "BUY", "o": "LIMIT", "f": "GTC",
        "q": "1.0", "p": "50000", "x": "CANCELED", "X": "CANCELED",
        "i": 12345678, "l": "0", "z": "0", "L": "0", "n": "0", "N": "USDT",
        "T": 1700000000002,
    }


def _execution_report_reject(client_id: str, reason: str = "INSUFFICIENT_BALANCE") -> dict:
    return {
        "e": "executionReport", "E": 1700000000000, "s": "BTCUSDT",
        "c": client_id, "S": "BUY", "o": "LIMIT", "f": "GTC",
        "q": "1.0", "p": "50000", "x": "REJECTED", "X": "REJECTED",
        "r": reason,
        "i": 0, "l": "0", "z": "0", "L": "0", "n": "0", "N": "USDT",
        "T": 1700000000003,
    }


def _execution_report_new(client_id: str) -> dict:
    return {
        "e": "executionReport", "E": 1700000000000, "s": "BTCUSDT",
        "c": client_id, "S": "BUY", "o": "LIMIT", "f": "GTC",
        "q": "1.0", "p": "50000", "x": "NEW", "X": "NEW",
        "i": 12345678, "l": "0", "z": "0", "L": "0", "n": "0", "N": "USDT",
        "T": 1700000000000,
    }


async def _make_user_data_stream(mapper, btc_binance):
    """Build a UDS without starting it; we'll exercise _handle_frame directly."""
    bus = MemoryBus()
    cfg = BinanceConfig(
        spot_rest_base="https://testnet.binance.vision",
        spot_ws_base="wss://testnet.binance.vision",
        futures_rest_base="",
        futures_ws_base="",
    )
    # We don't need a real listen-key manager for the parser tests.
    class _FakeLK:
        async def wait_for_recreation(self): return "fake-key"
    stream = BinanceUserDataStream(
        bus=bus, clock=LiveClock(), config=cfg,
        listen_key_manager=_FakeLK(),
        symbols=mapper,
        strategy_id_lookup=lambda coid: StrategyId("test-strat"),
    )
    return bus, stream


async def test_user_data_trade_publishes_fill(mapper, btc_binance):
    bus, stream = await _make_user_data_stream(mapper, btc_binance)
    # Seed the client_order_id -> order_id mapping by publishing an ack
    # the way the gateway would.
    order_id = OrderId(uuid4())
    client_id = ClientOrderId("strat-abc123")
    await bus.subscribe(Topic.ORDERS, stream._on_order_event)
    await bus.publish(Topic.ORDERS, OrderAcknowledged(
        ts_event=1, ts_ingest=1, source="binance",
        order_id=order_id, client_order_id=client_id,
        exchange_order_id=ExchangeOrderId("ex-1"),
    ))
    # Now feed a TRADE executionReport.
    msg = _execution_report_trade(client_id, qty="0.5", price="50000", cum="0.5", leaves="0.5")
    import json
    await stream._handle_frame(json.dumps(msg))

    fills = bus.published_on(Topic.FILLS)
    assert len(fills) == 1
    f = fills[0]
    assert isinstance(f, FillEvent)
    assert f.order_id == order_id
    assert f.client_order_id == client_id
    assert f.side == Side.BUY
    assert f.fill_price == Decimal("50000")
    assert f.fill_quantity == Decimal("0.5")
    assert f.cumulative_quantity == Decimal("0.5")
    assert f.leaves_quantity == Decimal("0.5")
    assert f.fee == Decimal("0.1")
    assert f.fee_currency == "USDT"
    assert f.is_maker is False
    assert f.strategy_id == "test-strat"


async def test_user_data_cancel_publishes_order_cancelled(mapper, btc_binance):
    bus, stream = await _make_user_data_stream(mapper, btc_binance)
    order_id = OrderId(uuid4())
    client_id = ClientOrderId("strat-cancel-1")
    await bus.subscribe(Topic.ORDERS, stream._on_order_event)
    await bus.publish(Topic.ORDERS, OrderAcknowledged(
        ts_event=1, ts_ingest=1, source="binance",
        order_id=order_id, client_order_id=client_id,
        exchange_order_id=ExchangeOrderId("ex-2"),
    ))
    import json
    await stream._handle_frame(json.dumps(_execution_report_cancel(client_id)))
    cancels = [e for e in bus.published_on(Topic.ORDERS) if isinstance(e, OrderCancelled)]
    assert len(cancels) == 1
    assert cancels[0].order_id == order_id


async def test_user_data_reject_publishes_order_rejected(mapper, btc_binance):
    bus, stream = await _make_user_data_stream(mapper, btc_binance)
    order_id = OrderId(uuid4())
    client_id = ClientOrderId("strat-reject-1")
    await bus.subscribe(Topic.ORDERS, stream._on_order_event)
    await bus.publish(Topic.ORDERS, OrderAcknowledged(
        ts_event=1, ts_ingest=1, source="binance",
        order_id=order_id, client_order_id=client_id,
        exchange_order_id=ExchangeOrderId("ex-3"),
    ))
    import json
    await stream._handle_frame(json.dumps(_execution_report_reject(client_id, "BAD")))
    rejs = [e for e in bus.published_on(Topic.ORDERS) if isinstance(e, OrderRejected)]
    # Only the reject event from WS — no extra ack from us.
    assert len(rejs) == 1
    assert "BAD" in rejs[0].reason


async def test_user_data_new_does_not_duplicate_ack(mapper, btc_binance):
    """Critical: the WS sends NEW on every accepted order. The gateway has
    already published OrderAcknowledged from the REST response. Suppress
    the WS one to avoid duplicate events."""
    bus, stream = await _make_user_data_stream(mapper, btc_binance)
    order_id = OrderId(uuid4())
    client_id = ClientOrderId("strat-new-1")
    await bus.subscribe(Topic.ORDERS, stream._on_order_event)
    # The gateway has already published one ack.
    await bus.publish(Topic.ORDERS, OrderAcknowledged(
        ts_event=1, ts_ingest=1, source="binance",
        order_id=order_id, client_order_id=client_id,
        exchange_order_id=ExchangeOrderId("ex-4"),
    ))
    pre_ack_count = sum(1 for e in bus.published_on(Topic.ORDERS)
                        if isinstance(e, OrderAcknowledged))
    # Feed the WS NEW report
    import json
    await stream._handle_frame(json.dumps(_execution_report_new(client_id)))
    # No additional ack should have been published.
    post_ack_count = sum(1 for e in bus.published_on(Topic.ORDERS)
                         if isinstance(e, OrderAcknowledged))
    assert post_ack_count == pre_ack_count


async def test_user_data_skips_unknown_client_order_id(mapper, btc_binance):
    """A fill for a clientId we never sent (e.g. manual trade on Binance UI)
    must not crash; just be skipped."""
    bus, stream = await _make_user_data_stream(mapper, btc_binance)
    import json
    msg = _execution_report_trade("never-sent", "0.1", "50000", "0.1", "0")
    # No ack published; client_to_order_id is empty.
    await stream._handle_frame(json.dumps(msg))
    # Nothing published.
    assert bus.published_on(Topic.FILLS) == []


async def test_user_data_skips_unknown_symbol(mapper, btc_binance):
    """A fill on a symbol we never registered must be silently dropped."""
    bus, stream = await _make_user_data_stream(mapper, btc_binance)
    import json
    # Forge an executionReport with a symbol the mapper doesn't know.
    msg = _execution_report_trade("c1", "0.1", "50000", "0.1", "0")
    msg["s"] = "ETHUSDT"  # not registered
    await stream._handle_frame(json.dumps(msg))
    assert bus.published_on(Topic.FILLS) == []


async def test_user_data_ignores_non_json(mapper, btc_binance):
    """Defensive: a non-JSON frame should not crash the parser."""
    bus, stream = await _make_user_data_stream(mapper, btc_binance)
    # Should not raise.
    await stream._handle_frame("this is not JSON {{{")
    assert bus.published_on(Topic.FILLS) == []


async def test_user_data_ignores_unknown_event_type(mapper, btc_binance):
    """A future event type we don't know about should be quietly ignored."""
    bus, stream = await _make_user_data_stream(mapper, btc_binance)
    import json
    await stream._handle_frame(json.dumps({"e": "someNewEventType", "data": "x"}))
    # No fills, no errors.
    assert bus.published_on(Topic.FILLS) == []
