"""Batch 3: strategy attribution — OMS reverse lookup and user-data stream wiring.

Tests:
  3.1  strategy_id_for_client_order returns correct strategy after a submit.
  3.2  strategy_id_for_client_order returns None for unknown coid.
  3.3  strategy_id_for_client_order returns None after the order is gone
       (guards against broken reverse-map).
  3.4  BinanceUserDataStream requires strategy_id_lookup (TypeError without it).
  3.5  BinanceUserDataStream lookup is called on fill and stamp is correct.
  3.6  Lookup returning None falls back to StrategyId("unknown") on the event.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from decimal import Decimal
from uuid import uuid4

import pytest

from trading.core import (
    AssetType,
    Instrument,
    OrderType,
    Side,
    SignalEvent,
    StrategyId,
    TimeInForce,
)
from trading.core.types import (
    ClientOrderId,
    OrderId,
    Quantity,
)
from trading.oms import OMSEngine
from trading.oms.execution_algos.base import ChildOrderSpec
from trading.oms.execution_algos.immediate import ImmediateAlgo


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
    """Bus that silently discards all publishes."""
    async def publish(self, topic, event): pass
    async def subscribe(self, topic, handler): pass
    async def subscribe_many(self, topics, handler): pass
    async def start(self): pass
    async def stop(self): pass


# ---------------------------------------------------------------------------
# 3.1  strategy_id_for_client_order happy path
# ---------------------------------------------------------------------------

async def test_strategy_id_for_client_order_found(
    clock, btc_inst, strategy_id
) -> None:
    """After _submit_child completes, the coid -> strategy_id lookup works."""
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    sig = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="test",
        strategy_id=strategy_id, instrument=btc_inst, side=Side.BUY,
        target_quantity=Decimal("1"),
        order_type=OrderType.MARKET, time_in_force=TimeInForce.IOC,
    )
    parent_id = OrderId(uuid4())
    algo = ImmediateAlgo(
        quantity=Quantity(Decimal("1")),
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.IOC,
    )
    oms._algos[parent_id] = algo
    oms._parents[parent_id] = sig

    spec = ChildOrderSpec(
        order_type=OrderType.MARKET,
        quantity=Quantity(Decimal("1")),
        price=None,
        time_in_force=TimeInForce.IOC,
    )
    await oms._submit_child(parent_id, spec)

    assert len(oms._orders) == 1
    (order,) = oms._orders.values()
    coid = order.client_order_id

    result = oms.strategy_id_for_client_order(coid)
    assert result == strategy_id


# ---------------------------------------------------------------------------
# 3.2  Unknown coid returns None
# ---------------------------------------------------------------------------

def test_strategy_id_for_unknown_coid(clock, btc_inst) -> None:
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    result = oms.strategy_id_for_client_order(ClientOrderId("nonexistent"))
    assert result is None


# ---------------------------------------------------------------------------
# 3.3  Reverse map is keyed by coid, not order_id
# ---------------------------------------------------------------------------

async def test_strategy_id_lookup_uses_coid_not_order_id(
    clock, btc_inst, strategy_id
) -> None:
    """The reverse map stores ClientOrderId; passing a raw OrderId str returns None."""
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    sig = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="test",
        strategy_id=strategy_id, instrument=btc_inst, side=Side.BUY,
        target_quantity=Decimal("1"),
        order_type=OrderType.MARKET, time_in_force=TimeInForce.IOC,
    )
    parent_id = OrderId(uuid4())
    algo = ImmediateAlgo(
        quantity=Quantity(Decimal("1")),
        order_type=OrderType.MARKET,
        time_in_force=TimeInForce.IOC,
    )
    oms._algos[parent_id] = algo
    oms._parents[parent_id] = sig

    spec = ChildOrderSpec(
        order_type=OrderType.MARKET,
        quantity=Quantity(Decimal("1")),
        price=None,
        time_in_force=TimeInForce.IOC,
    )
    await oms._submit_child(parent_id, spec)

    (order,) = oms._orders.values()
    # Using the order_id (not coid) should return None.
    fake_coid = ClientOrderId(str(order.order_id))
    assert oms.strategy_id_for_client_order(fake_coid) is None


# ---------------------------------------------------------------------------
# 3.4  BinanceUserDataStream requires strategy_id_lookup
# ---------------------------------------------------------------------------

def test_user_data_stream_requires_lookup(btc_inst, clock) -> None:
    """Omitting strategy_id_lookup raises TypeError (required kwarg, no default)."""
    # Skip if websockets is not installed — the ImportError would fire first.
    try:
        import websockets  # noqa: F401
    except ImportError:
        pytest.skip("websockets not installed")

    from trading.gateways.binance.config import BinanceConfig
    from trading.gateways.binance.listen_key import ListenKeyManager
    from trading.gateways.binance.symbols import SymbolMapper
    from trading.gateways.binance.user_data import BinanceUserDataStream

    # Confirm the parameter has no default (required).
    sig = inspect.signature(BinanceUserDataStream.__init__)
    param = sig.parameters["strategy_id_lookup"]
    assert param.default is inspect.Parameter.empty, (
        "strategy_id_lookup must be a required parameter with no default"
    )


def test_user_data_stream_lookup_type_annotation(btc_inst, clock) -> None:
    """strategy_id_lookup annotation should be Callable, not untyped."""
    from trading.gateways.binance.user_data import BinanceUserDataStream

    sig = inspect.signature(BinanceUserDataStream.__init__)
    param = sig.parameters["strategy_id_lookup"]
    # The annotation object should reference Callable (or its origin).
    annotation = param.annotation
    assert annotation is not inspect.Parameter.empty, (
        "strategy_id_lookup must be annotated"
    )


# ---------------------------------------------------------------------------
# 3.5  Lookup is called and stamps the fill event correctly
# ---------------------------------------------------------------------------

async def test_user_data_stream_stamps_strategy_id(btc_inst, clock) -> None:
    """_handle_execution_report calls lookup and the published FillEvent carries it."""
    try:
        import websockets  # noqa: F401
    except ImportError:
        pytest.skip("websockets not installed")

    import json
    from trading.core.types import ExchangeOrderId, OrderId
    from trading.gateways.binance.config import BinanceConfig
    from trading.gateways.binance.listen_key import ListenKeyManager
    from trading.gateways.binance.symbols import SymbolMapper
    from trading.gateways.binance.user_data import BinanceUserDataStream
    from trading.event_bus.base import Topic

    expected_strategy = StrategyId("my-strategy")
    lookup_calls: list[ClientOrderId] = []

    def my_lookup(coid: ClientOrderId) -> StrategyId | None:
        lookup_calls.append(coid)
        return expected_strategy

    published: list = []

    class _CaptureBus:
        async def publish(self, topic, event):
            published.append((topic, event))
        async def subscribe(self, topic, handler): pass
        async def subscribe_many(self, topics, handler): pass

    cfg = BinanceConfig(testnet=True)
    symbols = SymbolMapper([btc_inst])

    class _FakeLKM:
        async def wait_for_recreation(self): await asyncio.sleep(9999)

    import asyncio

    stream = BinanceUserDataStream(
        bus=_CaptureBus(),
        clock=clock,
        config=cfg,
        listen_key_manager=_FakeLKM(),
        symbols=symbols,
        strategy_id_lookup=my_lookup,
    )

    # Inject the client_order_id -> order_id mapping directly (normally
    # populated by snooping on OrderAcknowledged events).
    coid = ClientOrderId("my-strat-abc123")
    oid = OrderId(uuid4())
    stream._client_to_order_id[coid] = oid

    # Fabricate a minimal TRADE executionReport for BTC-USDT.
    msg = {
        "e": "executionReport",
        "E": 1700000000000,
        "s": "BTCUSDT",         # wire symbol used by SymbolMapper
        "c": coid,
        "S": "BUY",
        "x": "TRADE",
        "X": "PARTIALLY_FILLED",
        "i": "99999",
        "l": "0.1",
        "z": "0.1",
        "q": "1.0",
        "p": "50000.00",
        "L": "50000.00",
        "n": "0.0001",
        "N": "BNB",
        "T": 1700000000001,
        "t": "12345",
        "m": False,
    }

    await stream._handle_execution_report(msg)

    # The lookup must have been called exactly once.
    assert lookup_calls == [coid]

    # A FillEvent must have been published.
    fill_publishes = [(t, e) for t, e in published if t == Topic.FILLS]
    assert len(fill_publishes) == 1
    _, fill = fill_publishes[0]
    assert fill.strategy_id == expected_strategy


# ---------------------------------------------------------------------------
# 3.6  Lookup returning None falls back to "unknown"
# ---------------------------------------------------------------------------

async def test_user_data_stream_unknown_fallback(btc_inst, clock) -> None:
    """If lookup returns None, strategy_id on the fill event is 'unknown'."""
    try:
        import websockets  # noqa: F401
    except ImportError:
        pytest.skip("websockets not installed")

    import asyncio
    from trading.core.types import ExchangeOrderId, OrderId
    from trading.gateways.binance.config import BinanceConfig
    from trading.gateways.binance.symbols import SymbolMapper
    from trading.gateways.binance.user_data import BinanceUserDataStream
    from trading.event_bus.base import Topic

    published: list = []

    class _CaptureBus:
        async def publish(self, topic, event):
            published.append((topic, event))
        async def subscribe(self, topic, handler): pass

    class _FakeLKM:
        async def wait_for_recreation(self): await asyncio.sleep(9999)

    stream = BinanceUserDataStream(
        bus=_CaptureBus(),
        clock=clock,
        config=BinanceConfig(testnet=True),
        listen_key_manager=_FakeLKM(),
        symbols=SymbolMapper([btc_inst]),
        strategy_id_lookup=lambda coid: None,
    )

    coid = ClientOrderId("unknown-coid")
    oid = OrderId(uuid4())
    stream._client_to_order_id[coid] = oid

    msg = {
        "e": "executionReport", "E": 1700000000000,
        "s": "BTCUSDT", "c": coid, "S": "BUY", "x": "TRADE",
        "X": "FILLED", "i": "1", "l": "1.0", "z": "1.0",
        "q": "1.0", "p": "50000", "L": "50000",
        "n": "0", "N": "BNB", "T": 1700000000001, "t": "1", "m": False,
    }
    await stream._handle_execution_report(msg)

    fill_events = [e for _, e in published]
    assert len(fill_events) == 1
    assert fill_events[0].strategy_id == StrategyId("unknown")
