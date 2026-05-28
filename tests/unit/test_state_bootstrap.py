"""StateBootstrapper: adopt venue orders + publish venue net positions.

Verifies the bootstrapper publishes the exchange's net position verbatim as
ground truth (the dashboard 'net' row) rather than synthesizing fills into the
PositionEngine, and that it adopts open orders into the OMS.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.core import VenuePositionSnapshotEvent
from trading.core.types import Side
from trading.event_bus.base import Topic
from trading.oms import OMSEngine
from trading.oms.engine import EXTERNAL_STRATEGY_ID
from trading.order_gateways.binance.config import BinanceConfig
from trading.order_gateways.binance.state_bootstrap import StateBootstrapper
from trading.order_gateways.binance.symbols import SymbolMapper


class _CaptureBus:
    def __init__(self):
        self.published: list[tuple[str, object]] = []
    async def publish(self, topic, event):
        self.published.append((topic, event))
    async def subscribe(self, topic, handler): pass
    async def subscribe_many(self, topics, handler): pass
    async def start(self): pass
    async def stop(self): pass


class _FakeREST:
    """Returns canned responses keyed by (method, path)."""
    def __init__(self, responses):
        self._responses = responses
        self.calls: list[tuple[str, str]] = []
    async def request(self, method, path, *, params=None, signed=False, weight=1.0):
        self.calls.append((method, path))
        for (m, p), resp in self._responses.items():
            if m == method and p in path:
                return resp
        return []


def _futures_config() -> BinanceConfig:
    return BinanceConfig(
        spot_rest_base="", spot_ws_base="",
        futures_rest_base="https://testnet.binancefuture.com",
        futures_ws_base="wss://stream.binancefuture.com",
        futures=True,
    )


async def test_publishes_venue_net_position(clock, btc) -> None:
    """A short -0.006 BTC venue position is published verbatim, not seeded
    into the PositionEngine as a fill."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    rest = _FakeREST({
        ("GET", "/openOrders"): [],
        ("GET", "/fapi/v2/positionRisk"): [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-0.006",
                "entryPrice": "72819.88",
                "markPrice": "72707.12",
                "unRealizedProfit": "0.47",
            },
        ],
    })
    boot = StateBootstrapper(
        bus=bus, clock=clock, config=_futures_config(), rest=rest,
        oms=oms, symbols=SymbolMapper([btc]), tracked_instruments=[btc],
    )
    await boot.bootstrap()

    snaps = [e for t, e in bus.published
             if t == Topic.VENUE_POSITIONS and isinstance(e, VenuePositionSnapshotEvent)]
    assert len(snaps) == 1
    positions = snaps[-1].positions
    assert len(positions) == 1
    vp = positions[0]
    assert vp.net_quantity == Decimal("-0.006")   # short, sign preserved
    assert vp.entry_price == Decimal("72819.88")
    assert vp.mark_price == Decimal("72707.12")
    assert vp.unrealized_pnl == Decimal("0.47")


async def test_does_not_seed_position_engine_via_fills(clock, btc) -> None:
    """No synthetic fills are published — the PositionEngine is untouched."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    rest = _FakeREST({
        ("GET", "/openOrders"): [],
        ("GET", "/fapi/v2/positionRisk"): [
            {"symbol": "BTCUSDT", "positionAmt": "-0.006",
             "entryPrice": "72819.88", "markPrice": "72707.12",
             "unRealizedProfit": "0.47"},
        ],
    })
    boot = StateBootstrapper(
        bus=bus, clock=clock, config=_futures_config(), rest=rest,
        oms=oms, symbols=SymbolMapper([btc]), tracked_instruments=[btc],
    )
    await boot.bootstrap()

    # The fix: position adoption must NOT publish fills (which would corrupt
    # per-strategy books). Only venue-position snapshots are emitted.
    assert not any(t == Topic.FILLS for t, _ in bus.published)


async def test_adopts_open_orders(clock, btc) -> None:
    bus = _CaptureBus()
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    rest = _FakeREST({
        ("GET", "/openOrders"): [
            {
                "clientOrderId": "mm-0123456789ab",
                "orderId": "999",
                "side": "BUY",
                "type": "LIMIT",
                "timeInForce": "GTC",
                "origQty": "0.01",
                "executedQty": "0",
                "price": "70000",
                "time": 1700000000000,
            },
        ],
        ("GET", "/fapi/v2/positionRisk"): [],
    })
    boot = StateBootstrapper(
        bus=bus, clock=clock, config=_futures_config(), rest=rest,
        oms=oms, symbols=SymbolMapper([btc]), tracked_instruments=[btc],
    )
    await boot.bootstrap()

    orders = list(oms.open_orders())
    assert len(orders) == 1
    assert orders[0].strategy_id == "mm"
    assert orders[0].side is Side.BUY
    assert orders[0].quantity == Decimal("0.01")


class _NullBus:
    async def publish(self, topic, event): pass
    async def subscribe(self, topic, handler): pass
    async def subscribe_many(self, topics, handler): pass
    async def start(self): pass
    async def stop(self): pass
