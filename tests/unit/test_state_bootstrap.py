"""StateBootstrapper: adopt venue orders + publish venue net positions.

Verifies the bootstrapper publishes the exchange's net position verbatim as
ground truth (the dashboard 'net' row) rather than synthesizing fills into the
PositionEngine, and that it adopts open orders into the OMS.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.core import VenuePositionSnapshotEvent
from trading.core.types import OrderType, Side, TimeInForce
from trading.event_bus.base import Topic
from trading.oms import OMSEngine
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


class _RaisingREST:
    """REST stub whose /openOrders GET raises, simulating a transient failure.

    positionRisk still returns empty so the resync's position refresh is a
    no-op. Used to verify the resync does not act on venue state it failed to
    fetch.
    """
    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    async def request(self, method, path, *, params=None, signed=False, weight=1.0):
        self.calls.append((method, path))
        if "openOrders" in path:
            raise RuntimeError("simulated transient GET failure")
        return []


def _open_order_row(coid: str, *, order_id: str = "999") -> dict:
    return {
        "clientOrderId": coid, "orderId": order_id, "side": "BUY",
        "type": "LIMIT", "timeInForce": "GTC", "origQty": "0.01",
        "executedQty": "0", "price": "70000", "time": 1700000000000,
    }


def _cancel_requests(bus: _CaptureBus) -> list:
    from trading.core.events import CancelRequest
    return [e for t, e in bus.published
            if t == Topic.ORDERS and isinstance(e, CancelRequest)]


async def test_resync_adopts_untracked_order_verbatim_no_cancel(clock, btc) -> None:
    """Resync adopts an untracked venue order into the OMS verbatim and does
    NOT cancel it. Deciding whether the order should rest is the per-strategy
    reconciliation loop's job (it matches against desired legs), not the
    resync's — adopted and self-placed orders flow through the same matching."""
    oms_bus = _CaptureBus()
    oms = OMSEngine(bus=oms_bus, clock=clock)
    rest = _FakeREST({
        ("GET", "/openOrders"): [_open_order_row("mm-0123456789ab")],
        ("GET", "/fapi/v2/positionRisk"): [],
    })
    boot = StateBootstrapper(
        bus=_CaptureBus(), clock=clock, config=_futures_config(), rest=rest,
        oms=oms, symbols=SymbolMapper([btc]), tracked_instruments=[btc],
    )
    await boot._resync_once()

    # No cancel issued by the resync itself.
    assert _cancel_requests(oms_bus) == []
    # Order is tracked in the state the venue reported.
    adopted = list(oms.open_orders())
    assert len(adopted) == 1
    assert str(adopted[0].client_order_id) == "mm-0123456789ab"
    assert adopted[0].side is Side.BUY
    assert adopted[0].price == Decimal("70000")


async def test_resync_adopt_is_idempotent_across_passes(clock, btc) -> None:
    """Re-running resync against the same venue order does not duplicate it or
    cancel it — adoption is idempotent on client_order_id."""
    oms_bus = _CaptureBus()
    oms = OMSEngine(bus=oms_bus, clock=clock)
    rest = _FakeREST({
        ("GET", "/openOrders"): [_open_order_row("mm-0123456789ab")],
        ("GET", "/fapi/v2/positionRisk"): [],
    })
    boot = StateBootstrapper(
        bus=_CaptureBus(), clock=clock, config=_futures_config(), rest=rest,
        oms=oms, symbols=SymbolMapper([btc]), tracked_instruments=[btc],
    )
    await boot._resync_once()
    oms_bus.published.clear()
    await boot._resync_once()

    assert _cancel_requests(oms_bus) == []
    assert len(list(oms.open_orders())) == 1


async def test_resync_skips_terminalize_on_failed_fetch(clock, btc) -> None:
    """If the /openOrders GET fails, the symbol is absent from fetched_iids and
    the resync must NOT terminalize the locally-open order — it is still live
    on the venue; we just couldn't read venue state this pass."""
    oms_bus = _CaptureBus()
    oms = OMSEngine(bus=oms_bus, clock=clock)
    # Seed a tracked open order.
    await oms.adopt_order(
        instrument=btc,
        client_order_id="mm-0123456789ab",
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.01"),
        cumulative_filled=Decimal("0"),
        price=Decimal("70000"),
        time_in_force=TimeInForce.GTC,
        exchange_order_id="999",
    )
    assert len(list(oms.open_orders())) == 1

    boot = StateBootstrapper(
        bus=_CaptureBus(), clock=clock, config=_futures_config(),
        rest=_RaisingREST(), oms=oms,
        symbols=SymbolMapper([btc]), tracked_instruments=[btc],
    )
    await boot._resync_once()

    # Order survived — not terminalized despite the failed fetch.
    assert len(list(oms.open_orders())) == 1
