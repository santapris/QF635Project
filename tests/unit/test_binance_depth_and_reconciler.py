"""Unit tests for the Binance depth book manager and balance reconciler."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest

from trading.core import (
    AssetType,
    ClientOrderId,
    ExchangeOrderId,
    FillEvent,
    Instrument,
    LiveClock,
    OrderId,
    RiskAlertEvent,
    Side,
    StrategyId,
)
from trading.core.exceptions import SequenceGapError
from trading.event_bus import MemoryBus, Topic
from trading.order_gateways.binance import (
    BalanceReconciler,
    BinanceConfig,
    DepthBookManager,
    SymbolMapper,
)
from trading.position import AccountingMethod, PositionEngine
from uuid import uuid4


# --- Fixtures ------------------------------------------------------------

@pytest.fixture
def btc() -> Instrument:
    return Instrument(
        symbol="BTC-USDT", exchange="BINANCE", asset_type=AssetType.SPOT,
        base_currency="BTC", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.00001"),
    )


@pytest.fixture
def eth() -> Instrument:
    return Instrument(
        symbol="ETH-USDT", exchange="BINANCE", asset_type=AssetType.SPOT,
        base_currency="ETH", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.0001"),
    )


@pytest.fixture
def mapper(btc, eth) -> SymbolMapper:
    return SymbolMapper([btc, eth])


class _FakeREST:
    """Records calls; returns canned responses for {api_prefix}/depth and {api_prefix}/account."""

    def __init__(self):
        self.calls: list[tuple[str, str, dict | None]] = []
        self.responses: list[Any] = []

    async def connect(self) -> None: pass
    async def close(self) -> None: pass

    async def request(self, method, path, *, params=None, signed=False, user_data=False, weight=1.0):
        self.calls.append((method, path, dict(params) if params else None))
        if not self.responses:
            raise AssertionError(f"unexpected REST call: {method} {path}")
        resp = self.responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp


# --- DepthBookManager interleave rules ----------------------------------

def _snapshot(last_update_id, bids, asks):
    return {
        "lastUpdateId": last_update_id,
        "bids": bids,
        "asks": asks,
    }


def _diff(U, u, bids=None, asks=None):
    return {
        "e": "depthUpdate", "E": 0, "s": "BTCUSDT",
        "U": U, "u": u,
        "b": bids or [],
        "a": asks or [],
    }


async def test_depth_bootstrap_drops_stale_buffered_events(btc, mapper):
    """Events with u <= lastUpdateId must be dropped per binance docs."""
    rest = _FakeREST()
    rest.responses.append(_snapshot(
        last_update_id=1000,
        bids=[["50000", "1.0"]], asks=[["50100", "1.0"]],
    ))
    mgr = DepthBookManager(rest=rest, symbols=mapper, instrument=btc)
    # Pre-snapshot: feed a stale event (u=900 < 1000), then a valid one.
    mgr.apply_diff(_diff(U=801, u=900, bids=[["49000", "5.0"]]))
    mgr.apply_diff(_diff(U=1001, u=1005, bids=[["50000", "1.5"]]))
    await mgr.bootstrap()

    tob = mgr.book.top_of_book()
    # The stale event @ 49000 should NOT have applied; bid is whatever the
    # valid (post-snapshot) event left.
    assert tob is not None
    assert tob.bid_price == Decimal("50000")
    assert tob.bid_size == Decimal("1.5")  # updated by the U=1001 event


async def test_depth_bootstrap_requires_first_event_to_straddle_snapshot(btc, mapper):
    """The first applied event must satisfy U <= lastUpdateId+1 <= u.
    If the buffer has a gap that skips past the snapshot, abort."""
    rest = _FakeREST()
    rest.responses.append(_snapshot(
        last_update_id=1000,
        bids=[["50000", "1.0"]], asks=[["50100", "1.0"]],
    ))
    mgr = DepthBookManager(rest=rest, symbols=mapper, instrument=btc)
    # Buffer one stale and one too-far-future event — no event spans
    # lastUpdateId+1=1001.
    mgr.apply_diff(_diff(U=801, u=900, bids=[["1", "1"]]))
    mgr.apply_diff(_diff(U=1005, u=1010, bids=[["2", "1"]]))
    await mgr.bootstrap()
    # The book should be reset, awaiting another bootstrap.
    assert not mgr.is_initialized


async def test_depth_bootstrap_applies_clean_sequence(btc, mapper):
    rest = _FakeREST()
    rest.responses.append(_snapshot(
        last_update_id=1000,
        bids=[["50000", "1.0"]], asks=[["50100", "1.0"]],
    ))
    mgr = DepthBookManager(rest=rest, symbols=mapper, instrument=btc)
    # Event spans the snapshot: U=999 <= 1001 <= u=1002. Applies.
    mgr.apply_diff(_diff(U=999, u=1002, bids=[["50000", "1.5"]]))
    # Next event is contiguous: U=1003 = previous u+1.
    mgr.apply_diff(_diff(U=1003, u=1005, bids=[["50001", "2.0"]]))
    await mgr.bootstrap()
    tob = mgr.book.top_of_book()
    assert tob is not None
    # Best bid is the higher price: 50001 (added in second event)
    assert tob.bid_price == Decimal("50001")


async def test_depth_post_bootstrap_gap_raises(btc, mapper):
    rest = _FakeREST()
    rest.responses.append(_snapshot(
        last_update_id=1000,
        bids=[["50000", "1.0"]], asks=[["50100", "1.0"]],
    ))
    mgr = DepthBookManager(rest=rest, symbols=mapper, instrument=btc)
    mgr.apply_diff(_diff(U=999, u=1002, bids=[["50000", "1.5"]]))
    await mgr.bootstrap()
    # Skip from u=1002 to U=1010 — gap.
    with pytest.raises(SequenceGapError):
        mgr.apply_diff(_diff(U=1010, u=1015, bids=[["50000", "9.9"]]))
    # And the book should be reset.
    assert not mgr.is_initialized


async def test_depth_book_zero_quantity_removes_level(btc, mapper):
    """Binance signals removal of a price level by quantity=0."""
    rest = _FakeREST()
    rest.responses.append(_snapshot(
        last_update_id=1000,
        bids=[["50000", "1.0"], ["49999", "2.0"]],
        asks=[["50100", "1.0"]],
    ))
    mgr = DepthBookManager(rest=rest, symbols=mapper, instrument=btc)
    mgr.apply_diff(_diff(U=1001, u=1002, bids=[["50000", "0"]]))  # remove 50000
    await mgr.bootstrap()
    tob = mgr.book.top_of_book()
    assert tob is not None
    assert tob.bid_price == Decimal("49999")  # 50000 was removed


# --- BalanceReconciler --------------------------------------------------

async def _make_position_engine_with_position(clock, instrument, qty: str):
    """Helper: build a PositionEngine and inject one fill so it has a position."""
    bus = MemoryBus()
    engine = PositionEngine(bus=bus, clock=clock, method=AccountingMethod.WAVG)
    await engine.start()
    if Decimal(qty) != 0:
        await bus.publish(Topic.FILLS, FillEvent(
            ts_event=0, ts_ingest=0, source="test",
            order_id=OrderId(uuid4()),
            client_order_id=ClientOrderId("c1"),
            exchange_order_id=ExchangeOrderId("ex1"),
            strategy_id=StrategyId("s1"),
            instrument=instrument,
            side=Side.BUY,
            fill_price=Decimal("50000"),
            fill_quantity=Decimal(qty),
            cumulative_quantity=Decimal(qty),
            leaves_quantity=Decimal("0"),
        ))
    return bus, engine


async def test_reconciler_no_alert_when_matched(btc):
    clock = LiveClock()
    bus, pe = await _make_position_engine_with_position(clock, btc, "1.0")
    # Venue reports exactly 1.0 BTC.
    rest = _FakeREST()
    rest.responses.append({
        "balances": [
            {"asset": "BTC", "free": "1.0", "locked": "0"},
            {"asset": "USDT", "free": "50000", "locked": "0"},
        ],
    })
    alerts = []
    async def cap(e): alerts.append(e)
    await bus.subscribe(Topic.ALERTS, cap)

    rec = BalanceReconciler(
        bus=bus, clock=clock, config=BinanceConfig(
        spot_rest_base="https://testnet.binance.vision",
        spot_ws_base="wss://testnet.binance.vision",
        futures_rest_base="",
        futures_ws_base="",
    ),
        rest=rest, position_engine=pe, tracked_instruments=[btc],
        mismatch_threshold=Decimal("0.0001"),
    )
    result = await rec.reconcile_once()
    assert result["BTC"] == (Decimal("1.0"), Decimal("1.0"))
    assert alerts == []


async def test_reconciler_alerts_on_divergence(btc):
    clock = LiveClock()
    bus, pe = await _make_position_engine_with_position(clock, btc, "1.0")
    # Venue reports 1.5 — we're short by 0.5.
    rest = _FakeREST()
    rest.responses.append({
        "balances": [{"asset": "BTC", "free": "1.5", "locked": "0"}],
    })
    alerts: list[RiskAlertEvent] = []
    async def cap(e):
        if isinstance(e, RiskAlertEvent):
            alerts.append(e)
    await bus.subscribe(Topic.ALERTS, cap)

    rec = BalanceReconciler(
        bus=bus, clock=clock, config=BinanceConfig(
        spot_rest_base="https://testnet.binance.vision",
        spot_ws_base="wss://testnet.binance.vision",
        futures_rest_base="",
        futures_ws_base="",
    ),
        rest=rest, position_engine=pe, tracked_instruments=[btc],
        mismatch_threshold=Decimal("0.0001"),
    )
    await rec.reconcile_once()
    assert len(alerts) == 1
    assert alerts[0].rule_name == "balance_reconcile"
    assert alerts[0].metadata["asset"] == "BTC"
    assert alerts[0].metadata["diff"] == "0.5"


async def test_reconciler_ignores_untracked_assets(btc):
    """The venue has USDT, BNB, etc. — we don't track those, must not alert."""
    clock = LiveClock()
    bus, pe = await _make_position_engine_with_position(clock, btc, "1.0")
    rest = _FakeREST()
    rest.responses.append({
        "balances": [
            {"asset": "BTC", "free": "1.0", "locked": "0"},
            {"asset": "USDT", "free": "1000000", "locked": "0"},
            {"asset": "BNB", "free": "5", "locked": "0"},
        ],
    })
    alerts: list[RiskAlertEvent] = []
    async def cap(e):
        if isinstance(e, RiskAlertEvent):
            alerts.append(e)
    await bus.subscribe(Topic.ALERTS, cap)

    rec = BalanceReconciler(
        bus=bus, clock=clock, config=BinanceConfig(
        spot_rest_base="https://testnet.binance.vision",
        spot_ws_base="wss://testnet.binance.vision",
        futures_rest_base="",
        futures_ws_base="",
    ),
        rest=rest, position_engine=pe, tracked_instruments=[btc],
    )
    await rec.reconcile_once()
    assert alerts == []


async def test_reconciler_threshold_suppresses_small_diffs(btc):
    """Tiny rounding differences shouldn't fire alerts."""
    clock = LiveClock()
    bus, pe = await _make_position_engine_with_position(clock, btc, "1.0")
    rest = _FakeREST()
    rest.responses.append({
        "balances": [{"asset": "BTC", "free": "1.00001", "locked": "0"}],  # 0.00001 diff
    })
    alerts: list[RiskAlertEvent] = []
    async def cap(e):
        if isinstance(e, RiskAlertEvent):
            alerts.append(e)
    await bus.subscribe(Topic.ALERTS, cap)

    rec = BalanceReconciler(
        bus=bus, clock=clock, config=BinanceConfig(
        spot_rest_base="https://testnet.binance.vision",
        spot_ws_base="wss://testnet.binance.vision",
        futures_rest_base="",
        futures_ws_base="",
    ),
        rest=rest, position_engine=pe, tracked_instruments=[btc],
        mismatch_threshold=Decimal("0.001"),  # 0.001 threshold > 0.00001 diff
    )
    await rec.reconcile_once()
    assert alerts == []
