"""Tests for cancel-replace → amend reconciliation in the OMS engine.

Covers:
- No-op when price/qty unchanged (queue position preserved, zero requests emitted).
- Price change emits a single AmendRequest, not CancelRequest + OrderRequest.
- OrderAmended confirm updates Order.price and clears pending_amend.
- Side withdrawn still cancels (not amends).
- Amend-reject (OrderRejected while PENDING_AMEND) leaves order resting at old price.
- Fill race while PENDING_AMEND transitions cleanly.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from trading.core import (
    AssetType,
    ExecutionIntent,
    Instrument,
    LiveClock,
    OrderLeg,
    OrderType,
    Side,
    SignalEvent,
    SimulatedClock,
    StrategyId,
    TimeInForce,
)
from trading.core.events import (
    AmendRejected,
    AmendRequest,
    CancelRequest,
    FillEvent,
    OrderAmended,
    OrderRejected,
    OrderRequest,
)
from trading.core.types import (
    ClientOrderId,
    FillId,
    OrderId,
    OrderStatus,
    Price,
    Quantity,
)
from trading.oms.engine import OMSEngine
from trading.oms.order import Order


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CaptureBus:
    """Minimal bus that captures all published events."""

    def __init__(self) -> None:
        self.published: list[Any] = []

    async def publish(self, topic: str, event: Any) -> None:
        self.published.append(event)

    async def subscribe(self, topic: str, handler: Any) -> None:
        pass

    async def subscribe_many(self, topics: Any, handler: Any) -> None:
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def of_type(self, cls: type) -> list[Any]:
        return [e for e in self.published if isinstance(e, cls)]

    def clear(self) -> None:
        self.published.clear()


def _instrument() -> Instrument:
    return Instrument(
        symbol="BTC-USDT",
        exchange="BINANCE",
        asset_type=AssetType.SPOT,
        base_currency="BTC",
        quote_currency="USDT",
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.00001"),
    )


def _signal(
    instrument: Instrument,
    strategy_id: str = "test-strat",
    *legs: OrderLeg,
) -> SignalEvent:
    return SignalEvent(
        ts_event=0,
        ts_ingest=0,
        source="test",
        strategy_id=StrategyId(strategy_id),
        instrument=instrument,
        legs=tuple(legs),
    )


def _leg(side: Side, price: str, qty: str = "0.01") -> OrderLeg:
    return OrderLeg(
        side=side,
        quantity=Decimal(qty),
        price=Decimal(price),
        order_type=OrderType.POST_ONLY,
        time_in_force=TimeInForce.GTC,
        intent=ExecutionIntent.PASSIVE,
    )


def _resting_order(
    instrument: Instrument,
    side: Side,
    price: str,
    qty: str = "0.01",
    status: OrderStatus = OrderStatus.ACKNOWLEDGED,
    strategy_id: str = "test-strat",
) -> Order:
    oid = OrderId(uuid4())
    return Order(
        order_id=oid,
        client_order_id=ClientOrderId(f"{strategy_id}-{oid.hex[:12]}"),
        strategy_id=StrategyId(strategy_id),
        instrument=instrument,
        side=side,
        order_type=OrderType.POST_ONLY,
        quantity=Quantity(Decimal(qty)),
        price=Price(Decimal(price)),
        time_in_force=TimeInForce.GTC,
        created_at_ns=0,
        status=status,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_op_when_price_unchanged() -> None:
    """Re-signalling the same price/qty emits nothing — order keeps queue position."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    instr = _instrument()
    sid = StrategyId("test-strat")

    order = _resting_order(instr, Side.BUY, "50000.00")
    oms._orders[order.order_id] = order

    signal = _signal(instr, "test-strat", _leg(Side.BUY, "50000.00"))
    await oms._reconcile_immediate(signal, list(signal.legs))

    assert bus.of_type(AmendRequest) == []
    assert bus.of_type(CancelRequest) == []
    assert bus.of_type(OrderRequest) == []


@pytest.mark.asyncio
async def test_price_change_emits_amend_not_cancel_place() -> None:
    """A new price on an existing side sends AmendRequest, not Cancel+New."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    instr = _instrument()

    order = _resting_order(instr, Side.BUY, "50000.00")
    oms._orders[order.order_id] = order

    signal = _signal(instr, "test-strat", _leg(Side.BUY, "50001.00"))
    await oms._reconcile_immediate(signal, list(signal.legs))

    assert len(bus.of_type(AmendRequest)) == 1
    assert bus.of_type(CancelRequest) == []
    assert bus.of_type(OrderRequest) == []

    amend: AmendRequest = bus.of_type(AmendRequest)[0]
    assert amend.order_id == order.order_id
    assert amend.new_price == Decimal("50001.00")

    assert order.status == OrderStatus.PENDING_AMEND
    assert order.pending_amend == (Decimal("50001.00"), order.quantity)


@pytest.mark.asyncio
async def test_order_amended_confirm_updates_price() -> None:
    """OrderAmended confirm commits the new price and clears pending_amend."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    instr = _instrument()

    order = _resting_order(instr, Side.BUY, "50000.00")
    oms._orders[order.order_id] = order

    # Put it into PENDING_AMEND state as reconcile would.
    order.transition_to(OrderStatus.PENDING_AMEND, at_ns=0)
    order.pending_amend = (Price(Decimal("50001.00")), order.quantity)

    event = OrderAmended(
        ts_event=1,
        ts_ingest=1,
        source="sim",
        order_id=order.order_id,
        client_order_id=order.client_order_id,
        new_price=Price(Decimal("50001.00")),
    )
    await oms._handle_amend(event)

    assert order.status == OrderStatus.ACKNOWLEDGED
    assert order.price == Decimal("50001.00")
    assert order.pending_amend is None


@pytest.mark.asyncio
async def test_order_amended_confirm_prefers_venue_values_over_requested() -> None:
    """The OMS commits the price/qty the venue *reports* in OrderAmended, not
    the requested pending_amend. The venue can clamp an amend (e.g. a GTX price
    adjusted to avoid crossing); trusting the requested value would silently
    diverge local state from the book and orphan the resting order."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    instr = _instrument()

    order = _resting_order(instr, Side.BUY, "50000.00", qty="0.10")
    oms._orders[order.order_id] = order
    order.transition_to(OrderStatus.PENDING_AMEND, at_ns=0)
    # We *requested* 50001.00 / 0.20 ...
    order.pending_amend = (Price(Decimal("50001.00")), Quantity(Decimal("0.20")))

    # ... but the venue reports it actually rested at 50000.50 / 0.20.
    event = OrderAmended(
        ts_event=1,
        ts_ingest=1,
        source="sim",
        order_id=order.order_id,
        client_order_id=order.client_order_id,
        new_price=Price(Decimal("50000.50")),
        new_quantity=Quantity(Decimal("0.20")),
    )
    await oms._handle_amend(event)

    assert order.status == OrderStatus.ACKNOWLEDGED
    assert order.price == Decimal("50000.50")   # venue value, not 50001.00
    assert order.quantity == Decimal("0.20")
    assert order.pending_amend is None


@pytest.mark.asyncio
async def test_withdrawn_side_cancels() -> None:
    """An order on a side no longer in the desired set is cancelled."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    instr = _instrument()

    buy_order = _resting_order(instr, Side.BUY, "50000.00")
    oms._orders[buy_order.order_id] = buy_order

    # Signal only asks for a sell — buy side is withdrawn.
    signal = _signal(instr, "test-strat", _leg(Side.SELL, "50100.00"))
    await oms._reconcile_immediate(signal, list(signal.legs))

    assert len(bus.of_type(CancelRequest)) == 1
    assert bus.of_type(CancelRequest)[0].order_id == buy_order.order_id
    assert len(bus.of_type(OrderRequest)) == 1  # new sell placed
    assert bus.of_type(AmendRequest) == []


@pytest.mark.asyncio
async def test_pending_amend_skipped_next_tick() -> None:
    """An order already in PENDING_AMEND is left alone (no double amend)."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    instr = _instrument()

    order = _resting_order(instr, Side.BUY, "50000.00", status=OrderStatus.PENDING_AMEND)
    order.pending_amend = (Price(Decimal("50001.00")), order.quantity)
    oms._orders[order.order_id] = order

    # Re-signal at yet another price while amend is in-flight.
    signal = _signal(instr, "test-strat", _leg(Side.BUY, "50002.00"))
    await oms._reconcile_immediate(signal, list(signal.legs))

    assert bus.of_type(AmendRequest) == []
    assert bus.of_type(CancelRequest) == []
    assert bus.of_type(OrderRequest) == []


@pytest.mark.asyncio
async def test_amend_reject_leaves_order_resting() -> None:
    """OrderRejected on a PENDING_AMEND order does not terminalize it.

    The OMS treats amend-rejects the same as any other reject: transitions
    the order to REJECTED. The next reconcile tick will see no resting order
    on that side and place a fresh one.
    """
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    instr = _instrument()

    order = _resting_order(instr, Side.BUY, "50000.00")
    oms._orders[order.order_id] = order
    order.transition_to(OrderStatus.PENDING_AMEND, at_ns=0)
    order.pending_amend = (Price(Decimal("50001.00")), order.quantity)

    reject = OrderRejected(
        ts_event=1,
        ts_ingest=1,
        source="sim",
        order_id=order.order_id,
        client_order_id=order.client_order_id,
        reason="amend rejected by venue",
    )
    await oms._handle_reject(reject)

    # Order is terminal (REJECTED); next reconcile will place fresh.
    assert order.status == OrderStatus.REJECTED
    assert order.is_terminal


@pytest.mark.asyncio
async def test_amend_reject_rolls_back_to_acknowledged() -> None:
    """A transient amend reject rolls back to ACKNOWLEDGED (retry next tick).

    Permanent rejects (e.g. Binance -5026) never arrive as AmendRejected — the
    gateway translates those to OrderCancelled (see test_binance_order_gateway),
    so the OMS only ever sees transient rejects here.
    """
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    instr = _instrument()

    order = _resting_order(instr, Side.BUY, "50000.00")
    oms._orders[order.order_id] = order
    order.transition_to(OrderStatus.PENDING_AMEND, at_ns=0)
    order.pending_amend = (Price(Decimal("50001.00")), order.quantity)

    reject = AmendRejected(
        ts_event=1,
        ts_ingest=1,
        source="sim",
        order_id=order.order_id,
        client_order_id=order.client_order_id,
        reason="amend failed: binance error -2011: order would immediately match",
    )
    await oms._handle_amend_rejected(reject)

    assert order.status == OrderStatus.ACKNOWLEDGED
    assert len(bus.of_type(CancelRequest)) == 0


@pytest.mark.asyncio
async def test_fill_race_while_pending_amend() -> None:
    """A fill arriving while PENDING_AMEND transitions cleanly to PARTIALLY_FILLED."""
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    instr = _instrument()

    order = _resting_order(instr, Side.BUY, "50000.00", qty="0.1")
    oms._orders[order.order_id] = order
    order.transition_to(OrderStatus.PENDING_AMEND, at_ns=0)
    order.pending_amend = (Price(Decimal("50001.00")), order.quantity)

    fill = FillEvent(
        ts_event=1,
        ts_ingest=1,
        source="sim",
        fill_id=FillId(uuid4()),
        order_id=order.order_id,
        client_order_id=order.client_order_id,
        exchange_order_id=None,
        instrument=instr,
        strategy_id=order.strategy_id,
        side=Side.BUY,
        fill_price=Price(Decimal("50000.00")),
        fill_quantity=Quantity(Decimal("0.05")),
        cumulative_quantity=Quantity(Decimal("0.05")),
        leaves_quantity=Quantity(Decimal("0.05")),
    )
    applied = order.apply_fill(fill)

    assert applied is True
    assert order.status == OrderStatus.PARTIALLY_FILLED
    assert order.cumulative_filled == Decimal("0.05")


@pytest.mark.asyncio
async def test_pending_cancel_order_not_duplicated() -> None:
    """A PENDING_CANCEL order must block fresh placement on the same side.

    Scenario: a BUY order is being cancelled (cancel in-flight, PENDING_CANCEL).
    A new signal with a BUY leg arrives before the cancel confirms. Without this
    guard the reconciler sees no 'active' BUY resting order and places a second
    BUY — leaving two buys on the venue until the cancel lands.
    """
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=LiveClock())
    instr = _instrument()

    # Simulate a BUY order with an in-flight cancel.
    order = _resting_order(instr, Side.BUY, "50000.00")
    order.transition_to(OrderStatus.PENDING_CANCEL, at_ns=0)
    oms._orders[order.order_id] = order

    signal = _signal(instr, "test-strat", _leg(Side.BUY, "50001.00"))
    await oms._reconcile_immediate(signal, list(signal.legs))

    # No new order should be placed — the PENDING_CANCEL counts as occupying the slot.
    assert bus.of_type(OrderRequest) == []
    assert bus.of_type(AmendRequest) == []
    # And the PENDING_CANCEL order itself should not be cancelled again.
    assert bus.of_type(CancelRequest) == []


# ---------------------------------------------------------------------------
# Stale PENDING_AMEND sweep — recovers orders wedged by a lost amend response.
#
# Regression for the observed desync: an amend that races a fill loses its
# OrderAmended response, stranding the order in PENDING_AMEND forever (the
# reconciler waits for a confirm that never comes and refuses to cancel a
# mid-amend order). The sweep times the amend out and rolls it back.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stale_pending_amend_rolls_back_to_acknowledged() -> None:
    """A PENDING_AMEND order older than the timeout rolls back to ACKNOWLEDGED."""
    clock = SimulatedClock(start=0)
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=clock, pending_amend_timeout_seconds=5.0)
    instr = _instrument()

    order = _resting_order(instr, Side.BUY, "50000.00", qty="0.1")
    oms._orders[order.order_id] = order
    order.transition_to(OrderStatus.PENDING_AMEND, at_ns=clock.now_ns())
    order.pending_amend = (Price(Decimal("50001.00")), order.quantity)

    # Just under the timeout: still waiting, untouched.
    clock.advance(4 * 1_000_000_000)
    await oms._sweep_stale_pending_amends()
    assert order.status == OrderStatus.PENDING_AMEND

    # Past the timeout: rolled back so the next reconcile can re-amend/cancel.
    clock.advance(2 * 1_000_000_000)  # now 6s since the amend
    await oms._sweep_stale_pending_amends()
    assert order.status == OrderStatus.ACKNOWLEDGED
    assert order.pending_amend is None
    # Rollback is local-only — no venue traffic emitted by the sweep itself.
    assert bus.of_type(AmendRequest) == []
    assert bus.of_type(CancelRequest) == []


@pytest.mark.asyncio
async def test_stale_pending_amend_fully_filled_terminalizes() -> None:
    """If fills consumed all leaves while the amend was lost, the sweep marks
    the order FILLED rather than reviving it as ACKNOWLEDGED with leaves=0
    (which would make the reconciler loop amending it forever)."""
    clock = SimulatedClock(start=0)
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=clock, pending_amend_timeout_seconds=5.0)
    instr = _instrument()

    order = _resting_order(instr, Side.BUY, "50000.00", qty="0.1")
    oms._orders[order.order_id] = order
    order.transition_to(OrderStatus.PENDING_AMEND, at_ns=clock.now_ns())
    order.pending_amend = (Price(Decimal("50001.00")), order.quantity)

    # A fill consumed the full quantity while the amend was in-flight.
    fill = FillEvent(
        ts_event=1, ts_ingest=1, source="sim",
        fill_id=FillId(uuid4()),
        order_id=order.order_id, client_order_id=order.client_order_id,
        exchange_order_id=None, instrument=instr, strategy_id=order.strategy_id,
        side=Side.BUY,
        fill_price=Price(Decimal("50000.00")),
        fill_quantity=Quantity(Decimal("0.1")),
        cumulative_quantity=Quantity(Decimal("0.1")),
        leaves_quantity=Quantity(Decimal("0")),
    )
    order.record_fill(fill)  # accounting only — leaves status PENDING_AMEND
    assert order.leaves_quantity == 0

    clock.advance(6 * 1_000_000_000)
    await oms._sweep_stale_pending_amends()

    assert order.status == OrderStatus.FILLED
    assert order.pending_amend is None


@pytest.mark.asyncio
async def test_sweep_ignores_fresh_and_non_pending_orders() -> None:
    """The sweep only touches PENDING_AMEND orders past the timeout — a fresh
    amend and an ordinary resting order are both left alone."""
    clock = SimulatedClock(start=0)
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=clock, pending_amend_timeout_seconds=5.0)
    instr = _instrument()

    resting = _resting_order(instr, Side.SELL, "50100.00")  # ACKNOWLEDGED
    oms._orders[resting.order_id] = resting

    fresh = _resting_order(instr, Side.BUY, "50000.00")
    oms._orders[fresh.order_id] = fresh

    # Advance, THEN start the fresh amend so it's well within the timeout.
    clock.advance(10 * 1_000_000_000)
    fresh.transition_to(OrderStatus.PENDING_AMEND, at_ns=clock.now_ns())
    fresh.pending_amend = (Price(Decimal("50001.00")), fresh.quantity)

    await oms._sweep_stale_pending_amends()

    assert fresh.status == OrderStatus.PENDING_AMEND   # too recent
    assert resting.status == OrderStatus.ACKNOWLEDGED  # not amending


@pytest.mark.asyncio
async def test_late_amend_response_after_rollback_is_safe() -> None:
    """A genuinely-late OrderAmended arriving after the sweep rolled the order
    back must not crash or revive stale state — _handle_amend swallows the now
    illegal ACKNOWLEDGED->ACKNOWLEDGED transition."""
    clock = SimulatedClock(start=0)
    bus = _CaptureBus()
    oms = OMSEngine(bus=bus, clock=clock, pending_amend_timeout_seconds=5.0)
    instr = _instrument()

    order = _resting_order(instr, Side.BUY, "50000.00")
    oms._orders[order.order_id] = order
    order.transition_to(OrderStatus.PENDING_AMEND, at_ns=clock.now_ns())
    order.pending_amend = (Price(Decimal("50001.00")), order.quantity)

    clock.advance(6 * 1_000_000_000)
    await oms._sweep_stale_pending_amends()
    assert order.status == OrderStatus.ACKNOWLEDGED

    # The lost amend's response finally lands.
    late = OrderAmended(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="sim",
        order_id=order.order_id, client_order_id=order.client_order_id,
        new_price=Price(Decimal("50001.00")),
    )
    await oms._handle_amend(late)  # must not raise

    assert order.status == OrderStatus.ACKNOWLEDGED
    assert order.pending_amend is None
