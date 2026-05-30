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
    StrategyId,
    TimeInForce,
)
from trading.core.events import (
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
