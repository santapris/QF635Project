"""Unit tests for OMS engine internals — child/leg bookkeeping for sliced legs."""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from trading.core import (
    AssetType,
    Instrument,
    LiveClock,
    OrderType,
    Side,
    StrategyId,
    TimeInForce,
)
import asyncio

from trading.core.events import FillEvent
from trading.core.types import ClientOrderId, OrderId, OrderStatus
from trading.oms.engine import OMSEngine
from trading.oms.order import Order


class _NullBus:
    async def publish(self, topic, event): pass
    async def subscribe(self, topic, handler): pass
    async def subscribe_many(self, topics, handler): pass
    async def start(self): pass
    async def stop(self): pass


def _make_order(
    *,
    leg_id: str | None = None,
    quantity: str = "1",
    filled: str = "0",
    terminal: bool = False,
) -> Order:
    o = Order(
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId(f"test-{uuid4().hex[:8]}"),
        strategy_id=StrategyId("test-strat"),
        instrument=Instrument(
            symbol="BTC-USDT",
            exchange="BINANCE",
            asset_type=AssetType.SPOT,
            base_currency="BTC",
            quote_currency="USDT",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.00001"),
        ),
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal(quantity),
        price=Decimal("50000"),
        time_in_force=TimeInForce.GTC,
        created_at_ns=0,
        parent_leg_id=leg_id,
    )
    o.cumulative_filled = Decimal(filled)
    if terminal:
        o.status = OrderStatus.FILLED
    return o


def _oms() -> OMSEngine:
    return OMSEngine(bus=_NullBus(), clock=LiveClock())


def test_leg_has_live_children_true_when_open_child_exists() -> None:
    oms = _oms()
    child = _make_order(leg_id="leg-1")
    oms._orders[child.order_id] = child
    assert oms._leg_has_live_children("leg-1") is True


def test_leg_has_live_children_false_when_only_terminal_children() -> None:
    oms = _oms()
    done = _make_order(leg_id="leg-1", quantity="1", filled="1", terminal=True)
    oms._orders[done.order_id] = done
    assert oms._leg_has_live_children("leg-1") is False


def test_leg_has_live_children_ignores_other_legs() -> None:
    oms = _oms()
    other = _make_order(leg_id="leg-2")
    oms._orders[other.order_id] = other
    assert oms._leg_has_live_children("leg-1") is False


# --- fill-on-terminal race (amend-gone / cancel-fill) ----------------------


def _fill(order: Order, *, qty: str, price: str = "50000") -> FillEvent:
    return FillEvent(
        ts_event=0,
        ts_ingest=0,
        source="test",
        order_id=order.order_id,
        client_order_id=order.client_order_id,
        exchange_order_id=None,
        strategy_id=order.strategy_id,
        instrument=order.instrument,
        side=order.side,
        fill_price=Decimal(price),
        fill_quantity=Decimal(qty),
        cumulative_quantity=Decimal(qty),
        leaves_quantity=Decimal("0"),
    )


def test_record_fill_updates_accounting_without_transition() -> None:
    # A terminal order can still absorb fill accounting; status is untouched.
    order = _make_order(quantity="1", filled="0", terminal=True)
    assert order.status is OrderStatus.FILLED  # _make_order terminal marker

    applied = order.record_fill(_fill(order, qty="0.4"))

    assert applied is True
    assert order.cumulative_filled == Decimal("0.4")
    assert order.average_fill_price == Decimal("50000")
    assert order.status is OrderStatus.FILLED  # no illegal transition attempted


def test_record_fill_is_idempotent_on_duplicate() -> None:
    order = _make_order(quantity="1", terminal=True)
    fill = _fill(order, qty="0.4")
    assert order.record_fill(fill) is True
    assert order.record_fill(fill) is False  # same fill_id — dup
    assert order.cumulative_filled == Decimal("0.4")


def test_on_fill_records_onto_terminal_order_not_dropped() -> None:
    # Regression: amend-gone (-2013) terminalized the order as CANCELLED, then
    # the authoritative fill arrived and used to be silently dropped.
    oms = _oms()
    order = _make_order(quantity="0.002", filled="0")
    order.status = OrderStatus.CANCELLED  # terminalized by amend-gone race
    oms._orders[order.order_id] = order

    asyncio.run(oms._on_fill(_fill(order, qty="0.002", price="73501.7")))

    assert order.cumulative_filled == Decimal("0.002")
    assert order.leaves_quantity == Decimal("0")
    assert order.status is OrderStatus.CANCELLED  # stays terminal, no crash


def test_leg_has_live_children_ignores_plain_orders() -> None:
    """Orders with no parent_leg_id are plain quotes, not slice children."""
    oms = _oms()
    plain = _make_order(leg_id=None)
    oms._orders[plain.order_id] = plain
    assert oms._leg_has_live_children("leg-1") is False


def test_retire_algo_removes_state() -> None:
    from trading.oms.execution_algos import TWAPAlgo

    oms = _oms()
    algo = TWAPAlgo(
        quantity=Decimal("1"), duration_seconds=60, num_slices=5, start_ns=0,
    )
    oms._algos["leg-1"] = algo
    oms._algo_ctx["leg-1"] = (None, None)  # context not needed for retire

    oms._retire_algo("leg-1")
    assert "leg-1" not in oms._algos
    assert "leg-1" not in oms._algo_ctx
    assert algo.is_done()  # cancel() makes it done


# --- kill switch → cancel-all -----------------------------------------------


class _CapturingBus(_NullBus):
    """Bus that records every published (topic, event) pair."""

    def __init__(self) -> None:
        self.published: list[tuple[str, object]] = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


def _oms_with_bus(bus) -> OMSEngine:
    return OMSEngine(bus=bus, clock=LiveClock())


def _make_resting_order(*, leg_id: str | None = None) -> Order:
    """An order acked by the venue and resting on the book — the state the
    kill switch must be able to cancel. (A fresh order is PENDING_NEW, which
    cannot transition straight to PENDING_CANCEL.)"""
    o = _make_order(leg_id=leg_id)
    o.transition_to(OrderStatus.ACKNOWLEDGED, at_ns=0)
    return o


@pytest.mark.asyncio
async def test_kill_switch_cancels_all_resting_orders() -> None:
    from trading.core.events import CancelRequest, KillSwitchEvent

    bus = _CapturingBus()
    oms = _oms_with_bus(bus)

    live_a = _make_resting_order()
    live_b = _make_resting_order()
    done = _make_order(terminal=True)
    for o in (live_a, live_b, done):
        oms._orders[o.order_id] = o

    await oms._on_alert(
        KillSwitchEvent(
            ts_event=0, ts_ingest=0, source="risk_engine",
            triggered_by="daily_loss_limit", reason="loss > limit",
        )
    )

    cancels = [e for _, e in bus.published if isinstance(e, CancelRequest)]
    cancelled_ids = {c.order_id for c in cancels}
    assert cancelled_ids == {live_a.order_id, live_b.order_id}
    assert live_a.status == OrderStatus.PENDING_CANCEL
    assert live_b.status == OrderStatus.PENDING_CANCEL
    # The terminal order is left untouched.
    assert done.status == OrderStatus.FILLED


@pytest.mark.asyncio
async def test_kill_switch_cancels_algo_children_and_retires_algos() -> None:
    from trading.core.events import CancelRequest, KillSwitchEvent
    from trading.oms.execution_algos import TWAPAlgo

    bus = _CapturingBus()
    oms = _oms_with_bus(bus)

    algo = TWAPAlgo(quantity=Decimal("1"), duration_seconds=60, num_slices=5, start_ns=0)
    oms._algos["leg-1"] = algo
    oms._algo_ctx["leg-1"] = (None, None)
    child = _make_resting_order(leg_id="leg-1")
    oms._orders[child.order_id] = child

    await oms._on_alert(
        KillSwitchEvent(
            ts_event=0, ts_ingest=0, source="risk_engine",
            triggered_by="vpin_circuit_breaker", reason="toxic flow",
        )
    )

    cancels = [e for _, e in bus.published if isinstance(e, CancelRequest)]
    assert {c.order_id for c in cancels} == {child.order_id}
    assert child.status == OrderStatus.PENDING_CANCEL
    assert "leg-1" not in oms._algos
    assert algo.is_done()


@pytest.mark.asyncio
async def test_on_alert_ignores_non_kill_switch_events() -> None:
    from trading.core.events import CancelRequest, RiskAlertEvent
    from trading.core.types import Severity

    bus = _CapturingBus()
    oms = _oms_with_bus(bus)
    live = _make_order()
    oms._orders[live.order_id] = live

    await oms._on_alert(
        RiskAlertEvent(
            ts_event=0, ts_ingest=0, source="risk_engine",
            rule_name="drawdown", severity=Severity.WARN, message="soft warn",
        )
    )

    assert not [e for _, e in bus.published if isinstance(e, CancelRequest)]
    assert live.status != OrderStatus.PENDING_CANCEL
