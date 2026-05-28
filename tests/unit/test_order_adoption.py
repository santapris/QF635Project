"""Adoption of pre-existing venue orders (recover-mid-trade on restart).

Covers the attribution helper, OMS adopt_order (idempotency, status seeding,
strategy attribution), and that adopted EXTERNAL orders are never touched by
a strategy's own reconciliation.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.core import (
    OrderLeg,
    OrderType,
    Side,
    SignalEvent,
    TimeInForce,
)
from trading.core.types import ClientOrderId, OrderStatus, Quantity, Price, StrategyId
from trading.oms import OMSEngine
from trading.oms.engine import EXTERNAL_STRATEGY_ID, strategy_id_from_client_order_id


class _NullBus:
    async def publish(self, topic, event): pass
    async def subscribe(self, topic, handler): pass
    async def subscribe_many(self, topics, handler): pass
    async def start(self): pass
    async def stop(self): pass


# --- Attribution helper ---------------------------------------------------

def test_coid_parses_to_strategy() -> None:
    assert strategy_id_from_client_order_id("mm-0123456789ab") == StrategyId("mm")
    # Strategy ids may themselves contain hyphens.
    assert strategy_id_from_client_order_id("my-strat-abcdef012345") == StrategyId("my-strat")


def test_coid_external_fallback() -> None:
    # Human/UI order, no matching scheme.
    assert strategy_id_from_client_order_id("web_abc123") == EXTERNAL_STRATEGY_ID
    # Trailing token wrong length.
    assert strategy_id_from_client_order_id("mm-0123") == EXTERNAL_STRATEGY_ID
    # Trailing token not hex.
    assert strategy_id_from_client_order_id("mm-zzzzzzzzzzzz") == EXTERNAL_STRATEGY_ID


# --- adopt_order ----------------------------------------------------------

async def _adopt(oms, btc, coid, side=Side.BUY, qty="1", filled="0", price="50000"):
    return await oms.adopt_order(
        instrument=btc,
        client_order_id=ClientOrderId(coid),
        side=side,
        order_type=OrderType.LIMIT,
        quantity=Quantity(Decimal(qty)),
        cumulative_filled=Quantity(Decimal(filled)),
        price=Price(Decimal(price)),
        time_in_force=TimeInForce.GTC,
    )


async def test_adopt_attributes_by_coid(clock, btc) -> None:
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    oid = await _adopt(oms, btc, "mm-0123456789ab")
    order = oms.get_order(oid)
    assert order is not None
    assert order.strategy_id == StrategyId("mm")
    assert order.status == OrderStatus.ACKNOWLEDGED  # nothing filled
    assert order.parent_leg_id is None  # adopted orders aren't algo children


async def test_adopt_external_when_coid_unrecognized(clock, btc) -> None:
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    oid = await _adopt(oms, btc, "web_order_1")
    assert oms.get_order(oid).strategy_id == EXTERNAL_STRATEGY_ID


async def test_adopt_partial_fill_status(clock, btc) -> None:
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    oid = await _adopt(oms, btc, "mm-0123456789ab", qty="1", filled="0.3")
    order = oms.get_order(oid)
    assert order.status == OrderStatus.PARTIALLY_FILLED
    assert order.leaves_quantity == Decimal("0.7")


async def test_adopt_is_idempotent_on_coid(clock, btc) -> None:
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    oid1 = await _adopt(oms, btc, "mm-0123456789ab")
    oid2 = await _adopt(oms, btc, "mm-0123456789ab")
    assert oid1 == oid2
    assert len(oms._orders) == 1


async def test_adopted_order_appears_in_working_exposure(clock, btc) -> None:
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    await _adopt(oms, btc, "web_order_1", side=Side.BUY, qty="0.4")
    exposures = oms.working_exposures()
    assert len(exposures) == 1
    assert exposures[0].working_buy == Decimal("0.4")
    assert exposures[0].strategy_id == EXTERNAL_STRATEGY_ID


# --- External orders are not reconciled away by strategies ----------------

async def test_strategy_reconcile_ignores_external_orders(clock, btc) -> None:
    """An adopted EXTERNAL order must survive a different strategy's reconcile."""
    oms = OMSEngine(bus=_NullBus(), clock=clock)
    ext_oid = await _adopt(oms, btc, "web_order_1", side=Side.BUY, qty="0.4", price="49000")

    # Strategy "mm" reconciles its own (empty -> one bid) desired state.
    mm_leg = OrderLeg(
        side=Side.BUY, quantity=Decimal("0.1"), price=Decimal("50000"),
        order_type=OrderType.LIMIT, time_in_force=TimeInForce.GTC,
    )
    sig = SignalEvent(
        ts_event=clock.now_ns(), ts_ingest=clock.now_ns(), source="t",
        strategy_id=StrategyId("mm"), instrument=btc, legs=(mm_leg,),
    )
    await oms._reconcile_quotes(sig)

    # The external order is still open — a different strategy never cancels it.
    ext = oms.get_order(ext_oid)
    assert ext is not None
    assert not ext.is_terminal
    assert ext.strategy_id == EXTERNAL_STRATEGY_ID
