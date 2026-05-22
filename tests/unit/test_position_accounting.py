"""Unit tests for position accounting (WAVG/FIFO/LIFO)."""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from trading.core import (
    ClientOrderId,
    ExchangeOrderId,
    FillEvent,
    OrderId,
    Side,
)
from trading.position import FIFOBook, LIFOBook, WAVGBook


def _fill(clock, btc, strategy_id, side, qty, price, fee="0"):
    return FillEvent(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source="test",
        order_id=OrderId(uuid4()),
        client_order_id=ClientOrderId(str(uuid4())[:8]),
        exchange_order_id=ExchangeOrderId(str(uuid4())[:8]),
        strategy_id=strategy_id,
        instrument=btc,
        side=side,
        fill_price=Decimal(price),
        fill_quantity=Decimal(qty),
        cumulative_quantity=Decimal(qty),
        leaves_quantity=Decimal(0),
        fee=Decimal(fee),
    )


# --- WAVG ------------------------------------------------------------------

def test_wavg_open_then_extend(clock, btc, strategy_id) -> None:
    b = WAVGBook()
    b.apply_fill(_fill(clock, btc, strategy_id, Side.BUY, "5", "100"))
    b.apply_fill(_fill(clock, btc, strategy_id, Side.BUY, "3", "110"))
    assert b.quantity == Decimal("8")
    assert b.average_entry_price == Decimal("103.75")


def test_wavg_partial_close(clock, btc, strategy_id) -> None:
    b = WAVGBook()
    b.apply_fill(_fill(clock, btc, strategy_id, Side.BUY, "5", "100"))
    b.apply_fill(_fill(clock, btc, strategy_id, Side.SELL, "2", "110"))
    assert b.quantity == Decimal("3")
    assert b.realized_pnl == Decimal("20")  # 2 * (110 - 100)


def test_wavg_flip(clock, btc, strategy_id) -> None:
    b = WAVGBook()
    b.apply_fill(_fill(clock, btc, strategy_id, Side.BUY, "5", "100"))
    b.apply_fill(_fill(clock, btc, strategy_id, Side.SELL, "7", "110"))
    assert b.quantity == Decimal("-2")
    assert b.average_entry_price == Decimal("110")
    assert b.realized_pnl == Decimal("50")


def test_wavg_fees_reduce_realized(clock, btc, strategy_id) -> None:
    b = WAVGBook()
    b.apply_fill(_fill(clock, btc, strategy_id, Side.BUY, "5", "100", fee="1"))
    b.apply_fill(_fill(clock, btc, strategy_id, Side.SELL, "5", "110", fee="2"))
    # gross 50, fees 3, net 47
    assert b.realized_pnl == Decimal("47")


# --- FIFO ------------------------------------------------------------------

def test_fifo_consumes_oldest_first(clock, btc, strategy_id) -> None:
    b = FIFOBook()
    b.apply_fill(_fill(clock, btc, strategy_id, Side.BUY, "5", "100"))
    b.apply_fill(_fill(clock, btc, strategy_id, Side.BUY, "3", "110"))
    b.apply_fill(_fill(clock, btc, strategy_id, Side.SELL, "4", "120"))
    # FIFO closes 4 from the (5, 100) lot -> 4 * (120 - 100) = 80
    assert b.realized_pnl == Decimal("80")


# --- LIFO ------------------------------------------------------------------

def test_lifo_consumes_newest_first(clock, btc, strategy_id) -> None:
    b = LIFOBook()
    b.apply_fill(_fill(clock, btc, strategy_id, Side.BUY, "5", "100"))
    b.apply_fill(_fill(clock, btc, strategy_id, Side.BUY, "3", "110"))
    b.apply_fill(_fill(clock, btc, strategy_id, Side.SELL, "4", "120"))
    # LIFO closes 3 from (3, 110) + 1 from (5, 100) -> 30 + 20 = 50
    assert b.realized_pnl == Decimal("50")


def test_fifo_lifo_have_same_total_pnl(clock, btc, strategy_id) -> None:
    """The accounting choice changes timing of recognition, not total PnL."""
    fifo, lifo = FIFOBook(), LIFOBook()
    fills = [
        _fill(clock, btc, strategy_id, Side.BUY, "5", "100"),
        _fill(clock, btc, strategy_id, Side.BUY, "3", "110"),
        _fill(clock, btc, strategy_id, Side.SELL, "4", "120"),
    ]
    for f in fills:
        fifo.apply_fill(f)
        lifo.apply_fill(f)
    mark = Decimal("120")
    assert fifo.realized_pnl + fifo.unrealized_pnl(mark) == \
           lifo.realized_pnl + lifo.unrealized_pnl(mark) == Decimal("130")
