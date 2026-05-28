"""Unit tests: AvellanedaStoikov calculator + quote_filters."""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.analytics.avellaneda_stoikov import AvellanedaStoikov
from trading.analytics.quote_filters import (
    passes_min_notional,
    post_only_guard,
    round_to_lot,
    round_to_tick,
)
from trading.core.types import Side


# --- AvellanedaStoikov ----------------------------------------------------


def test_as_zero_inventory_reservation_equals_mid() -> None:
    calc = AvellanedaStoikov(gamma=0.1, k=1.5, tau_seconds=300.0)
    q = calc.quotes(mid=100.0, inventory=0.0, sigma=0.02)
    assert q.reservation == pytest.approx(100.0)


def test_as_positive_inventory_reservation_below_mid() -> None:
    """Long position → reservation < mid (skewed to encourage selling)."""
    calc = AvellanedaStoikov(gamma=0.1, k=1.5, tau_seconds=300.0)
    q = calc.quotes(mid=100.0, inventory=1.0, sigma=0.02)
    assert q.reservation < 100.0


def test_as_negative_inventory_reservation_above_mid() -> None:
    """Short position → reservation > mid (skewed to encourage buying)."""
    calc = AvellanedaStoikov(gamma=0.1, k=1.5, tau_seconds=300.0)
    q = calc.quotes(mid=100.0, inventory=-1.0, sigma=0.02)
    assert q.reservation > 100.0


def test_as_half_spread_monotone_in_sigma() -> None:
    """Half-spread increases with vol."""
    calc = AvellanedaStoikov(gamma=0.1, k=1.5, tau_seconds=300.0)
    spreads = [
        calc.quotes(mid=100.0, inventory=0.0, sigma=s).half_spread
        for s in [0.01, 0.02, 0.05, 0.10]
    ]
    assert spreads == sorted(spreads)


def test_as_bid_below_ask() -> None:
    calc = AvellanedaStoikov(gamma=0.1, k=1.5, tau_seconds=300.0)
    q = calc.quotes(mid=100.0, inventory=0.0, sigma=0.02)
    assert q.bid < q.ask


def test_as_bid_ask_symmetric_around_reservation() -> None:
    calc = AvellanedaStoikov(gamma=0.1, k=1.5, tau_seconds=300.0)
    q = calc.quotes(mid=100.0, inventory=0.0, sigma=0.02)
    assert q.ask - q.reservation == pytest.approx(q.reservation - q.bid)


def test_as_invalid_params_raise() -> None:
    with pytest.raises(ValueError):
        AvellanedaStoikov(gamma=0.0, k=1.5, tau_seconds=300.0)
    with pytest.raises(ValueError):
        AvellanedaStoikov(gamma=0.1, k=0.0, tau_seconds=300.0)
    with pytest.raises(ValueError):
        AvellanedaStoikov(gamma=0.1, k=1.5, tau_seconds=0.0)


# --- quote_filters --------------------------------------------------------


def test_round_to_tick_exact() -> None:
    assert round_to_tick(Decimal("100.005"), Decimal("0.01")) == Decimal("100.01")


def test_round_to_tick_already_on_tick() -> None:
    assert round_to_tick(Decimal("100.02"), Decimal("0.01")) == Decimal("100.02")


def test_round_to_lot_truncates() -> None:
    # 0.00137 → 0.001 (lot_size=0.001)
    assert round_to_lot(Decimal("0.00137"), Decimal("0.001")) == Decimal("0.001")


def test_round_to_lot_never_overshoots() -> None:
    result = round_to_lot(Decimal("0.00999"), Decimal("0.001"))
    assert result <= Decimal("0.00999")


def test_passes_min_notional_true() -> None:
    assert passes_min_notional(
        price=Decimal("50000"), qty=Decimal("0.001"), min_notional=Decimal("10")
    )


def test_passes_min_notional_false() -> None:
    assert not passes_min_notional(
        price=Decimal("1"), qty=Decimal("0.001"), min_notional=Decimal("10")
    )


def test_post_only_guard_buy_below_ask() -> None:
    assert post_only_guard(
        side=Side.BUY,
        our_price=Decimal("100.00"),
        best_bid=Decimal("99.99"),
        best_ask=Decimal("100.01"),
    )


def test_post_only_guard_buy_at_ask_rejected() -> None:
    assert not post_only_guard(
        side=Side.BUY,
        our_price=Decimal("100.01"),
        best_bid=Decimal("99.99"),
        best_ask=Decimal("100.01"),
    )


def test_post_only_guard_sell_above_bid() -> None:
    assert post_only_guard(
        side=Side.SELL,
        our_price=Decimal("100.02"),
        best_bid=Decimal("100.00"),
        best_ask=Decimal("100.05"),
    )


def test_post_only_guard_sell_at_bid_rejected() -> None:
    assert not post_only_guard(
        side=Side.SELL,
        our_price=Decimal("100.00"),
        best_bid=Decimal("100.00"),
        best_ask=Decimal("100.05"),
    )
