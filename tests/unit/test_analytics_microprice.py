"""Unit tests: Microprice."""

from __future__ import annotations

import pytest

from trading.analytics.microprice import Microprice


def test_symmetric_book_equals_mid() -> None:
    mp = Microprice()
    result = mp.update(bid=100.0, bid_size=10.0, ask=102.0, ask_size=10.0)
    assert result == pytest.approx(101.0)


def test_imbalanced_book_leans_toward_bid_when_ask_heavy() -> None:
    mp = Microprice()
    # ask_size >> bid_size → large ask queue (many sellers) → price leans toward bid
    result = mp.update(bid=100.0, bid_size=1.0, ask=102.0, ask_size=100.0)
    assert result is not None
    assert result < 101.0  # below arithmetic mid (sellers dominate)


def test_imbalanced_book_leans_toward_ask_when_bid_heavy() -> None:
    mp = Microprice()
    # bid_size >> ask_size → large bid queue (many buyers) → price leans toward ask
    result = mp.update(bid=100.0, bid_size=100.0, ask=102.0, ask_size=1.0)
    assert result is not None
    assert result > 101.0  # above arithmetic mid (buyers dominate)


def test_zero_total_size_returns_last() -> None:
    mp = Microprice()
    # First update gives None (last is None), then we feed a zero-size update
    first = mp.update(bid=100.0, bid_size=5.0, ask=102.0, ask_size=5.0)
    second = mp.update(bid=100.0, bid_size=0.0, ask=102.0, ask_size=0.0)
    assert second == first  # returns cached last


def test_value_property_before_update() -> None:
    mp = Microprice()
    assert mp.value is None


def test_serialize_restore_roundtrip() -> None:
    mp = Microprice()
    mp.update(bid=50000.0, bid_size=1.0, ask=50002.0, ask_size=3.0)
    state = mp.serialize()

    mp2 = Microprice()
    mp2.restore(state)
    assert mp2.value == mp.value


def test_extreme_imbalance_139_to_1() -> None:
    """Testnet observation: 139:1 ask/bid asymmetry. Heavy ask → microprice leans toward bid."""
    mp = Microprice()
    result = mp.update(bid=50000.0, bid_size=1.0, ask=50001.0, ask_size=139.0)
    arithmetic_mid = (50000.0 + 50001.0) / 2
    assert result is not None
    # 139:1 ask/bid → microprice strongly below arithmetic mid (sellers dominate)
    assert result < arithmetic_mid
    assert result < arithmetic_mid - 0.4  # at least 0.4 below mid out of 0.5 range
