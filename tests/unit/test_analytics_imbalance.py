"""Unit tests: OBI and OFI."""

from __future__ import annotations

import pytest

from trading.core.clock import SimulatedClock
from trading.analytics.imbalance import OBI, OFI

_T0 = 1_700_000_000_000_000_000  # nanosecond epoch anchor


def _clock(start: int = _T0) -> SimulatedClock:
    return SimulatedClock(start=start)


# --- OBI -------------------------------------------------------------------


def test_obi_symmetric_book_zero() -> None:
    obi = OBI()
    result = obi.update(bid_size=10.0, ask_size=10.0)
    assert result == pytest.approx(0.0)


def test_obi_full_bid_side() -> None:
    obi = OBI()
    result = obi.update(bid_size=10.0, ask_size=0.0)
    assert result == pytest.approx(1.0)


def test_obi_full_ask_side() -> None:
    obi = OBI()
    result = obi.update(bid_size=0.0, ask_size=10.0)
    assert result == pytest.approx(-1.0)


def test_obi_zero_total_returns_last() -> None:
    obi = OBI()
    obi.update(bid_size=5.0, ask_size=5.0)
    result = obi.update(bid_size=0.0, ask_size=0.0)
    assert result == pytest.approx(0.0)


def test_obi_range_bounded() -> None:
    obi = OBI()
    for bid_s, ask_s in [(100.0, 1.0), (1.0, 100.0), (50.0, 50.0)]:
        v = obi.update(bid_size=bid_s, ask_size=ask_s)
        assert v is not None
        assert -1.0 <= v <= 1.0


def test_obi_serialize_restore() -> None:
    obi = OBI()
    obi.update(bid_size=3.0, ask_size=7.0)
    state = obi.serialize()
    obi2 = OBI()
    obi2.restore(state)
    assert obi2.value == obi.value


# --- OFI -------------------------------------------------------------------


def _ns(seconds: float, base: int = _T0) -> int:
    return base + int(seconds * 1_000_000_000)


def test_ofi_no_book_change_returns_zero() -> None:
    clock = _clock()
    ofi = OFI(window_seconds=10.0, clock=clock)
    ts = _T0
    # First call establishes prev state, no delta yet
    r1 = ofi.update(bid=100.0, bid_size=5.0, ask=101.0, ask_size=5.0, ts_ns=ts)
    assert r1 is None  # no events yet (first call just initializes)
    # Second call with identical prices → delta = 0
    ts2 = _ns(1.0)
    r2 = ofi.update(bid=100.0, bid_size=5.0, ask=101.0, ask_size=5.0, ts_ns=ts2)
    assert r2 == pytest.approx(0.0)


def test_ofi_bid_price_rise_positive() -> None:
    clock = _clock()
    ofi = OFI(window_seconds=10.0, clock=clock)
    ofi.update(bid=100.0, bid_size=5.0, ask=101.0, ask_size=5.0, ts_ns=_T0)
    r = ofi.update(bid=100.5, bid_size=5.0, ask=101.0, ask_size=5.0, ts_ns=_ns(1.0))
    # bid rose → positive contribution
    assert r is not None and r > 0.0


def test_ofi_ask_price_rise_negative() -> None:
    clock = _clock()
    ofi = OFI(window_seconds=10.0, clock=clock)
    ofi.update(bid=100.0, bid_size=5.0, ask=101.0, ask_size=5.0, ts_ns=_T0)
    r = ofi.update(bid=100.0, bid_size=5.0, ask=101.5, ask_size=5.0, ts_ns=_ns(1.0))
    # ask rose → negative contribution
    assert r is not None and r < 0.0


def test_ofi_rolling_window_expiry() -> None:
    clock = _clock()
    ofi = OFI(window_seconds=5.0, clock=clock)
    # First call sets prev state
    ts = _T0
    ofi.update(bid=100.0, bid_size=5.0, ask=101.5, ask_size=5.0, ts_ns=ts)
    # Second call: bid rises, ask unchanged → positive OFI delta
    ts += int(1e9)
    r_before = ofi.update(bid=101.0, bid_size=5.0, ask=101.5, ask_size=5.0, ts_ns=ts)
    assert r_before is not None and r_before > 0.0

    # Jump 10 seconds → old event expires (5s window)
    ts += int(10e9)
    # No book change in this call → delta = 0, and old event expired
    r_after = ofi.update(bid=101.0, bid_size=5.0, ask=101.5, ask_size=5.0, ts_ns=ts)
    assert r_after == pytest.approx(0.0)


def test_ofi_serialize_restore_roundtrip() -> None:
    clock = _clock()
    ofi = OFI(window_seconds=10.0, clock=clock)
    ofi.update(bid=100.0, bid_size=5.0, ask=101.0, ask_size=5.0, ts_ns=_T0)
    ts2 = _ns(1.0)
    ofi.update(bid=101.0, bid_size=5.0, ask=102.0, ask_size=5.0, ts_ns=ts2)
    state = ofi.serialize()

    ofi2 = OFI(window_seconds=10.0, clock=_clock())
    ofi2.restore(state)
    assert ofi2.value == ofi.value
