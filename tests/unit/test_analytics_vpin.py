"""Unit tests: VPIN + classifiers."""

from __future__ import annotations

import pytest

from trading.analytics.classifiers import BVCClassifier, TickRuleClassifier
from trading.analytics.vpin import VPIN


# --- TickRuleClassifier ---------------------------------------------------


def test_tick_rule_uptick_is_buy() -> None:
    clf = TickRuleClassifier()
    clf.classify(price=100.0, volume=1.0)  # init
    buy, sell = clf.classify(price=101.0, volume=2.0)
    assert buy == pytest.approx(2.0)
    assert sell == pytest.approx(0.0)


def test_tick_rule_downtick_is_sell() -> None:
    clf = TickRuleClassifier()
    clf.classify(price=100.0, volume=1.0)
    buy, sell = clf.classify(price=99.0, volume=2.0)
    assert buy == pytest.approx(0.0)
    assert sell == pytest.approx(2.0)


def test_tick_rule_unchanged_inherits() -> None:
    clf = TickRuleClassifier()
    clf.classify(price=100.0, volume=1.0)
    clf.classify(price=101.0, volume=1.0)  # uptick → buy
    buy, sell = clf.classify(price=101.0, volume=3.0)  # unchanged → buy
    assert buy == pytest.approx(3.0)


# --- BVCClassifier --------------------------------------------------------


def test_bvc_first_call_neutral_split() -> None:
    clf = BVCClassifier()
    buy, sell = clf.classify(price=100.0, volume=2.0)
    assert buy == pytest.approx(1.0)
    assert sell == pytest.approx(1.0)


def test_bvc_buy_sell_sum_equals_volume() -> None:
    clf = BVCClassifier()
    clf.classify(price=100.0, volume=1.0)
    for price in [100.5, 99.5, 101.0, 98.0]:
        buy, sell = clf.classify(price=price, volume=5.0)
        assert buy + sell == pytest.approx(5.0)


# --- VPIN -----------------------------------------------------------------


def test_vpin_balanced_flow_near_zero() -> None:
    """Equal buy/sell → VPIN near 0."""
    vpin = VPIN(bucket_volume=10.0, rolling_buckets=5, classifier=TickRuleClassifier())
    # Alternate buy/sell trades, each 1 unit, 100 trades
    price = 100.0
    for i in range(100):
        price = 100.0 + (1 if i % 2 == 0 else -1) * 0.01
        vpin.update(price=price, volume=1.0)
    assert vpin.value is not None
    assert vpin.value < 0.3


def test_vpin_one_sided_flow_approaches_one() -> None:
    """All buy-side trades → VPIN should approach 1."""
    vpin = VPIN(bucket_volume=10.0, rolling_buckets=5, classifier=TickRuleClassifier())
    price = 100.0
    for i in range(200):
        price += 0.01
        vpin.update(price=price, volume=1.0)
    assert vpin.value is not None
    assert vpin.value > 0.7


def test_vpin_below_one() -> None:
    vpin = VPIN(bucket_volume=10.0, rolling_buckets=10, classifier=TickRuleClassifier())
    price = 100.0
    for i in range(500):
        price += 0.01
        vpin.update(price=price, volume=1.0)
    assert vpin.value is not None
    assert vpin.value <= 1.0


def test_vpin_serialize_restore() -> None:
    vpin = VPIN(bucket_volume=10.0, rolling_buckets=5, classifier=TickRuleClassifier())
    for i in range(50):
        vpin.update(price=100.0 + i * 0.01, volume=1.0)
    state = vpin.serialize()

    vpin2 = VPIN(bucket_volume=10.0, rolling_buckets=5, classifier=TickRuleClassifier())
    vpin2.restore(state)
    assert vpin2.value == vpin.value
