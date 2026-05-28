"""Unit tests: AvellanedaStoikovStrategy signal output.

Tests strategy logic in isolation — no bus, no risk, no OMS.
Directly calls on_tick() and asserts signal structure.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
import structlog

from trading.core import AssetType, Instrument, SimulatedClock, StrategyId
from trading.core.events import TickEvent
from trading.core.positions import Position
from trading.core.types import OrderType, Side, TimeInForce
from trading.strategy.context import StaticPortfolioView, StrategyContext
from trading.strategy.examples.avellaneda_stoikov import AvellanedaStoikovStrategy

_T0 = 1_700_000_000_000_000_000


@pytest.fixture
def inst() -> Instrument:
    return Instrument(
        symbol="BTC-USDT",
        exchange="BINANCE",
        asset_type=AssetType.FUTURES,
        base_currency="BTC",
        quote_currency="USDT",
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.00001"),
        min_notional=Decimal("10"),
    )


@pytest.fixture
def strategy(inst) -> AvellanedaStoikovStrategy:
    return AvellanedaStoikovStrategy(
        strategy_id=StrategyId("as-test"),
        instruments=[inst],
        gamma=0.1,
        k=1.5,
        tau_seconds=300.0,
        half_life_seconds=10.0,
        ofi_window_seconds=5.0,
        quote_size=Decimal("0.00001"),
        max_position=Decimal("0.001"),
        min_vol=0.01,
    )


@pytest.fixture
def clock() -> SimulatedClock:
    return SimulatedClock(start=_T0)


def _ctx(inst: Instrument, clock: SimulatedClock,
         portfolio: StaticPortfolioView | None = None) -> StrategyContext:
    return StrategyContext(
        strategy_id=StrategyId("as-test"),
        clock=clock,
        portfolio=portfolio or StaticPortfolioView(),
        logger=structlog.get_logger("test"),
        parameters={},
    )


def _tick(inst: Instrument, ts_ns: int,
          bid: str = "50000.00", bid_size: str = "1.0",
          ask: str = "50001.00", ask_size: str = "1.0") -> TickEvent:
    return TickEvent(
        ts_event=ts_ns,
        ts_ingest=ts_ns,
        source="test",
        instrument=inst,
        bid_price=Decimal(bid),
        bid_size=Decimal(bid_size),
        ask_price=Decimal(ask),
        ask_size=Decimal(ask_size),
    )


async def _feed(strategy, inst, clock, n: int, bid="50000.00", ask="50001.00"):
    """Feed n ticks 1s apart with price moving 0.20/tick to trigger throttle."""
    ctx = _ctx(inst, clock)
    signals = []
    base_bid = Decimal(bid)
    base_ask = Decimal(ask)
    tick_move = Decimal("0.20")  # 2 ticks per step — always triggers min_price_move_ticks=1
    for i in range(n):
        ts = _T0 + i * 1_000_000_000
        b = str(base_bid + tick_move * i)
        a = str(base_ask + tick_move * i)
        signals = await strategy.on_tick(_tick(inst, ts, bid=b, ask=a), ctx)
    return signals


def _legs(signals):
    """Extract legs from a single-signal response."""
    assert len(signals) == 1
    return signals[0].legs


async def test_first_tick_emits_signals_with_vol_floor(strategy, inst, clock) -> None:
    """First tick: microprice works immediately, vol=None → min_vol floor → signals emitted."""
    ctx = _ctx(inst, clock)
    signals = await strategy.on_tick(_tick(inst, _T0), ctx)
    assert len(signals) == 1
    assert len(signals[0].legs) == 2


async def test_signals_emitted_after_warmup(strategy, inst, clock) -> None:
    signals = await _feed(strategy, inst, clock, 3)
    legs = _legs(signals)
    assert len(legs) == 2
    sides = {leg.side for leg in legs}
    assert Side.BUY in sides
    assert Side.SELL in sides


async def test_signals_are_post_only_gtc(strategy, inst, clock) -> None:
    signals = await _feed(strategy, inst, clock, 3)
    for leg in _legs(signals):
        assert leg.order_type == OrderType.POST_ONLY
        assert leg.time_in_force == TimeInForce.GTC


async def test_buy_signal_below_ask(strategy, inst, clock) -> None:
    signals = await _feed(strategy, inst, clock, 3, ask="50001.00")
    legs = _legs(signals)
    buy = [leg for leg in legs if leg.side == Side.BUY]
    assert buy
    assert buy[0].price < Decimal("50001.00")


async def test_sell_signal_above_bid(strategy, inst, clock) -> None:
    # Last tick bid is 50000.00 + 0.40 = 50000.40; sell must be above that
    signals = await _feed(strategy, inst, clock, 3, bid="50000.00")
    legs = _legs(signals)
    sell = [leg for leg in legs if leg.side == Side.SELL]
    assert sell
    assert sell[0].price > Decimal("50000.00")


async def test_bid_below_ask_in_signals(strategy, inst, clock) -> None:
    signals = await _feed(strategy, inst, clock, 3)
    legs = _legs(signals)
    buy_price = next(leg.price for leg in legs if leg.side == Side.BUY)
    sell_price = next(leg.price for leg in legs if leg.side == Side.SELL)
    assert buy_price < sell_price


async def test_quote_size_matches_config(strategy, inst, clock) -> None:
    signals = await _feed(strategy, inst, clock, 3)
    for leg in _legs(signals):
        assert leg.quantity == Decimal("0.00001")


async def test_prices_on_tick_grid(strategy, inst, clock) -> None:
    signals = await _feed(strategy, inst, clock, 5)
    for leg in _legs(signals):
        remainder = leg.price % Decimal("0.01")
        assert remainder == Decimal("0"), f"price {leg.price} not on tick grid"


async def test_serialize_restore_shape_preserved(strategy, inst, clock) -> None:
    """State restored from serialize() produces same-shaped signals."""
    await _feed(strategy, inst, clock, 5)
    state = strategy.serialize_state()

    strategy2 = AvellanedaStoikovStrategy(
        strategy_id=StrategyId("as-test"),
        instruments=[inst],
        gamma=0.1, k=1.5, tau_seconds=300.0,
        half_life_seconds=10.0, ofi_window_seconds=5.0,
        quote_size=Decimal("0.00001"), max_position=Decimal("0.001"),
        min_vol=0.01,
    )
    strategy2.restore_state(state)

    clock2 = SimulatedClock(start=_T0)
    ctx2 = _ctx(inst, clock2)
    # Use a different price so throttle fires (strategy2 has _last_bid=None after restore)
    ts = _T0 + 5 * 1_000_000_000
    t = _tick(inst, ts, bid="50002.00", ask="50003.00")
    sigs1 = await strategy.on_tick(t, _ctx(inst, clock))
    sigs2 = await strategy2.on_tick(t, ctx2)

    assert len(sigs2) == len(sigs1)
    if sigs1 and sigs2:
        legs1 = sigs1[0].legs
        legs2 = sigs2[0].legs
        assert len(legs2) == len(legs1)
        for l1, l2 in zip(legs1, legs2):
            assert l1.side == l2.side


async def test_max_position_suppresses_buy_side(inst, clock) -> None:
    """At max long inventory, no BUY signal emitted."""
    strategy = AvellanedaStoikovStrategy(
        strategy_id=StrategyId("as-test"),
        instruments=[inst],
        gamma=0.1, k=1.5, tau_seconds=300.0,
        half_life_seconds=10.0, ofi_window_seconds=5.0,
        quote_size=Decimal("0.00001"), max_position=Decimal("0.001"),
        min_vol=0.01,
    )
    portfolio = StaticPortfolioView()
    portfolio.set_position(Position(
        strategy_id=StrategyId("as-test"),
        instrument=inst,
        quantity=Decimal("0.001"),  # at max_position
        average_entry_price=Decimal("50000"),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
    ))
    ctx = _ctx(inst, clock, portfolio)
    signals = []
    for i in range(3):
        ts = _T0 + i * 1_000_000_000
        # Vary price each tick so throttle doesn't suppress signals
        b = str(Decimal("50000.00") + Decimal("0.20") * i)
        a = str(Decimal("50001.00") + Decimal("0.20") * i)
        signals = await strategy.on_tick(_tick(inst, ts, bid=b, ask=a), ctx)

    # Should have signal with only SELL leg (no BUY at max position)
    if signals:
        sides = {leg.side for leg in signals[0].legs}
        assert Side.BUY not in sides
