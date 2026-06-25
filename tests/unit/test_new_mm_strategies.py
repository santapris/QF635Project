"""Unit tests: GLFT / OBI-alpha / grid / microprice MM strategy signals.

Tests each strategy in isolation — no bus, no risk, no OMS. Directly calls
on_tick() and asserts signal structure, mirroring test_as_strategy_signals.py.
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
from trading.strategy.examples.glft import GLFTStrategy
from trading.strategy.examples.grid import GridStrategy
from trading.strategy.examples.microprice_mm import MicropriceMMStrategy
from trading.strategy.examples.obi_alpha import OBIAlphaStrategy

_T0 = 1_700_000_000_000_000_000
_SID = StrategyId("mm-test")


@pytest.fixture
def inst() -> Instrument:
    return Instrument(
        symbol="BTC-USDT", exchange="BINANCE", asset_type=AssetType.FUTURES,
        base_currency="BTC", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.00001"),
        min_notional=Decimal("10"),
    )


@pytest.fixture
def clock() -> SimulatedClock:
    return SimulatedClock(start=_T0)


def _ctx(clock: SimulatedClock,
         portfolio: StaticPortfolioView | None = None) -> StrategyContext:
    return StrategyContext(
        strategy_id=_SID, clock=clock,
        portfolio=portfolio or StaticPortfolioView(),
        logger=structlog.get_logger("test"), parameters={},
    )


def _tick(inst: Instrument, ts_ns: int,
          bid: str = "50000.00", bid_size: str = "1.0",
          ask: str = "50001.00", ask_size: str = "1.0") -> TickEvent:
    return TickEvent(
        ts_event=ts_ns, ts_ingest=ts_ns, source="test", instrument=inst,
        bid_price=Decimal(bid), bid_size=Decimal(bid_size),
        ask_price=Decimal(ask), ask_size=Decimal(ask_size),
    )


def _long_position(inst: Instrument, qty: str) -> StaticPortfolioView:
    pv = StaticPortfolioView()
    pv.set_position(Position(
        strategy_id=_SID, instrument=inst, quantity=Decimal(qty),
        average_entry_price=Decimal("50000"),
        realized_pnl=Decimal("0"), unrealized_pnl=Decimal("0"),
    ))
    return pv


def _legs(signals):
    assert len(signals) == 1
    return signals[0].legs


# --- shared structural checks across all four strategies --------------------

def _make(strategy_type, inst):
    if strategy_type is GLFTStrategy:
        return GLFTStrategy(
            strategy_id=_SID, instruments=[inst],
            quote_size=Decimal("0.00001"), max_position=Decimal("0.001"),
            min_vol=0.5,
        )
    if strategy_type is OBIAlphaStrategy:
        return OBIAlphaStrategy(
            strategy_id=_SID, instruments=[inst],
            quote_size=Decimal("0.00001"), max_position=Decimal("0.001"),
            target_spread_bps=2.0,
        )
    if strategy_type is GridStrategy:
        return GridStrategy(
            strategy_id=_SID, instruments=[inst],
            quote_size=Decimal("0.00001"), max_position=Decimal("0.001"),
            n_levels=1, grid_step_bps=2.0,
            min_quote_interval_s=0.0, requote_threshold_bps=0.0,
        )
    return MicropriceMMStrategy(
        strategy_id=_SID, instruments=[inst],
        quote_size=Decimal("0.00001"), max_position=Decimal("0.001"),
        target_spread_bps=2.0,
        min_quote_interval_s=0.0, requote_threshold_bps=0.0,
    )


ALL = [GLFTStrategy, OBIAlphaStrategy, GridStrategy, MicropriceMMStrategy]


@pytest.mark.parametrize("stype", ALL)
async def test_emits_two_sided_quote(stype, inst, clock) -> None:
    strat = _make(stype, inst)
    legs = _legs(await strat.on_tick(_tick(inst, _T0), _ctx(clock)))
    assert len(legs) == 2
    assert {leg.side for leg in legs} == {Side.BUY, Side.SELL}


@pytest.mark.parametrize("stype", ALL)
async def test_legs_post_only_gtc(stype, inst, clock) -> None:
    strat = _make(stype, inst)
    for leg in _legs(await strat.on_tick(_tick(inst, _T0), _ctx(clock))):
        assert leg.order_type == OrderType.POST_ONLY
        assert leg.time_in_force == TimeInForce.GTC


@pytest.mark.parametrize("stype", ALL)
async def test_bid_below_ask(stype, inst, clock) -> None:
    strat = _make(stype, inst)
    legs = _legs(await strat.on_tick(_tick(inst, _T0), _ctx(clock)))
    buy = next(leg.price for leg in legs if leg.side == Side.BUY)
    sell = next(leg.price for leg in legs if leg.side == Side.SELL)
    assert buy < sell


@pytest.mark.parametrize("stype", ALL)
async def test_prices_on_tick_grid(stype, inst, clock) -> None:
    strat = _make(stype, inst)
    for leg in _legs(await strat.on_tick(_tick(inst, _T0), _ctx(clock))):
        assert leg.price % Decimal("0.01") == Decimal("0")


@pytest.mark.parametrize("stype", ALL)
async def test_quote_size_matches(stype, inst, clock) -> None:
    strat = _make(stype, inst)
    for leg in _legs(await strat.on_tick(_tick(inst, _T0), _ctx(clock))):
        assert leg.quantity == Decimal("0.00001")


@pytest.mark.parametrize("stype", ALL)
async def test_max_long_suppresses_buy(stype, inst, clock) -> None:
    strat = _make(stype, inst)
    ctx = _ctx(clock, _long_position(inst, "0.001"))  # at max_position
    signals = await strat.on_tick(_tick(inst, _T0), ctx)
    if signals:
        sides = {leg.side for leg in signals[0].legs}
        assert Side.BUY not in sides


@pytest.mark.parametrize("stype", ALL)
async def test_max_short_suppresses_sell(stype, inst, clock) -> None:
    strat = _make(stype, inst)
    ctx = _ctx(clock, _long_position(inst, "-0.001"))  # at max short
    signals = await strat.on_tick(_tick(inst, _T0), ctx)
    if signals:
        sides = {leg.side for leg in signals[0].legs}
        assert Side.SELL not in sides


# --- grid-specific ----------------------------------------------------------

async def test_grid_emits_ladder(inst, clock) -> None:
    strat = GridStrategy(
        strategy_id=_SID, instruments=[inst],
        quote_size=Decimal("0.00001"), max_position=Decimal("1.0"),
        n_levels=3, grid_step_bps=5.0,
        min_quote_interval_s=0.0, requote_threshold_bps=0.0,
    )
    legs = _legs(await strat.on_tick(_tick(inst, _T0), _ctx(clock)))
    buys = sorted((leg.price for leg in legs if leg.side == Side.BUY), reverse=True)
    sells = sorted(leg.price for leg in legs if leg.side == Side.SELL)
    assert len(buys) == 3
    assert len(sells) == 3
    # Levels step monotonically outward from the center.
    assert buys[0] > buys[1] > buys[2]
    assert sells[0] < sells[1] < sells[2]


async def test_grid_spacing_matches_step(inst, clock) -> None:
    step_bps = 5.0
    strat = GridStrategy(
        strategy_id=_SID, instruments=[inst],
        quote_size=Decimal("0.00001"), max_position=Decimal("1.0"),
        n_levels=2, grid_step_bps=step_bps,
        min_quote_interval_s=0.0, requote_threshold_bps=0.0,
    )
    legs = _legs(await strat.on_tick(_tick(inst, _T0), _ctx(clock)))
    buys = sorted((leg.price for leg in legs if leg.side == Side.BUY), reverse=True)
    center = Decimal("50000.50")  # mid of 50000.00 / 50001.00
    expected_step = inst.round_price(center * Decimal(str(step_bps / 10_000.0)))
    assert (buys[0] - buys[1]) == expected_step


# --- obi-alpha-specific -----------------------------------------------------

async def test_obi_alpha_tilts_fair_value_up_on_positive_imbalance(inst, clock) -> None:
    """With a positive alpha and bid-heavy book (OBI>0), quotes shift up."""
    tick = _tick(inst, _T0, bid="50000.00", bid_size="9.0",
                 ask="50001.00", ask_size="1.0")
    base = OBIAlphaStrategy(
        strategy_id=_SID, instruments=[inst],
        quote_size=Decimal("0.00001"), max_position=Decimal("1.0"),
        target_spread_bps=2.0, obi_alpha=0.0,
    )
    tilted = OBIAlphaStrategy(
        strategy_id=_SID, instruments=[inst],
        quote_size=Decimal("0.00001"), max_position=Decimal("1.0"),
        target_spread_bps=2.0, obi_alpha=50.0,
    )
    base_ask = next(leg.price for leg in _legs(await base.on_tick(tick, _ctx(clock)))
                    if leg.side == Side.SELL)
    tilted_ask = next(leg.price for leg in _legs(await tilted.on_tick(tick, _ctx(clock)))
                      if leg.side == Side.SELL)
    assert tilted_ask > base_ask


# --- glft-specific ----------------------------------------------------------

async def test_glft_grid_variant_emits_multiple_levels(inst, clock) -> None:
    strat = GLFTStrategy(
        strategy_id=_SID, instruments=[inst],
        quote_size=Decimal("0.00001"), max_position=Decimal("1.0"),
        min_vol=0.5, n_levels=3, grid_step_bps=5.0,
    )
    legs = _legs(await strat.on_tick(_tick(inst, _T0), _ctx(clock)))
    assert sum(1 for leg in legs if leg.side == Side.BUY) == 3
    assert sum(1 for leg in legs if leg.side == Side.SELL) == 3


async def test_glft_diagnostics_conform_to_event_schema(inst, clock) -> None:
    """Diagnostics must construct a StrategyDiagnosticsEvent (strict schema)."""
    from trading.core.events import StrategyDiagnosticsEvent

    strat = _make(GLFTStrategy, inst)
    await strat.on_tick(_tick(inst, _T0), _ctx(clock))
    diag = strat.get_strategy_diagnostics()
    assert diag is not None
    # Raises if a field is missing or an extra key is present (extra="forbid").
    event = StrategyDiagnosticsEvent(**diag)
    assert event.reservation == diag["reservation"]
    assert event.vpin_widened is False
