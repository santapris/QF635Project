"""Regression: a strategy's diagnostics-publish failure must be isolated.

The registry publishes a StrategyDiagnosticsEvent after each tick from the
dict a strategy returns. If that dict doesn't fit the (strict) event schema,
construction raises — the registry must log and swallow it, never let it
propagate out of dispatch (an earlier version called the non-existent
``structlog.exc_info()`` inside the handler, turning a benign validation
error into an AttributeError that aborted dispatch).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.core import AssetType, Instrument, SimulatedClock, StrategyId
from trading.core.events import TickEvent
from trading.strategy.base import AbstractStrategy
from trading.strategy.context import StaticPortfolioView, StrategyContext
from trading.strategy.registry import StrategyRegistry

_T0 = 1_700_000_000_000_000_000


class _CaptureBus:
    """Records published events; supports the registry's subscribe calls."""

    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event) -> None:
        self.published.append((topic, event))

    async def subscribe(self, topic, handler) -> None:
        pass

    async def subscribe_many(self, topics, handler) -> None:
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


@pytest.fixture
def inst() -> Instrument:
    return Instrument(
        symbol="BTC-USDT", exchange="BINANCE", asset_type=AssetType.SPOT,
        base_currency="BTC", quote_currency="USDT",
        tick_size=Decimal("0.01"), lot_size=Decimal("0.0001"),
    )


class _BadDiagnosticsStrategy(AbstractStrategy):
    """Returns a diagnostics dict that can't build a StrategyDiagnosticsEvent."""

    async def on_tick(self, event, ctx) -> list:
        return []

    def get_strategy_diagnostics(self) -> dict | None:
        return {"not_a_valid_field": 1}


def _tick(inst: Instrument) -> TickEvent:
    return TickEvent(
        ts_event=_T0, ts_ingest=_T0, source="test", instrument=inst,
        bid_price=Decimal("50000.00"), bid_size=Decimal("1"),
        ask_price=Decimal("50001.00"), ask_size=Decimal("1"),
    )


async def test_bad_diagnostics_is_isolated_not_raised(inst) -> None:
    bus = _CaptureBus()
    registry = StrategyRegistry(
        bus=bus, clock=SimulatedClock(start=_T0), portfolio=StaticPortfolioView(),
    )
    registry.register(
        _BadDiagnosticsStrategy(strategy_id=StrategyId("bad"), instruments=[inst])
    )
    await registry.start()

    # Must not raise even though StrategyDiagnosticsEvent(**bad) fails to build.
    await registry._handle_market_data(_tick(inst))
