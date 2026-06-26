"""Per-strategy pause/resume in the StrategyRegistry.

Pausing a strategy suppresses its signal dispatch entirely; resuming restores
it. Pausing/resuming an unknown strategy_id raises KeyError. State is per
strategy, so pausing one must not affect another.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.core import AssetType, Instrument, SimulatedClock, StrategyId
from trading.core.events import OrderLeg, SignalEvent, TickEvent
from trading.core.types import OrderType, Side
from trading.strategy.base import AbstractStrategy
from trading.strategy.context import StaticPortfolioView
from trading.strategy.registry import StrategyRegistry

_T0 = 1_700_000_000_000_000_000


class _CaptureBus:
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


class _AlwaysSignals(AbstractStrategy):
    """Emits one signal on every tick — a clean dispatch tripwire."""

    async def on_tick(self, event, ctx) -> list:
        return [
            SignalEvent(
                ts_event=event.ts_event, ts_ingest=event.ts_ingest, source="test",
                strategy_id=self.strategy_id, instrument=event.instrument,
                legs=[OrderLeg(
                    side=Side.BUY, quantity=Decimal("0.001"),
                    price=event.bid_price, order_type=OrderType.LIMIT,
                )],
                rationale="test",
            )
        ]


def _tick(inst: Instrument) -> TickEvent:
    return TickEvent(
        ts_event=_T0, ts_ingest=_T0, source="test", instrument=inst,
        bid_price=Decimal("50000.00"), bid_size=Decimal("1"),
        ask_price=Decimal("50001.00"), ask_size=Decimal("1"),
    )


def _signals(bus: _CaptureBus) -> list:
    return [e for (_topic, e) in bus.published if isinstance(e, SignalEvent)]


async def _make_registry(bus, inst, *sids) -> StrategyRegistry:
    registry = StrategyRegistry(
        bus=bus, clock=SimulatedClock(start=_T0), portfolio=StaticPortfolioView(),
    )
    for sid in sids:
        registry.register(_AlwaysSignals(strategy_id=StrategyId(sid), instruments=[inst]))
    await registry.start()
    return registry


async def test_pause_suppresses_signals_resume_restores(inst) -> None:
    bus = _CaptureBus()
    registry = await _make_registry(bus, inst, "s1")

    await registry._handle_market_data(_tick(inst))
    assert len(_signals(bus)) == 1  # baseline: dispatch works

    registry.pause(StrategyId("s1"))
    assert registry.is_paused(StrategyId("s1"))
    await registry._handle_market_data(_tick(inst))
    assert len(_signals(bus)) == 1  # no new signal while paused

    registry.resume(StrategyId("s1"))
    assert not registry.is_paused(StrategyId("s1"))
    await registry._handle_market_data(_tick(inst))
    assert len(_signals(bus)) == 2  # dispatch resumed


async def test_pause_is_per_strategy(inst) -> None:
    bus = _CaptureBus()
    registry = await _make_registry(bus, inst, "s1", "s2")

    registry.pause(StrategyId("s1"))
    await registry._handle_market_data(_tick(inst))

    sids = {s.strategy_id for s in _signals(bus)}
    assert StrategyId("s1") not in sids
    assert StrategyId("s2") in sids
    assert registry.paused_ids == [StrategyId("s1")]


async def test_pause_unknown_strategy_raises(inst) -> None:
    bus = _CaptureBus()
    registry = await _make_registry(bus, inst, "s1")
    with pytest.raises(KeyError):
        registry.pause(StrategyId("nope"))
    with pytest.raises(KeyError):
        registry.resume(StrategyId("nope"))


async def test_pause_is_idempotent(inst) -> None:
    bus = _CaptureBus()
    registry = await _make_registry(bus, inst, "s1")
    registry.pause(StrategyId("s1"))
    registry.pause(StrategyId("s1"))
    assert registry.paused_ids == [StrategyId("s1")]
    registry.resume(StrategyId("s1"))
    registry.resume(StrategyId("s1"))
    assert registry.paused_ids == []
