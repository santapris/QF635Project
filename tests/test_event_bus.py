from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from trading.core.events import TickEvent
from trading.event_bus.asyncio_bus import AsyncioBus


def _make_tick(**kwargs) -> TickEvent:
    defaults = dict(
        instrument_id="BTC-USDT",
        bid_price=30000.0,
        ask_price=30001.0,
        bid_size=1.0,
        ask_size=1.0,
        exchange="binance",
    )
    return TickEvent(**{**defaults, **kwargs})


@pytest.mark.asyncio
async def test_publish_subscribe_delivers_event():
    bus = AsyncioBus()
    received: list[TickEvent] = []

    async def handler(evt: TickEvent) -> None:
        received.append(evt)

    await bus.subscribe("ticks", handler)
    tick = _make_tick()
    await bus.publish("ticks", tick)
    await asyncio.sleep(0.05)

    assert len(received) == 1
    assert received[0].event_id == tick.event_id


@pytest.mark.asyncio
async def test_multiple_handlers_all_called():
    bus = AsyncioBus()
    calls: list[str] = []

    async def handler_a(evt: TickEvent) -> None:
        calls.append("a")

    async def handler_b(evt: TickEvent) -> None:
        calls.append("b")

    await bus.subscribe("ticks", handler_a)
    await bus.subscribe("ticks", handler_b)
    await bus.publish("ticks", _make_tick())
    await asyncio.sleep(0.05)

    assert sorted(calls) == ["a", "b"]


@pytest.mark.asyncio
async def test_topics_are_isolated():
    bus = AsyncioBus()
    received: list[TickEvent] = []

    async def handler(evt: TickEvent) -> None:
        received.append(evt)

    await bus.subscribe("ticks", handler)
    await bus.publish("other_topic", _make_tick())
    await asyncio.sleep(0.05)

    assert received == []


@pytest.mark.asyncio
async def test_handler_exception_does_not_kill_bus():
    bus = AsyncioBus()
    good_received: list[TickEvent] = []

    async def bad_handler(evt: TickEvent) -> None:
        raise RuntimeError("intentional error")

    async def good_handler(evt: TickEvent) -> None:
        good_received.append(evt)

    await bus.subscribe("ticks", bad_handler)
    await bus.subscribe("ticks", good_handler)

    tick = _make_tick()
    await bus.publish("ticks", tick)
    await asyncio.sleep(0.05)

    # good_handler still ran despite bad_handler raising
    assert len(good_received) == 1


@pytest.mark.asyncio
async def test_publish_multiple_events_ordered():
    bus = AsyncioBus()
    prices: list[float] = []

    async def handler(evt: TickEvent) -> None:
        prices.append(evt.bid_price)

    await bus.subscribe("ticks", handler)
    for price in [100.0, 200.0, 300.0]:
        await bus.publish("ticks", _make_tick(bid_price=price))
    await asyncio.sleep(0.05)

    assert prices == [100.0, 200.0, 300.0]
