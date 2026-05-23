"""Unit tests for the event bus."""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.core import Side, SignalEvent, StrategyId
from trading.core.exceptions import BackpressureError, EventBusError
from trading.event_bus import AsyncioBus, MemoryBus, Topic


def _signal(clock, btc, strategy_id, qty="1") -> SignalEvent:
    return SignalEvent(
        ts_event=clock.now_ns(),
        ts_ingest=clock.now_ns(),
        source="test",
        strategy_id=strategy_id,
        instrument=btc,
        side=Side.BUY,
        target_quantity=Decimal(qty),
    )


async def test_memory_bus_basic_publish_subscribe(clock, btc, strategy_id) -> None:
    bus = MemoryBus()
    received = []

    async def handler(evt):
        received.append(evt)

    await bus.subscribe(Topic.SIGNALS, handler)
    await bus.publish(Topic.SIGNALS, _signal(clock, btc, strategy_id))
    assert len(received) == 1


async def test_memory_bus_propagates_errors(clock, btc, strategy_id) -> None:
    bus = MemoryBus()

    async def bad(_):
        raise ValueError("test failure")

    await bus.subscribe(Topic.SIGNALS, bad)
    with pytest.raises(ValueError):
        await bus.publish(Topic.SIGNALS, _signal(clock, btc, strategy_id))


async def test_memory_bus_records_published(clock, btc, strategy_id) -> None:
    bus = MemoryBus()
    sig = _signal(clock, btc, strategy_id)
    await bus.publish(Topic.SIGNALS, sig)
    assert bus.published_on(Topic.SIGNALS) == [sig]


async def test_asyncio_bus_fan_out(clock, btc, strategy_id) -> None:
    bus = AsyncioBus(queue_size=100)
    a, b = [], []

    async def ah(evt): a.append(evt)
    async def bh(evt): b.append(evt)

    await bus.subscribe(Topic.SIGNALS, ah)
    await bus.subscribe(Topic.SIGNALS, bh)
    await bus.start()
    await bus.publish(Topic.SIGNALS, _signal(clock, btc, strategy_id))

    import asyncio
    await asyncio.sleep(0.05)
    await bus.stop()
    assert len(a) == 1 and len(b) == 1


async def test_asyncio_bus_isolates_handler_errors(clock, btc, strategy_id) -> None:
    bus = AsyncioBus(queue_size=100)
    good = []
    errs = []

    async def good_h(evt): good.append(evt)
    async def bad_h(_): raise RuntimeError("boom")

    async def on_error(topic, evt, exc):
        errs.append((topic, type(exc).__name__))

    bus._on_handler_error = on_error
    await bus.subscribe(Topic.SIGNALS, good_h)
    await bus.subscribe(Topic.SIGNALS, bad_h)
    await bus.start()
    await bus.publish(Topic.SIGNALS, _signal(clock, btc, strategy_id))

    import asyncio
    await asyncio.sleep(0.05)
    await bus.stop()
    assert len(good) == 1
    assert errs == [(Topic.SIGNALS, "RuntimeError")]


async def test_asyncio_bus_backpressure(clock, btc, strategy_id) -> None:
    import asyncio
    bus = AsyncioBus(queue_size=2)
    block = asyncio.Event()

    async def slow(_): await block.wait()

    await bus.subscribe(Topic.SIGNALS, slow)
    await bus.start()
    await bus.publish(Topic.SIGNALS, _signal(clock, btc, strategy_id))
    await bus.publish(Topic.SIGNALS, _signal(clock, btc, strategy_id))
    with pytest.raises(BackpressureError):
        await bus.publish(Topic.SIGNALS, _signal(clock, btc, strategy_id))
    block.set()
    await bus.stop()


async def test_asyncio_bus_topics_are_isolated(clock, btc, strategy_id) -> None:
    import asyncio
    bus = AsyncioBus(queue_size=100)
    received_a: list = []
    received_b: list = []

    async def handler_a(evt): received_a.append(evt)
    async def handler_b(evt): received_b.append(evt)

    await bus.subscribe(Topic.SIGNALS, handler_a)
    await bus.subscribe(Topic.MARKET_DATA, handler_b)
    await bus.start()
    await bus.publish(Topic.SIGNALS, _signal(clock, btc, strategy_id))
    await asyncio.sleep(0.05)
    await bus.stop()
    assert len(received_a) == 1
    assert received_b == []


async def test_asyncio_bus_ordered_delivery(clock, btc, strategy_id) -> None:
    import asyncio
    bus = AsyncioBus(queue_size=100)
    prices: list[Decimal] = []

    async def handler(evt): prices.append(evt.target_quantity)

    await bus.subscribe(Topic.SIGNALS, handler)
    await bus.start()
    for qty in ["1", "2", "3"]:
        await bus.publish(Topic.SIGNALS, _signal(clock, btc, strategy_id, qty=qty))
    await asyncio.sleep(0.05)
    await bus.stop()
    assert prices == [Decimal("1"), Decimal("2"), Decimal("3")]
