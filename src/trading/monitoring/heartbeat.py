"""Bus topic heartbeats — periodic aggregate log lines for high-volume topics.

For topics where per-event terminal logging is unreadable (market data, in
particular), this counts events over a window and emits one line per window:

    market_data_heartbeat ticks=423 trades=12 last_age_ms=87 instruments=2

That answers "is data flowing?" — the question you actually have in the
terminal — without drowning out everything else.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from typing import Any

import structlog

from ..event_bus.base import AbstractEventBus, Topic

_Logger = structlog.stdlib.BoundLogger


class _TopicCounter:
    """Per-topic event counter with last-seen timestamp and event-type tally."""

    __slots__ = ("count", "by_type", "last_seen_monotonic", "instruments")

    def __init__(self) -> None:
        self.count: int = 0
        self.by_type: dict[str, int] = {}
        self.last_seen_monotonic: float | None = None
        self.instruments: set[str] = set()

    def record(self, event: Any) -> None:
        self.count += 1
        self.last_seen_monotonic = time.monotonic()
        type_name = type(event).__name__
        self.by_type[type_name] = self.by_type.get(type_name, 0) + 1
        instrument = getattr(event, "instrument", None)
        if instrument is not None:
            symbol = getattr(instrument, "symbol", None) or str(instrument)
            self.instruments.add(str(symbol))


class BusHeartbeat:
    """Subscribes to chosen topics, emits an aggregate log line per interval.

    Designed for topics excluded from per-event logging (e.g. market-data).
    The default interval is 5 seconds — small enough to spot a stall quickly,
    large enough to keep the terminal calm.
    """

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        log: _Logger,
        topics: Iterable[str] = (Topic.MARKET_DATA,),
        interval_sec: float = 5.0,
    ) -> None:
        self._bus = bus
        self._log = log
        self._topics = tuple(topics)
        self._interval = interval_sec
        self._counters: dict[str, _TopicCounter] = {t: _TopicCounter() for t in self._topics}
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        for topic in self._topics:
            await self._bus.subscribe(topic, self._handler_for(topic))
        self._task = asyncio.create_task(self._run(), name="bus-heartbeat")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=self._interval + 1.0)
            except asyncio.TimeoutError:
                self._task.cancel()

    def _handler_for(self, topic: str):
        counter = self._counters[topic]

        async def _handle(event: Any) -> None:
            counter.record(event)

        return _handle

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                self._emit()
        self._emit()  # final flush on shutdown

    def _emit(self) -> None:
        now = time.monotonic()
        for topic, counter in self._counters.items():
            event_name = f"{topic.replace('-', '_')}_heartbeat"
            last_age_ms: int | None = (
                int((now - counter.last_seen_monotonic) * 1000)
                if counter.last_seen_monotonic is not None
                else None
            )
            self._log.info(
                event_name,
                count=counter.count,
                by_type=dict(counter.by_type),
                instruments=len(counter.instruments),
                last_age_ms=last_age_ms,
                window_sec=self._interval,
            )
            # Reset window. Keep last_seen_monotonic so stall detection works
            # across windows.
            counter.count = 0
            counter.by_type = {}


__all__ = ["BusHeartbeat"]
