"""In-memory synchronous bus. Test-only — do not use in production paths.

Why a dedicated test bus instead of always using AsyncioBus?

- Synchronous delivery means a test can ``await bus.publish(...)`` and
  immediately assert on the consequences. No ``asyncio.sleep(0)`` dance,
  no flaky scheduling.
- Handler exceptions propagate to the publisher rather than being caught
  and logged. A test that expects a handler to fail can ``pytest.raises``
  around the publish call.
- Recording every published event makes "did this side effect happen?"
  assertions trivial.

If a test depends on the production bus's queueing or error-isolation
semantics — and a few will — it should use AsyncioBus directly with a
small queue size.
"""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable

from ..core.events import BaseEvent
from ..core.exceptions import EventBusError
from .base import EventHandler


class MemoryBus:
    """Synchronous in-memory pub/sub. Intended for unit tests."""

    def __init__(self, *, record_events: bool = True) -> None:
        self._subscribers: dict[str, list[EventHandler]] = defaultdict(list)
        self._record_events = record_events
        self._published: deque[tuple[str, BaseEvent]] = deque()
        self._stopped = False

    # --- Bus protocol ------------------------------------------------------

    async def publish(self, topic: str, event: BaseEvent) -> None:
        if self._stopped:
            raise EventBusError("bus is stopped", topic=topic)
        if self._record_events:
            self._published.append((topic, event))
        # Snapshot the handler list — handlers may subscribe/unsubscribe
        # mid-dispatch and we don't want to surprise them.
        for handler in list(self._subscribers.get(topic, ())):
            await handler(event)

    async def subscribe(self, topic: str, handler: EventHandler) -> None:
        self._subscribers[topic].append(handler)

    async def subscribe_many(
        self, topics: Iterable[str], handler: EventHandler
    ) -> None:
        for topic in topics:
            await self.subscribe(topic, handler)

    async def start(self) -> None:
        # Synchronous bus has no background work; start is a no-op.
        return None

    async def stop(self) -> None:
        self._stopped = True
        self._subscribers.clear()

    # --- Test helpers ------------------------------------------------------

    @property
    def published(self) -> list[tuple[str, BaseEvent]]:
        """All events published since construction (or last ``clear()``).

        Ordered by publication time. Consumed defensively — returning a
        list copy means the test can iterate without worrying about
        mutation.
        """
        return list(self._published)

    def published_on(self, topic: str) -> list[BaseEvent]:
        """Events published on a specific topic."""
        return [evt for t, evt in self._published if t == topic]

    def subscriber_count(self, topic: str) -> int:
        return len(self._subscribers.get(topic, ()))

    def clear(self) -> None:
        """Forget recorded events. Subscriptions remain."""
        self._published.clear()


__all__ = ["MemoryBus"]
