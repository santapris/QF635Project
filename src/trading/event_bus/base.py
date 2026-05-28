"""Event bus interface.

Three implementations live alongside this file:

- :class:`MemoryBus` — synchronous, single-process; the default for unit
  tests. Handler exceptions propagate so test failures surface clearly.
- :class:`AsyncioBus` — the production single-process bus. Per-subscriber
  bounded queues, error isolation, graceful shutdown.
- :class:`KafkaBus` — the production multi-process bus. Same protocol;
  the rest of the system never knows the difference.

The protocol is deliberately tiny. Anything richer (replay, offsets,
admin) belongs on a concrete bus, not on the abstraction every component
depends on.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from typing import Final, Protocol, runtime_checkable

from ..core.events import BaseEvent

EventHandler = Callable[[BaseEvent], Awaitable[None]] # safe way to say this must be async
"""All bus handlers are async. Sync work goes inside the coroutine body."""


class Topic:
    """Canonical topic names. Modules import these instead of hardcoding strings.

    Custom topics are allowed (the bus does not enforce membership), but
    using the constants prevents typos that silently never deliver.
    """
    # Final[str] locks the value for example, MARKET_DATA can only = "market-data"
    MARKET_DATA: Final[str] = "market-data" 
    SIGNALS: Final[str] = "signals"
    RISK_DECISIONS: Final[str] = "risk-decisions"
    ORDERS: Final[str] = "orders"
    OPEN_ORDERS: Final[str] = "open-orders"
    FILLS: Final[str] = "fills"
    POSITIONS: Final[str] = "positions"
    ACCOUNT: Final[str] = "account"
    ALERTS: Final[str] = "alerts"


@runtime_checkable
class AbstractEventBus(Protocol):
    """Pub/sub interface implemented by every bus backend."""

    async def publish(self, topic: str, event: BaseEvent) -> None:
        """Publish ``event`` to ``topic``.

        Ordering: events published to the same topic from the same task
        are delivered to each subscriber in publication order. No ordering
        guarantee across topics or across producers.

        Backpressure: implementations may raise
        :class:`~trading.core.exceptions.BackpressureError` when a bounded
        queue is full. Callers should treat that as a hard signal — slow
        down or drop, do not retry in a tight loop.
        """
        ...

    async def subscribe(self, topic: str, handler: EventHandler) -> None:
        """Register ``handler`` to receive events on ``topic``.

        Handlers are called once per published event. Multiple handlers on
        the same topic each receive their own copy. Subscribing after the
        bus has started is supported but the handler will only see events
        published after subscription.
        """
        ...

    async def subscribe_many(
        self, topics: Iterable[str], handler: EventHandler
    ) -> None:
        """Subscribe one handler to several topics. Same handler instance."""
        ...

    async def start(self) -> None:
        """Begin dispatching. Idempotent — calling on a running bus is a no-op."""
        ...

    async def stop(self) -> None:
        """Drain pending events, cancel consumer tasks, release resources.

        After ``stop()``, further ``publish`` calls raise. This is a hard
        shutdown intended for application teardown, not for pausing.
        """
        ...


__all__ = ["AbstractEventBus", "EventHandler", "Topic"]
