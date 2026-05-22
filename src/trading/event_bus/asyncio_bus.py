"""Production single-process bus.

Design:

- One :class:`asyncio.Queue` per subscriber. A slow handler backs up its
  own queue without affecting others.
- Bounded queues. ``put_nowait`` raises ``QueueFull`` immediately, which
  the bus translates into :class:`BackpressureError`. Publishers are
  expected to either drop, slow down, or alert — never block waiting for
  a slow consumer.
- One consumer task per subscription. Handler exceptions are caught and
  forwarded to ``on_handler_error`` (default: structured log). One bad
  handler can never take down the bus.
- Graceful shutdown via a sentinel object pushed onto every queue.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Final

from ..core.events import BaseEvent
from ..core.exceptions import BackpressureError, EventBusError
from .base import EventHandler

_log = logging.getLogger(__name__)

# A unique sentinel pushed onto consumer queues during shutdown. The type
# annotation lies — Queue[BaseEvent] — but the consumer checks identity
# before treating it as an event, so the lie is safe.
_SHUTDOWN: Final[object] = object()

ErrorCallback = Callable[[str, BaseEvent, BaseException], Awaitable[None] | None]


async def _default_error_callback(
    topic: str, event: BaseEvent, exc: BaseException
) -> None:
    _log.exception(
        "event handler raised",
        extra={
            "topic": topic,
            "event_type": getattr(event, "event_type", "unknown"),
            "event_id": str(getattr(event, "event_id", "")),
        },
        exc_info=exc,
    )


@dataclass # without this you will need to define __init__, __repr__, __eq__
class _Subscription:
    """One handler's view of a topic. Owns its queue and consumer task."""

    handler: EventHandler
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue())
    task: asyncio.Task[None] | None = None
    name: str = ""


class AsyncioBus:
    """Single-process asyncio event bus with bounded per-subscriber queues."""

    def __init__(
        self,
        *,
        queue_size: int = 10_000,
        on_handler_error: ErrorCallback | None = None,
    ) -> None:
        if queue_size <= 0:
            raise ValueError("queue_size must be positive")
        self._queue_size = queue_size
        self._on_handler_error = on_handler_error or _default_error_callback
        self._subscribers: dict[str, list[_Subscription]] = defaultdict(list)
        self._running = False
        self._stopped = False

    # --- Bus protocol ------------------------------------------------------

    async def publish(self, topic: str, event: BaseEvent) -> None:
        if self._stopped:
            raise EventBusError("bus is stopped", topic=topic)
        subs = self._subscribers.get(topic, ())
        for sub in subs:
            try:
                sub.queue.put_nowait(event)
            except asyncio.QueueFull as exc:
                raise BackpressureError(
                    "subscriber queue full",
                    topic=topic,
                    handler=sub.name,
                    queue_size=self._queue_size,
                ) from exc

    async def subscribe(self, topic: str, handler: EventHandler) -> None:
        if self._stopped:
            raise EventBusError("bus is stopped")
        sub = _Subscription(
            handler=handler,
            queue=asyncio.Queue(maxsize=self._queue_size),
            name=getattr(handler, "__qualname__", repr(handler)),
        )
        self._subscribers[topic].append(sub)
        if self._running:
            # Subscribed after start; spin up its consumer right now.
            sub.task = asyncio.create_task(
                self._consume(topic, sub), name=f"bus-{topic}-{sub.name}"
            )

    async def subscribe_many(
        self, topics: Iterable[str], handler: EventHandler
    ) -> None:
        for topic in topics:
            await self.subscribe(topic, handler)

    async def start(self) -> None:
        if self._running or self._stopped:
            return
        self._running = True
        for topic, subs in self._subscribers.items():
            for sub in subs:
                if sub.task is None:
                    sub.task = asyncio.create_task(
                        self._consume(topic, sub),
                        name=f"bus-{topic}-{sub.name}",
                    )

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._running = False

        # Push the sentinel onto every queue so consumers exit cleanly
        # after they finish their current event.
        all_tasks: list[asyncio.Task[None]] = []
        for subs in self._subscribers.values():
            for sub in subs:
                if sub.task is not None and not sub.task.done():
                    # put (not put_nowait) — if the queue is full, wait
                    # for the consumer to drain it before signalling.
                    await sub.queue.put(_SHUTDOWN)  # type: ignore[arg-type]
                    all_tasks.append(sub.task)

        if all_tasks:
            # Bound the shutdown — a stuck handler must not hang us forever.
            try:
                await asyncio.wait_for(
                    asyncio.gather(*all_tasks, return_exceptions=True),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                _log.warning(
                    "bus stop timed out; cancelling %d hung consumers",
                    sum(1 for t in all_tasks if not t.done()),
                )
                for task in all_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*all_tasks, return_exceptions=True)

        self._subscribers.clear()

    # --- Internals ---------------------------------------------------------

    async def _consume(self, topic: str, sub: _Subscription) -> None:
        """Drain a single subscription's queue until the shutdown sentinel."""
        while True:
            event = await sub.queue.get()
            if event is _SHUTDOWN:
                return
            try:
                await sub.handler(event)
            except asyncio.CancelledError:
                # Cooperative cancellation during shutdown — let it through.
                raise
            except BaseException as exc:
                # Catch BaseException, not Exception: SystemExit and friends
                # raised inside a handler should not crash the bus loop.
                # They will still be re-raised at the application level if
                # the error callback chooses to.
                try:
                    result = self._on_handler_error(topic, event, exc)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    _log.exception("error callback itself raised")

    # --- Diagnostics -------------------------------------------------------

    def queue_depth(self, topic: str) -> list[int]:
        """Current depth of every subscriber queue on ``topic``. Monitoring hook."""
        return [sub.queue.qsize() for sub in self._subscribers.get(topic, ())]

    def subscriber_count(self, topic: str) -> int:
        return len(self._subscribers.get(topic, ()))


__all__ = ["AsyncioBus", "ErrorCallback"]
