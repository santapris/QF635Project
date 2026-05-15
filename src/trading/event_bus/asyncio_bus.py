from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List

from trading.core.events import BaseEvent


Handler = Callable[[BaseEvent], Awaitable[None]]


class AsyncioBus:
    """Minimal in-process event bus using asyncio queues.

    - One queue per topic
    - Handlers subscribe per topic
    - A background task per topic reads and dispatches
    """

    def __init__(self) -> None:
        self._queues: Dict[str, asyncio.Queue[BaseEvent]] = defaultdict(asyncio.Queue)
        self._handlers: Dict[str, List[Handler]] = defaultdict(list)
        self._tasks: Dict[str, asyncio.Task] = {}

    async def publish(self, topic: str, event: BaseEvent) -> None:
        await self._queues[topic].put(event)

    async def subscribe(self, topic: str, handler: Handler) -> None:
        self._handlers[topic].append(handler)
        if topic not in self._tasks:
            self._tasks[topic] = asyncio.create_task(self._run_topic(topic))

    async def _run_topic(self, topic: str) -> None:
        q = self._queues[topic]
        while True:
            evt = await q.get()
            for h in list(self._handlers[topic]):
                try:
                    await h(evt)
                except Exception:
                    # In MVP, swallow to keep bus alive. Real impl should log/alert.
                    pass

    async def flush(self) -> None:
        # Cooperatively yield; no strict guarantee but helpful in tests
        await asyncio.sleep(0)

