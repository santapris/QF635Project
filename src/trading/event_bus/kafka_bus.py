"""Kafka-backed event bus.

Production multi-process bus. Same protocol as :class:`AsyncioBus` so a
single config flag swaps the two — no code changes anywhere downstream.

Design notes:

- ``aiokafka`` is imported lazily inside the constructor. Users who never
  instantiate :class:`KafkaBus` do not need the dependency installed.
- Each ``subscribe`` call creates its own consumer group named
  ``{client_id}-{handler_name}``. That gives every handler the full event
  stream (broadcast semantics). To horizontally scale a single handler
  across processes, set the same ``client_id`` on each instance — they
  will then share the group and load-balance across partitions.
- Events are JSON-serialized via Pydantic and parsed back through the
  ``Event`` discriminated union, which preserves Decimal precision and
  reconstructs the correct subtype.
- Partition key: ``instrument.instrument_id`` when present, else
  ``strategy_id``, else ``None``. This keeps order-of-events for the same
  instrument inside one partition (Kafka's only ordering guarantee).
"""

from __future__ import annotations

import structlog
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter

from ..core.events import BaseEvent, Event
from ..core.exceptions import EventBusError
from .base import EventHandler

if TYPE_CHECKING:  # pragma: no cover
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

_log = structlog.get_logger(__name__)

# Built once at import time. The discriminated union dispatches to the
# correct subtype based on the ``event_type`` field.
_event_adapter: TypeAdapter[BaseEvent] = TypeAdapter(Event)  # type: ignore[arg-type]

ErrorCallback = Callable[[str, bytes, BaseException], Awaitable[None] | None]


@dataclass
class _KafkaSubscription:
    handler: EventHandler
    consumer: "AIOKafkaConsumer | None" = None
    task: Any = field(default=None)  # asyncio.Task; loose typing to avoid import cycle
    consumer_group: str = ""
    topics: tuple[str, ...] = ()


class KafkaBus:
    """Production multi-process event bus."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        client_id: str,
        topic_prefix: str = "trading",
        on_handler_error: ErrorCallback | None = None,
    ) -> None:
        try:
            from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "aiokafka is required for KafkaBus. "
                "Install with: pip install 'aiokafka>=0.11'"
            ) from exc

        self._bootstrap = bootstrap_servers
        self._client_id = client_id
        self._topic_prefix = topic_prefix
        self._on_handler_error = on_handler_error or _default_error_callback

        self._producer: AIOKafkaProducer | None = None
        self._subscriptions: list[_KafkaSubscription] = []
        self._running = False
        self._stopped = False

    # --- Bus protocol ------------------------------------------------------

    async def publish(self, topic: str, event: BaseEvent) -> None:
        if self._stopped:
            raise EventBusError("bus is stopped", topic=topic)
        if self._producer is None:
            raise EventBusError("bus not started; call start() first")

        kafka_topic = self._kafka_topic(topic)
        payload = event.model_dump_json().encode("utf-8")
        key = self._partition_key(event)
        await self._producer.send_and_wait(kafka_topic, value=payload, key=key)

    async def subscribe(self, topic: str, handler: EventHandler) -> None:
        await self.subscribe_many([topic], handler)

    async def subscribe_many(
        self, topics: Iterable[str], handler: EventHandler
    ) -> None:
        if self._stopped:
            raise EventBusError("bus is stopped")
        topic_tuple = tuple(topics)
        sub = _KafkaSubscription(
            handler=handler,
            consumer_group=f"{self._client_id}-{_handler_name(handler)}",
            topics=topic_tuple,
        )
        self._subscriptions.append(sub)
        if self._running:
            await self._start_subscription(sub)

    async def start(self) -> None:
        if self._running or self._stopped:
            return
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap,
            client_id=self._client_id,
            # acks=all + idempotent gives at-least-once with no duplicates
            # within a producer session — the right default for trading.
            acks="all",
            enable_idempotence=True,
            compression_type="lz4",
        )
        await self._producer.start()
        self._running = True
        for sub in self._subscriptions:
            await self._start_subscription(sub)

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._running = False
        # Cancel consumers first so they stop pulling, then close producer.
        import asyncio as _asyncio

        for sub in self._subscriptions:
            if sub.task is not None and not sub.task.done():
                sub.task.cancel()
        for sub in self._subscriptions:
            if sub.task is not None:
                try:
                    await sub.task
                except _asyncio.CancelledError:
                    pass
            if sub.consumer is not None:
                await sub.consumer.stop()
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    # --- Internals ---------------------------------------------------------

    async def _start_subscription(self, sub: _KafkaSubscription) -> None:
        import asyncio as _asyncio

        from aiokafka import AIOKafkaConsumer

        kafka_topics = [self._kafka_topic(t) for t in sub.topics]
        consumer = AIOKafkaConsumer(
            *kafka_topics,
            bootstrap_servers=self._bootstrap,
            group_id=sub.consumer_group,
            client_id=self._client_id,
            enable_auto_commit=False,
            auto_offset_reset="latest",
        )
        await consumer.start()
        sub.consumer = consumer
        sub.task = _asyncio.create_task(
            self._consume(sub),
            name=f"kafka-bus-{sub.consumer_group}",
        )

    async def _consume(self, sub: _KafkaSubscription) -> None:
        import asyncio as _asyncio

        assert sub.consumer is not None
        try:
            async for msg in sub.consumer:
                try:
                    event = _event_adapter.validate_json(msg.value)
                except Exception as exc:
                    _log.exception(
                        "failed_to_deserialize_kafka_message",
                        topic=msg.topic, offset=msg.offset,
                    )
                    await sub.consumer.commit()
                    continue

                try:
                    await sub.handler(event)
                except _asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    try:
                        result = self._on_handler_error(msg.topic, msg.value, exc)
                        if _asyncio.iscoroutine(result):
                            await result
                    except Exception:
                        _log.exception("error_callback_itself_raised")

                # Commit only after the handler has succeeded (or its error
                # was acknowledged via the callback). At-least-once.
                await sub.consumer.commit()
        except _asyncio.CancelledError:
            return

    def _kafka_topic(self, topic: str) -> str:
        return f"{self._topic_prefix}.{topic}"

    @staticmethod
    def _partition_key(event: BaseEvent) -> bytes | None:
        # Order is preserved within a partition only. Picking a stable key
        # per instrument keeps a single instrument's events on one partition.
        instrument = getattr(event, "instrument", None)
        if instrument is not None:
            return str(instrument.instrument_id).encode("utf-8")
        strategy_id = getattr(event, "strategy_id", None)
        if strategy_id is not None:
            return str(strategy_id).encode("utf-8")
        return None


async def _default_error_callback(
    topic: str, raw: bytes, exc: BaseException
) -> None:
    _log.exception(
        "kafka_handler_raised",
        topic=topic, raw_size=len(raw),
    )


def _handler_name(handler: EventHandler) -> str:
    """Best-effort stable name for a handler, used in consumer group ids."""
    name = getattr(handler, "__qualname__", None) or getattr(
        handler, "__name__", None
    )
    if name:
        return name.replace(".", "_").replace("<", "_").replace(">", "_")
    return f"handler_{id(handler):x}"


__all__ = ["KafkaBus"]
