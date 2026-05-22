"""Pub/sub abstraction with three interchangeable backends."""

from .asyncio_bus import AsyncioBus
from .base import AbstractEventBus, EventHandler, Topic
from .memory_bus import MemoryBus

# KafkaBus is *not* eagerly imported — it pulls in aiokafka. Users who
# need it can ``from trading.event_bus.kafka_bus import KafkaBus``.

__all__ = [
    "AbstractEventBus",
    "AsyncioBus",
    "EventHandler",
    "MemoryBus",
    "Topic",
]
