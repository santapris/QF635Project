# Event Bus

**Package**: `trading.event_bus`

## Responsibilities

- Decouple all components via pub/sub
- Guarantee ordered delivery within a topic partition
- Support both in-process (`asyncio.Queue`) and out-of-process (Kafka) modes
- Persist all events for replay (Kafka log retention / event store)
- Provide backpressure signaling to publishers

## Module Structure

```
event_bus/
├── base.py          # AbstractEventBus Protocol definition
├── asyncio_bus.py   # Single-process asyncio.Queue implementation (Phase 1–2)
├── kafka_bus.py     # Production Kafka implementation (Phase 3+)
└── memory_bus.py    # Synchronous in-memory bus for unit tests and backtesting
```

## Interface

```python
class AbstractEventBus(Protocol):
    async def publish(self, topic: str, event: BaseEvent) -> None: ...
    async def subscribe(self, topic: str, handler: Callable) -> None: ...
    async def subscribe_many(self, topics: list[str], handler: Callable) -> None: ...
```

## Switching Implementations

```python
# Development / Phase 1–2
bus = AsyncioBus()

# Production / Phase 3+
bus = KafkaBus(bootstrap_servers="kafka:9092", group_id="strategy-engine-1")

# Tests / Backtest
bus = MemoryBus()
```

The bus constructor argument is the only thing that changes — all component code is identical across modes.

## Backpressure Handling

```python
class AsyncioBus:
    def __init__(self, max_queue_size: int = 10_000):
        self._queues: dict[str, asyncio.Queue] = defaultdict(
            lambda: asyncio.Queue(maxsize=max_queue_size)
        )

    async def publish(self, topic: str, event: BaseEvent):
        try:
            self._queues[topic].put_nowait(event)
        except asyncio.QueueFull:
            metrics.event_bus_dropped_total.labels(topic=topic).inc()
            logger.warning("bus_queue_full", topic=topic, event_type=event.event_type)
            if topic in CRITICAL_TOPICS:
                await self._queues[topic].put(event)  # Block on critical topics
            # else: drop silently with metric increment
```

## Topic Map

| Topic           | Producers              | Consumers                         |
|-----------------|------------------------|-----------------------------------|
| `market-data`   | Feed Handler           | Strategy, Risk, Position          |
| `signals`       | Strategy Engine        | Risk Engine                       |
| `risk-decisions`| Risk Engine            | OMS                               |
| `orders`        | OMS                    | Order Gateways, Monitoring        |
| `fills`         | Order Gateways         | OMS, Position Engine, Monitoring  |
| `positions`     | Position Engine        | Risk Engine, Dashboard            |
| `alerts`        | Risk, Feed, OMS        | Monitoring, Dashboard             |
| `system`        | Kill Switch, Admin     | All components                    |
