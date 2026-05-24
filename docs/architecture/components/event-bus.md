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

## Design Rationale

The system is built on a pub/sub event bus rather than direct method calls
between components. The choice is deliberate and worth recording so future
contributors don't relitigate it.

### Why a bus

- **Fan-out is the dominant pattern.** A single fill must update positions,
  PnL, the dashboard, persistence, and (eventually) reconciliation and metrics.
  A pipeline of direct calls forces every producer to know every consumer;
  pub/sub keeps producers ignorant of who is listening.
- **Component swap-ability.** The sim ↔ Binance gateway swap requires zero
  changes to the OMS, risk engine, or strategies. New observers (the dashboard,
  the account snapshot publisher) are added by subscribing to a topic, not by
  editing producers.
- **Observation matches structure.** The dashboard and any future metrics
  exporter live as bus subscribers at the system boundary, not as
  cross-cutting hooks inside components. Component-internal logging
  (e.g. the Binance gateway's REST error logs) stays inside the component
  and does not depend on the bus.
- **Implementation portability.** `AbstractEventBus` is the seam — the same
  components run against `AsyncioBus`, `MemoryBus`, or a future `KafkaBus` /
  `RedisBus` without modification.

### Costs accepted

- **Indirection in tracebacks.** Following an event end-to-end requires reading
  multiple files instead of one call stack. Mitigated by the topic map above
  and (planned) causation IDs (see TODO below).
- **Per-event allocation.** Pydantic validation and immutable event models
  cost more than a function call. Acceptable at our scale (single-venue,
  hundreds of ticks/sec); revisit if hot paths emerge.
- **Ordering and backpressure are explicit concerns.** Ordering only holds
  per (topic, producer); backpressure raises `BackpressureError` rather than
  blocking silently. Components must handle both.

### When to reconsider

Switch off the bus pattern (or layer something on top) when:

- Sub-millisecond internal latency becomes a requirement (not the case for
  retail crypto where venue round-trip dominates).
- The system collapses to a strict single-producer-single-consumer pipeline
  (it doesn't; the graph is multi-fanout).
- Operationally distributed deployment is needed — at that point swap the
  backend (`KafkaBus`, `RedisBus`) rather than the pattern.

### TODO

- [ ] **Causation IDs.** Thread a `causation_id` through `BaseEvent` so a
  signal → risk decision → order → fill chain can be filtered to a single
  trace. The biggest quality-of-life fix for the indirection cost. Already
  have `event_id`; need to propagate a parent reference at each hop.

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
