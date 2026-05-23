# Feed Handler

**Package**: `trading.feed_handler`

## Responsibilities

- Connect to exchange WebSocket and REST endpoints
- Reconnect with exponential backoff on disconnection
- Normalize raw exchange-specific data into canonical events
- Sequence and timestamp all incoming data (exchange time + local receive time)
- Publish normalized events to the event bus
- Maintain an in-memory order book (bid/ask ladder)

**Inputs**: Raw WebSocket frames, REST poll responses  
**Outputs**: `TickEvent`, `TradeEvent`, `OrderBookEvent`

## Module Structure

```
feed_handler/
├── base.py              # AbstractFeedHandler interface
├── engine.py            # Feed handler orchestration loop
├── normalizer.py        # Base normalizer interface
├── order_book.py        # L2/L3 order book reconstruction
├── sequencer.py         # Gap detection, sequence number tracking
├── connectors/
│   └── simulated.py     # Simulated connector for backtesting
└── normalizers/
    └── binance.py       # Binance-specific JSON → canonical events
```

## Failure Handling

- WebSocket disconnect → exponential backoff reconnect (1s, 2s, 4s, max 60s)
- Gap in sequence numbers → request snapshot, re-subscribe
- Stale data detection → heartbeat check every N seconds
- Circuit breaker: reconnect fails > 10 times → raise `FeedUnavailableAlert`

### Reconnect Pattern

```python
async def reconnect_with_backoff(connect_fn, max_retries=10):
    for attempt in range(max_retries):
        try:
            return await connect_fn()
        except ConnectionError:
            if attempt == max_retries - 1:
                raise
            delay = min(2 ** attempt + random.uniform(0, 1), 60)
            logger.warning("reconnect_attempt", attempt=attempt, delay=delay)
            await asyncio.sleep(delay)
```

### Staleness Detection

```python
class FeedHealthMonitor:
    async def monitor(self):
        while True:
            await asyncio.sleep(self.check_interval_seconds)
            for instrument_id, last_tick_time in self.last_tick_times.items():
                age = datetime.utcnow() - last_tick_time
                if age > self.stale_threshold:
                    await self.alerting.send_warning(
                        f"Stale feed: {instrument_id}, last tick {age.seconds}s ago"
                    )
```

## Scaling

One Feed Handler process per exchange. For ultra-low latency: replace Python with C++ using Boost.Asio, publishing to the same Kafka topic.
