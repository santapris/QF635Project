# Order Gateways

**Package**: `trading.order_gateways`

## Responsibilities

- Translate internal `OrderRequest` events into exchange-specific API calls
- Handle authentication (API keys, HMAC signing)
- Map exchange order IDs ↔ internal order IDs
- Receive fills and cancellations; publish to the event bus
- Rate limit management and throttle queuing

**Inputs**: `OrderRequest`, `CancelRequest`, `AmendRequest`  
**Outputs**: `OrderAcknowledged`, `FillEvent`, `OrderCancelled`, `OrderRejected`

## Module Structure

```
order_gateways/
├── base.py              # AbstractOrderGateway interface
├── registry.py          # Gateway registration and lookup
├── rate_limiter.py      # Request-weight and order-count windows
├── sim_config.py        # Simulation gateway configuration
├── simulation.py        # In-process simulated gateway
└── binance/
    ├── config.py            # Binance-specific config (URLs, limits)
    ├── signing.py           # HMAC-SHA256 request signing
    ├── rest_client.py       # Raw HTTP client (httpx)
    ├── rest.py              # REST order placement / cancellation
    ├── ws.py                # WebSocket connection management
    ├── public_ws.py         # Public market data streams
    ├── user_data.py         # User data stream (fills, order updates)
    ├── listen_key.py        # listenKey keepalive (refreshed every 25 min)
    ├── depth_book.py        # Local order book from depth stream
    ├── stream_names.py      # Stream name constants
    ├── symbols.py           # Symbol normalization utilities
    ├── order_translation.py # Internal order ↔ Binance order format
    ├── order_gateway.py     # Full Binance gateway implementation
    ├── errors.py            # Binance error code handling
    └── reconciler.py        # Periodic position reconciliation via REST
```

## Supported Gateway Types

| Gateway Type    | Protocol        | Latency Profile |
|-----------------|-----------------|-----------------|
| Binance         | WebSocket / REST | Low            |
| Simulation      | In-process      | Zero            |

## Failure Handling

- Order rejection → publish `OrderRejected`, notify OMS
- Network timeout → idempotent retry using `client_order_id` for dedup
- Partial fill → tracked in OMS; gateway publishes each fill event independently
- Binance `-2011` (cancel received for already-filled order) → handled gracefully in `errors.py`

## Rate Limiting

The `rate_limiter.py` module tracks:
- **Request weight** window (synced to `X-MBX-USED-WEIGHT` response header)
- **Order count** windows: 1s / 1m / 1d

`can_place()` / `record()` / `wait_if_needed()` are called by the gateway before each order submission to avoid exchange bans.
