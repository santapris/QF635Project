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
    ├── reconciler.py        # Periodic balance reconciliation (detect + alert)
    └── state_bootstrap.py   # Startup adoption + periodic order/position resync
```

## State Adoption & Reconciliation

The system treats its in-memory order/position state as a cache that must be
rebuilt from the venue — never as authoritative — so it can recover mid-trade
across a restart. `state_bootstrap.py` (`StateBootstrapper`) owns this:

**At startup** it fetches `GET /openOrders` and `GET /positionRisk` and:
- adopts every open order into the OMS via `adopt_order(...)`, attributing by
  `clientOrderId` (matching orders → their strategy; others → `external`);
- publishes the venue's net positions verbatim as `VenuePositionSnapshotEvent`
  (ground truth for the dashboard). It deliberately does **not** synthesize
  fills into the Position Engine — that would corrupt the fill-derived
  per-strategy books and PnL.

**Periodically** (default 30 s) it re-pulls and reconciles: adopt orders the
venue reports but we don't track; terminalize locally-open orders the venue no
longer reports (filled/cancelled during a user-data-stream gap); refresh venue
positions. This repairs drift from missed WS events.

The older blanket "cancel all stale orders at startup" path is now opt-in
(`oms.cancel_stale_orders_on_start`, default `False`) — wiping pre-existing
orders is the opposite of adoption.

`reconciler.py` remains the **balance** reconciler: it compares venue
balances against our position books and *alerts* on mismatch (it does not
silently correct).

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
