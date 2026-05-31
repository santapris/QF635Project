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

The system treats its in-memory order/position state as a **cache** that must
be rebuilt from the venue — never as authoritative — so it can recover
mid-trade across a restart. `state_bootstrap.py` (`StateBootstrapper`) owns
this.

### Startup bootstrap

Calls `GET /openOrders` per tracked instrument and `GET /positionRisk`
(futures only):

- Adopts every open order into the OMS via `adopt_order(...)`, attributing by
  `clientOrderId` (orders minted by us → their strategy; others → `external`).
- Publishes the venue's net positions verbatim as `VenuePositionSnapshotEvent`
  (ground truth for the dashboard).

Position fills are deliberately **not** synthesized into the PositionEngine —
that would corrupt the fill-derived per-strategy books and PnL with quantities
no strategy actually traded. The venue net position row in the dashboard is
separate from the per-strategy fill-derived rows.

### Periodic resync (default 30 s)

Re-pulls `GET /openOrders` and reconciles in two steps:

1. **Adopt** any venue order we don't track. Adopted orders enter the OMS in
   the state the venue reports (ACKNOWLEDGED/PARTIALLY_FILLED) and flow
   through the per-strategy reconciliation loop on the next signal tick —
   the same matching logic as self-placed orders. The reconciler decides keep
   (matches a desired leg) or cancel (matches none); there is no separate
   "orphan cancel" at adoption time.

2. **Terminalize** any locally-open order the venue no longer reports. These
   disappeared because they filled or were cancelled during a user-data-stream
   gap. They are stamped `CANCELLED` (best available inference without an
   extra `GET /order` round-trip).

Both steps are **scoped to instruments whose `GET /openOrders` succeeded** that
pass. A transient fetch failure contributes nothing to either step — it is
never treated as "the venue has no orders for this symbol", which would
terminalize live orders on a network blip.

### Balance reconciler

`reconciler.py` compares venue balances (`GET /account`) against our
PositionEngine books and **alerts** on mismatch above a configurable threshold.
It does not auto-correct: silently rewriting positions to match the venue would
paper over a bug rather than surface it, and a wrong correction can make the
divergence worse. Operators decide on action.

## Supported Gateway Types

| Gateway Type    | Protocol        | Latency Profile |
|-----------------|-----------------|-----------------|
| Binance         | WebSocket / REST | Low            |
| Simulation      | In-process      | Zero            |

## Failure Handling

- **Logical order rejection** (bad symbol, insufficient balance, filter
  violation) → `OrderRejected` published; OMS terminalizes the order.
- **Transport / auth / rate-limit error on placement** → `OrderRejected`
  published with a distinguishing reason. The order's actual venue status is
  unknown in this case; the periodic resync will adopt it if the venue accepted
  it, or leave it terminal if not.
- **Partial fill** → each fill published independently as `FillEvent`;
  fill dedup is keyed on `fill_id` inside `Order._applied_fills`.
- **Cancel on already-terminal order** → Binance `-2011` ("unknown order");
  the gateway publishes `CancelRejected` and the OMS rolls back to
  `ACKNOWLEDGED` (order still live). Handled in `errors.py`.
- **Futures amend that cancels the order** → a GTX/post-only amend whose new
  price would cross, or a quantity reduced below `executedQty`, causes Binance
  to cancel the order and return HTTP 200 with `status: CANCELED`. The gateway
  detects this in the PUT response and publishes `OrderCancelled` (not
  `OrderAmended`), so the OMS terminalizes correctly and the strategy re-places
  on the next reconcile tick.

## Rate Limiting

The `rate_limiter.py` module tracks:
- **Request weight** window (synced to `X-MBX-USED-WEIGHT` response header)
- **Order count** windows: 1s / 1m / 1d

`can_place()` / `record()` / `wait_if_needed()` are called by the gateway before each order submission to avoid exchange bans.
