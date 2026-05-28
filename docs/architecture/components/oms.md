# Order Management System (OMS)

**Package**: `trading.oms`

## Responsibilities

- Cache each `SignalEvent`, then on the matching approved `RiskDecision`
  reconcile open orders against the strategy's desired leg state
- Maintain a lifecycle state machine for every order
- Track partial fills and aggregate fill quantities
- Route each leg through an `ExecutionRouter` that maps the leg's
  `ExecutionIntent` to either a single order or an execution algo (slicing)
- Publish a working-order snapshot (`OpenOrdersSnapshotEvent`) on every
  order-state change, for risk and the dashboard
- Adopt pre-existing venue orders at startup (recover mid-trade)

**Inputs**: `SignalEvent`, `RiskDecision`, `FillEvent`, order-gateway responses,
`market-data` (mark cache only)  
**Outputs**: `OrderRequest`, `CancelRequest`, `AmendRequest`,
`OpenOrdersSnapshotEvent`, `ExecutionRoutedEvent`

## Module Structure

```
oms/
├── engine.py            # Reconciliation, lifecycle, algo driver, adoption
├── router.py            # ExecutionRouter: intent → algo-or-immediate
├── state_machine.py     # Order FSM transitions
├── order.py             # Order data model (carries parent_leg_id)
└── execution_algos/
    ├── base.py          # ExecutionAlgo interface + ChildOrderSpec
    ├── immediate.py     # Market / aggressive limit
    ├── twap.py          # Time-weighted average price slicer
    └── vwap.py          # Volume-weighted average price slicer
```

## Reconciliation Model

A `SignalEvent` declares the **complete desired order state** for an
instrument. The OMS diffs the approved legs against currently open orders:

- A leg matching an open order's `(side, price, leaves)` is left untouched —
  preserving queue position.
- A leg with no match is placed fresh.
- An open order matching no leg is cancelled (withdrawn or stale).

Matching is **per-leg keyed by `leg_id`**, so a price ladder with several legs
on the same side reconciles correctly. Reconciliation is scoped to the
signal's `strategy_id`, so a strategy never cancels orders it doesn't own
(including adopted `external` orders — see Adoption).

## Execution Routing & Slicing

Strategies declare *intent* (`PASSIVE` / `NORMAL` / `URGENT`); they never name
an algorithm. An injectable `ExecutionRouter` maps each leg's intent — plus
market state (cached mark) and venue rules — to a `RoutingDecision`:

- `None` → place a single order, reconcile in place. `PASSIVE` always routes
  here, so market-making is unchanged and never slices.
- an `ExecutionAlgo` → the OMS owns it, keyed by `leg_id`, and drives it to
  emit child orders (each stamped with `parent_leg_id`).

Algos are driven by a **timer** (`algo_driver_interval_seconds`), not by
market-data ticks — so slicing cadence can't be starved or flooded by tick
rate. Re-signalling the same `leg_id` resumes the running algo; a withdrawn
leg cancels the algo and its in-flight children. Each routing decision is
emitted as an `ExecutionRoutedEvent` for observability.

`router.py` ships a `DefaultExecutionRouter` (TWAP above a notional threshold;
single clip otherwise). VWAP is stubbed pending traded-volume in the routing
context.

## Adoption (recover mid-trade)

The OMS exposes `adopt_order(...)` to seed an order that already exists on the
venue (left resting across a restart, or placed externally). Adopted orders
enter at `ACKNOWLEDGED`/`PARTIALLY_FILLED`, carry no `parent_leg_id`, and are
attributed to a strategy by parsing the venue `clientOrderId` — falling back
to the reserved `external` strategy id when it doesn't match our minting
scheme (`{strategy_id}-{12hex}`). Idempotent on `clientOrderId`. The venue
gateway's state reconciler calls this at startup and on a periodic resync (see
[Order Gateways](order-gateways.md)).

## Order State Machine

```
PENDING_NEW
    │
    ▼
ACKNOWLEDGED ───────────────▶ REJECTED
    │
    ├──▶ PARTIALLY_FILLED ──▶ FILLED
    │
    ├──▶ PENDING_CANCEL ──▶ CANCELLED
    │
    └──▶ FILLED
```

## Idempotency

Every `OrderRequest` carries a `client_order_id` (UUID). The OMS uses this to deduplicate retries — if an order is submitted twice with the same `client_order_id`, the second submission is a no-op. This protects against double-submission on network retry.
