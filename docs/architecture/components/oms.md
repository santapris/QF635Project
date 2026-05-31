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
- Adopt pre-existing venue orders at startup and on periodic resync

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
- A leg with a different price or quantity on the same side is amended
  (futures: native PUT; spot: cancel then re-place).
- A leg with no resting order is placed fresh.
- A resting order matching no leg is cancelled (`reconcile_withdrawn`).

Matching is **per-leg keyed by `leg_id`**, so a price ladder with several legs
on the same side reconciles correctly. Reconciliation is scoped to the
signal's `strategy_id`, so a strategy never cancels orders it doesn't own
(including adopted `external` orders — see Adoption).

**Reconciliation is signal-driven.** It runs only when the strategy emits a new
signal for an instrument. Resting orders for an instrument a strategy has gone
quiet on are not re-evaluated until the next signal arrives.

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
venue (left resting across a restart, or discovered on a periodic resync).
Adopted orders enter at `ACKNOWLEDGED`/`PARTIALLY_FILLED`, carry no
`parent_leg_id`, and are attributed to a strategy by parsing the venue
`clientOrderId` — falling back to the reserved `external` strategy id when it
doesn't match our minting scheme (`{strategy_id}-{12hex}`). Idempotent on
`clientOrderId`.

**Adoption policy**: adopted orders flow through exactly the same
per-strategy reconciliation loop as self-placed orders. The reconciler decides
whether to keep (matches a desired leg) or cancel (matches no desired leg) on
the next signal for that instrument. There is no separate "orphan detection" at
adoption time — the single desired-leg-matching rule is the source of truth for
what should be resting, regardless of how an order was created locally.

The venue gateway's `StateBootstrapper` calls `adopt_order` at startup and on a
periodic resync (see [Order Gateways](order-gateways.md)).

## Order State Machine

```
PENDING_NEW
    │
    ├──▶ ACKNOWLEDGED ──▶ PENDING_AMEND ──▶ ACKNOWLEDGED
    │         │                │
    │         │                └──▶ CANCELLED / REJECTED / FILLED
    │         │
    │         ├──▶ PARTIALLY_FILLED ──▶ FILLED
    │         ├──▶ PENDING_CANCEL ──▶ CANCELLED
    │         ├──▶ CANCELLED
    │         └──▶ FILLED
    │
    ├──▶ REJECTED
    └──▶ CANCELLED  (cancel raced the ack)
```

Terminal states: `FILLED`, `CANCELLED`, `REJECTED`, `EXPIRED`. No transitions
are permitted out of a terminal state; attempts raise `InvalidStateTransitionError`.

Notable race-handling transitions documented in `state_machine.py`:
- `PENDING_NEW → CANCELLED`: cancel arrived before the gateway acked (some
  venues handle this).
- `PENDING_CANCEL → FILLED/PARTIALLY_FILLED`: fill raced the cancel and won.
- `PENDING_AMEND → CANCELLED`: venue cancelled the order during an in-flight
  amend (e.g. a futures GTX amend that would cross the book — see Amend
  Semantics below).

## Amend Semantics

Amend behaviour is venue-dependent and deliberately hidden from the OMS:

**Futures** (`futures=True`): the gateway issues a native `PUT /fapi/v1/order`.
Binance modifies the order in place (same `orderId` and `clientOrderId`). The
OMS receives `OrderAmended` with the venue's *actual* resulting price and
quantity (parsed from the PUT response), not the requested values — important
because the venue can clamp a partial-fill amend. If the PUT returns
`status: CANCELED` (a GTX/post-only amend whose new price would cross, or a
quantity reduced below `executedQty`), the gateway publishes `OrderCancelled`
instead of `OrderAmended`, so the OMS terminalizes correctly and the strategy
re-places on the next reconcile tick.

**Spot** (`futures=False`): no modify endpoint — the gateway cancels the old
order (`OrderCancelled`). The OMS exits `PENDING_AMEND`, and the reconciler
re-places a fresh order on the next signal tick.

In both cases the OMS sees only canonical events; it does not know which path
ran.

## Idempotency

Every `OrderRequest` carries a `client_order_id` minted as
`{strategy_id}-{order_id.hex[:12]}`. The venue deduplicates on this id, so a
timed-out REST call that is retried will not double-submit. The OMS's
`_coid_to_order_id` map provides O(1) reverse-lookup from venue events back to
internal `OrderId` without scanning all orders.

## What the OMS Is and Is Not

The OMS is the **local source of truth for resting orders** — lifecycle, price,
leaves quantity, fill accounting per order. It is not the source of truth for
positions. Fill events are published on `Topic.FILLS` and consumed independently
by the `PositionEngine`; the OMS also applies fills to per-order accounting, but
the two components do not share state and can transiently disagree during a
missed-event gap. See [Position](position.md) for the position side.
