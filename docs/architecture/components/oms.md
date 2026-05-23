# Order Management System (OMS)

**Package**: `trading.oms`

## Responsibilities

- Receive approved `RiskDecision` events and convert to exchange orders
- Maintain a lifecycle state machine for every order
- Track partial fills and aggregate fill quantities
- Route orders to the correct exchange order gateway
- Support order types: market, limit, stop-limit, IOC, FOK, TWAP, VWAP

**Inputs**: `RiskDecision`  
**Outputs**: `OrderRequest`, `CancelRequest`, `AmendRequest`

## Module Structure

```
oms/
├── engine.py            # Order lifecycle management, partials, timeouts
├── router.py            # Routes orders to the correct gateway
├── state_machine.py     # Order FSM transitions
├── order.py             # Order data model
└── execution_algos/
    ├── base.py          # AbstractExecutionAlgo interface
    ├── immediate.py     # Market / aggressive limit
    ├── twap.py          # Time-weighted average price slicer
    └── vwap.py          # Volume-weighted average price slicer
```

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
