# Position & PnL Engine

**Package**: `trading.position`

## Responsibilities

- Maintain real-time inventory per instrument and strategy
- Calculate unrealized and realized PnL using mark-to-market prices
- Track cost basis using configurable accounting method (FIFO, LIFO, weighted average)
- Publish `PositionUpdateEvent` on every fill or price update
- Reconcile against exchange positions periodically

**Inputs**: `FillEvent`, `TickEvent` (for mark-to-market)  
**Outputs**: `PositionUpdateEvent`, `PnLSnapshotEvent`

## Module Structure

```
position/
├── engine.py            # Core position tracking and event dispatch
├── pnl.py               # Realized / unrealized PnL computation
├── lots.py              # Lot tracking for FIFO/LIFO accounting
├── portfolio_view.py    # Read-only portfolio snapshot for strategy/risk
└── accounting/
    ├── base.py          # AbstractCostBasisMethod interface
    ├── lot_book.py      # FIFO / LIFO lot matching
    └── wavg.py          # Weighted-average cost basis
```

## PnL Calculations

- **Realized PnL**: computed on each closing fill using the configured cost basis method
- **Unrealized PnL**: `(mark_price − avg_entry_price) × net_quantity`
- **Mark price**: mid-price from latest `TickEvent`, or microprice if available
- **Spread capture**: tracked separately for market-making strategies (maker fills)
- **Adverse selection**: mark price at `t+N` ticks post-fill for microstructure analysis
