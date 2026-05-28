# Dashboard Implementation Plan

## Overview

A real-time pipeline visualizer with **two channels of distinct semantics**:

- **WebSocket (`/ws`) — the event stream.** Activity events (ticks, trades,
  signals, risk decisions, orders, fills, execution-routing decisions) and log
  records are broadcast as JSON. Clients see only events that fire while
  connected.
- **REST (`/state/*`) — state-of-the-world snapshots.** Positions, account
  balances, and open orders are *state*, not events: they exist in the trading
  process regardless of when the dashboard connects. The frontend polls these
  on a timer, so panels populate correctly whether opened before or after the
  pipeline starts, and a missed event can never leave a stale row.

This split matches production dashboards: state is queryable and cacheable;
events are streamed. The server is standalone — any stage wires it in with two
lines, and it can be added or removed without touching runner logic.

---

## Architecture

```
AsyncioBus
    │
    ▼
DashboardServer          (trading.monitoring.dashboard_server)
  Starlette + uvicorn    ws://localhost:8765/ws        (event stream)
                         GET  /state/positions          (snapshot)
                         GET  /state/account            (snapshot)
                         GET  /state/open_orders         (snapshot)
    │
    ▼
React App                (dashboard/)
  usePipelineSocket      WS stream; reconnects on drop
  useStatePoll           polls /state/* on a timer
    │
    ├── MarketDataPanel
    ├── SignalsPanel
    ├── OrdersPanel               (order lifecycle log)
    ├── OpenOrdersPanel           (currently-resting orders, from snapshot)
    ├── PositionPanel + PnlChart  (per-strategy rows + exchange net row)
    └── LogsPage
```

---

## Backend

### File
`src/trading/monitoring/dashboard_server.py`

### Responsibilities
- Start a FastAPI + uvicorn server on a configurable port (default `8765`)
- Serve the WebSocket endpoint at `ws://localhost:8765/ws`
- Subscribe to all relevant bus topics on startup
- Register a structlog processor that forwards log records as `logs` topic messages
- On each event or log record, serialize to JSON and broadcast to all connected clients
- Handle client connect/disconnect cleanly
- Expose a `start()` / `stop()` interface matching other components

### Topics streamed on the WebSocket
| Topic | Events forwarded |
|-------|-----------------|
| `market-data` | `TickEvent`, `TradeEvent` |
| `signals` | `SignalEvent` |
| `risk-decisions` | `RiskDecision` |
| `orders` | `OrderRequest`, `OrderAcknowledged`, `OrderCancelled`, `OrderRejected`, `ExecutionRoutedEvent` |
| `fills` | `FillEvent` |
| `alerts` | `RiskAlertEvent`, `KillSwitchEvent` |
| `logs`      | structlog records (level, logger, message, timestamp, extra fields) |

Position, account, and open-order topics are intentionally **not** streamed —
they are state, served via REST below. The server still subscribes to
`open-orders`, `venue-positions`, and `account` to keep the latest snapshot in
memory for those endpoints.

### REST state endpoints
| Endpoint | Source | Payload |
|----------|--------|---------|
| `GET /state/positions` | Position Engine (live) + cached `venue-positions` | per-strategy `positions[]` **and** `venue_net[]` (exchange ground truth) |
| `GET /state/account` | cached `AccountSnapshotEvent` | wallet `balances[]` |
| `GET /state/open_orders` | cached `OpenOrdersSnapshotEvent` | `exposures[]` (per-side aggregate) **and** `orders[]` (per-order detail) |

CORS is permissive (read-only operator tool). The frontend `useStatePoll` hook
polls these every 2–5 s and dispatches replace-wholesale snapshot actions.

### Wire-in to any stage (two lines)
```python
from trading.monitoring import DashboardServer

dashboard = DashboardServer(bus=bus, port=8765)
# in startup:
await dashboard.start()
# in shutdown:
await dashboard.stop()
```

### JSON message envelope
```json
{
  "topic": "fills",
  "event_type": "FillEvent",
  "timestamp": "2026-05-23T10:00:00.000Z",
  "data": { ...event fields... }
}
```

### Log message envelope
```json
{
  "topic": "logs",
  "event_type": "LogRecord",
  "timestamp": "2026-05-23T10:00:00.000Z",
  "data": {
    "level": "warning",
    "logger": "trading.risk.engine",
    "message": "signal_rejected",
    "extra": { "reason": "max_position_exceeded", "strategy": "momentum" }
  }
}
```

Implemented as a structlog processor injected at `configure_logging()` time — forwards every record to the dashboard broadcast queue without touching any component code.

### Dependencies to add
- `fastapi>=0.111` — WebSocket + REST framework
- `uvicorn[standard]>=0.29` — ASGI server (runs alongside asyncio event loop)

---

## Frontend

### Location
`dashboard/` at project root

### Stack
- **Vite** + **React 18** + **TypeScript**
- **MUI (Material UI) v9** — layout, panels, header, status indicators
- **MUI X DataGrid v9** (`@mui/x-data-grid`, community tier) — orders/fills and position tables
- **Recharts** — PnL line chart
- **React Router v6** — client-side routing

### File structure
```
dashboard/
├── package.json
├── vite.config.ts
├── index.html
└── src/
    ├── main.tsx
    ├── App.tsx                        # router setup, MUI theme provider
    ├── hooks/
    │   ├── usePipelineSocket.ts       # WebSocket connect/reconnect, message dispatch
    │   └── useStatePoll.ts            # polls /state/* REST snapshots on a timer
    ├── store/
    │   └── pipelineStore.ts           # useReducer state (events + snapshots)
    ├── pages/
    │   ├── DashboardPage.tsx          # route: /
    │   ├── LogsPage.tsx               # route: /logs
    │   ├── BacktestPage.tsx           # route: /backtest  (Phase C+)
    │   └── KillSwitchPage.tsx         # route: /killswitch  (Phase C)
    └── components/
        ├── NavBar.tsx                 # MUI AppBar with route links + connection status
        ├── MarketDataPanel.tsx        # latest bid/ask/last per instrument
        ├── SignalsPanel.tsx           # signal + risk decision feed
        ├── OrdersPanel.tsx            # order lifecycle log (all statuses)
        ├── OpenOrdersPanel.tsx        # currently-resting orders (from snapshot)
        ├── PositionPanel.tsx          # exchange net row + per-strategy rows
        ├── PnlChart.tsx               # Recharts line chart of PnL over time
        └── LogViewer.tsx              # filterable log feed (level + logger filters)
```

### Routes

| Route | Page | Phase |
|-------|------|-------|
| `/` | Live pipeline dashboard | A |
| `/logs` | Structured log viewer | A |
| `/killswitch` | Kill switch control panel | C |
| `/backtest` | Backtest results viewer | C+ |

### Layout (single page)
```
┌─────────────────────────────────────────────────────┐
│  QF635 Pipeline Dashboard          ● CONNECTED      │
├──────────────┬──────────────────────────────────────┤
│ Market Data  │  Position & PnL                      │
│              │  ┌─────────────────────────────────┐ │
│ BTC-USDT     │  │  PnL chart (Recharts line)      │ │
│ Bid  Ask     │  └─────────────────────────────────┘ │
│ Last Trade   │  Net qty  Avg price  Unrealised PnL  │
├──────────────┼──────────────────────────────────────┤
│ Signals &    │  Orders & Fills                      │
│ Risk         │                                      │
│ [feed]       │  [feed]                              │
└──────────────┴──────────────────────────────────────┘
```

### Logs page layout (`/logs`)
```
┌─────────────────────────────────────────────────────┐
│  QF635 Pipeline Dashboard          ● CONNECTED       │
│  [Dashboard] [Logs] [Kill Switch] [Backtest]         │
├──────────────────────────────────────────────────────┤
│  Level: [ALL▼]  Logger: [all▼]  [Clear]             │
├───────────┬──────────────────────┬───────────────────┤
│ Timestamp │ Logger               │ Message + extras  │
├───────────┼──────────────────────┼───────────────────┤
│ 10:00:01  │ trading.risk.engine  │ signal_rejected … │
│ 10:00:01  │ trading.oms          │ order_created …   │
│ …         │ …                    │ …                 │
└───────────┴──────────────────────┴───────────────────┘
```
- MUI X DataGrid with level colour coding (DEBUG=grey, INFO=default, WARNING=amber, ERROR=red)
- Level dropdown filter and logger name filter
- Clear button resets the in-memory buffer
- Auto-scrolls to newest row; pause-on-hover

### WebSocket behaviour
- Connects to `ws://localhost:8765/ws` on mount
- Exponential backoff reconnect (1s, 2s, 4s, max 30s) on close/error
- Shows connection status indicator in header (green / amber reconnecting / red)
- No auth — localhost dev tool only

### State model
Event-stream panels (market data, signals, orders, fills, logs) maintain capped
rolling buffers (last ~100) so the page doesn't grow unbounded. Snapshot-backed
panels (positions, account, open orders) are **replaced wholesale** on each
poll — so anything no longer present on the server disappears, which is exactly
the freshness behaviour we want.

- **Positions**: `OpenOrdersPanel` and the per-strategy position rows are
  fill-derived attribution; the **exchange net row** comes from `venue_net`
  (ground truth, comparable to the exchange UI). Per-strategy rows are not
  expected to sum to the net — the venue net includes external/manual activity.
- **Open orders**: rendered from `/state/open_orders` `orders[]`, the
  authoritative resting set — not filtered from the order log (a filtered log
  drifts because fills arrive on a separate topic and never flip order status).
- **PnL chart**: time-series sampled from the polled position snapshot.

---

## Open Questions / Decisions to Make

- [ ] Should the server throttle high-frequency market data ticks before
      forwarding (e.g. forward at most 1 tick/s per instrument) to avoid
      flooding the WebSocket?
- [ ] Rolling buffer size per panel — 100 events? Configurable?
- [x] Should filled orders be visually distinguished from rejected ones with
      colour coding? — yes; order/open-order panels colour-code by status.
- [ ] Dark mode only, or light/dark toggle?
- [ ] Should the PnL chart show unrealised PnL only, or also realised?

---

## Dev Setup (once built)

```bash
# Terminal 1 — backend with dashboard wired in
uv run python -m trading.runners.stage3_risk_oms

# Terminal 2 — frontend dev server
cd dashboard
npm install
npm run dev          # opens http://localhost:5173
```
