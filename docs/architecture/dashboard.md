# Dashboard Implementation Plan

## Overview

A real-time pipeline visualizer: a small WebSocket bridge in the Python backend
broadcasts bus events and structured log records as JSON; a React frontend renders
them across four panels plus a dedicated log viewer. The server is standalone —
any stage wires it in with two lines, and it can be added or removed without
touching existing runner logic.

---

## Architecture

```
AsyncioBus
    │
    ▼
DashboardServer          (trading.monitoring.dashboard_server)
  FastAPI + uvicorn      ws://localhost:8765/ws
                         POST /api/killswitch  (Phase C)
    │
    ▼
React App                (dashboard/)
  usePipelineSocket      reconnects automatically on drop
    │
    ├── MarketDataPanel
    ├── SignalsPanel
    ├── OrdersPanel
    ├── PositionPanel + PnlChart
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

### Topics subscribed
| Topic | Events forwarded |
|-------|-----------------|
| `market-data` | `TickEvent`, `TradeEvent` |
| `signals` | `SignalEvent` |
| `risk-decisions` | `RiskDecision` |
| `orders` | `OrderRequest`, `OrderAcknowledged`, `OrderCancelled`, `OrderRejected` |
| `fills` | `FillEvent` |
| `positions` | `PositionUpdateEvent` |
| `logs`      | structlog records (level, logger, message, timestamp, extra fields) |

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
    │   └── usePipelineSocket.ts       # WebSocket connect/reconnect, message dispatch
    ├── store/
    │   └── pipelineStore.ts           # lightweight state (useReducer or zustand)
    ├── pages/
    │   ├── DashboardPage.tsx          # route: /
    │   ├── LogsPage.tsx               # route: /logs
    │   ├── BacktestPage.tsx           # route: /backtest  (Phase C+)
    │   └── KillSwitchPage.tsx         # route: /killswitch  (Phase C)
    └── components/
        ├── NavBar.tsx                 # MUI AppBar with route links + connection status
        ├── MarketDataPanel.tsx        # latest bid/ask/last per instrument
        ├── SignalsPanel.tsx           # signal + risk decision feed
        ├── OrdersPanel.tsx            # order + fill event feed
        ├── PositionPanel.tsx          # net qty, avg price, unrealised PnL table
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
Each panel maintains a capped rolling buffer (e.g. last 100 events) so the
page doesn't grow unbounded. PnL chart keeps a time-series array of
`{ timestamp, unrealised_pnl }` points sampled from `PositionUpdateEvent`.

---

## Open Questions / Decisions to Make

- [ ] Should the server throttle high-frequency market data ticks before
      forwarding (e.g. forward at most 1 tick/s per instrument) to avoid
      flooding the WebSocket?
- [ ] Rolling buffer size per panel — 100 events? Configurable?
- [ ] Should filled orders be visually distinguished from rejected ones with
      colour coding?
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
