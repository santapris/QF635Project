/**
 * Central pipeline state store using React's useReducer pattern.
 * Each panel reads from this store via context; the WebSocket hook writes to it.
 */

export type ConnectionStatus = "connecting" | "connected" | "reconnecting" | "disconnected";

export interface TickData {
  id: string;
  instrument: string;
  bid_price: string;
  ask_price: string;
  bid_size: string;
  ask_size: string;
  ts: number;  // ms since epoch
}

export interface TradeData {
  id: string;
  instrument: string;
  price: string;
  quantity: string;
  aggressor_side: string | null;
  ts: number;  // ms since epoch
}

export interface OrderLeg {
  leg_id: string;
  side: string;
  quantity: string;
  price: string | null;
  order_type: string;
}

export interface SignalRow {
  id: string;
  ts: number;  // ms since epoch
  strategy_id: string;
  instrument: string;
  legs: OrderLeg[];
  rationale: string;
}

export interface RiskRow {
  id: string;
  ts: number;  // ms since epoch
  strategy_id: string;
  approved: boolean;
  rule_name: string | null;
  reason: string;
  approved_quantity: string | null;
}

export interface OrderRow {
  id: string;
  ts: number;  // ms since epoch
  event_type: string;
  order_id: string;
  strategy_id: string;
  instrument: string;
  side: string;
  order_type: string;
  quantity: string;
  price: string | null;
  status: string;
}

export interface FillRow {
  id: string;
  ts: number;  // ms since epoch
  order_id: string;
  strategy_id: string;
  instrument: string;
  side: string;
  fill_price: string;
  fill_quantity: string;
  fee: string;
  is_maker: boolean | null;
}

export interface RoutingRow {
  id: string;
  ts: number;  // ms since epoch
  strategy_id: string;
  instrument: string;
  leg_id: string;
  side: string;
  intent: string;
  quantity: string;
  algo: string;
  reason: string;
}

export interface WorkingExposureRow {
  id: string;          // `${strategy_id}:${instrument}`
  strategy_id: string;
  instrument: string;
  working_buy: string;
  working_sell: string;
  open_order_count: number;
}

export interface OpenOrderRow {
  id: string;          // order_id
  ts: number;          // ms since epoch (created_at)
  order_id: string;
  strategy_id: string;
  instrument: string;
  side: string;
  order_type: string;
  quantity: string;
  leaves_quantity: string;
  price: string | null;
  status: string;
}

export interface PositionRow {
  id: string;
  strategy_id: string;
  instrument: string;
  quantity: string;
  average_entry_price: string;
  unrealized_pnl: string;
  realized_pnl: string;
  mark_price: string;
  ts: number;  // ms since epoch
}

// Exchange-reported net position per instrument — ground truth, comparable
// to the exchange UI. Distinct from the per-strategy (fill-derived) rows.
export interface VenueNetRow {
  id: string;          // instrument
  instrument: string;
  net_quantity: string;
  entry_price: string;
  mark_price: string;
  unrealized_pnl: string;
}

export interface PnlPoint {
  ts: number;  // ms since epoch
  unrealized_pnl: number;
  realized_pnl: number;
}

export interface AccountBalance {
  asset: string;
  free: string;
  locked: string;
}

export interface AccountSnapshot {
  ts: number;  // ms since epoch
  balances: AccountBalance[];
}

export interface LogRow {
  id: string;
  ts: number;  // ms since epoch
  level: string;
  logger: string;
  message: string;
  extra: Record<string, string>;
}

export interface PipelineState {
  status: ConnectionStatus;
  ticks: Record<string, TickData>;          // instrument -> latest tick
  tickHistory: TickData[];                  // rolling 200 — for tape view
  recentTrades: TradeData[];                // rolling 50
  signals: SignalRow[];                     // rolling 100
  riskDecisions: RiskRow[];                 // rolling 100
  orders: OrderRow[];                       // rolling 100
  fills: FillRow[];                         // rolling 100
  routings: RoutingRow[];                   // rolling 100 — OMS execution-routing decisions
  openExposures: WorkingExposureRow[];      // working-order exposure per (strategy, instrument)
  openOrders: OpenOrderRow[];               // individual currently-resting orders (authoritative snapshot)
  positions: Record<string, PositionRow>;   // instrument -> latest position
  venueNet: VenueNetRow[];                  // exchange-reported net per instrument (ground truth)
  pnlHistory: PnlPoint[];                   // time-series for chart, rolling 500
  account: AccountSnapshot | null;          // latest exchange account snapshot
  logs: LogRow[];                           // rolling 500
  _lastPnlSampleMs: number;                 // wall-clock ms of last pnlHistory sample
}

export const initialState: PipelineState = {
  status: "connecting",
  ticks: {},
  tickHistory: [],
  recentTrades: [],
  signals: [],
  riskDecisions: [],
  orders: [],
  fills: [],
  routings: [],
  openExposures: [],
  openOrders: [],
  positions: {},
  venueNet: [],
  pnlHistory: [],
  account: null,
  logs: [],
  _lastPnlSampleMs: 0,
};

export type PipelineAction =
  | { type: "SET_STATUS"; payload: ConnectionStatus }
  | { type: "TICK"; payload: TickData }
  | { type: "TRADE"; payload: TradeData }
  | { type: "SIGNAL"; payload: SignalRow }
  | { type: "RISK"; payload: RiskRow }
  | { type: "ORDER"; payload: OrderRow }
  | { type: "FILL"; payload: FillRow }
  | { type: "ROUTING"; payload: RoutingRow }
  | { type: "OPEN_ORDERS_SNAPSHOT"; payload: { exposures: WorkingExposureRow[]; orders: OpenOrderRow[] } }
  | { type: "POSITIONS_SNAPSHOT"; payload: { positions: PositionRow[]; venueNet: VenueNetRow[] } }
  | { type: "ACCOUNT"; payload: AccountSnapshot }
  | { type: "LOG"; payload: LogRow }
  | { type: "CLEAR_LOGS" };

/** Minimum wall-clock interval between PnL chart samples (1 second). */
const PNL_SAMPLE_INTERVAL_MS = 1_000;
/** Maximum PnL history points retained in memory. */
const PNL_HISTORY_LIMIT = 500;

function cap<T>(arr: T[], item: T, limit: number): T[] {
  const next = [item, ...arr];
  return next.length > limit ? next.slice(0, limit) : next;
}

function capDedup<T extends { id: string }>(arr: T[], item: T, limit: number): T[] {
  const filtered = arr.filter((x) => x.id !== item.id);
  const next = [item, ...filtered];
  return next.length > limit ? next.slice(0, limit) : next;
}

export function pipelineReducer(
  state: PipelineState,
  action: PipelineAction
): PipelineState {
  switch (action.type) {
    case "SET_STATUS":
      return { ...state, status: action.payload };

    case "TICK":
      return {
        ...state,
        ticks: { ...state.ticks, [action.payload.instrument]: action.payload },
        tickHistory: cap(state.tickHistory, action.payload, 200),
      };

    case "TRADE":
      return { ...state, recentTrades: cap(state.recentTrades, action.payload, 50) };

    case "SIGNAL":
      return { ...state, signals: cap(state.signals, action.payload, 100) };

    case "RISK":
      return { ...state, riskDecisions: cap(state.riskDecisions, action.payload, 100) };

    case "ORDER": {
      // Merge with existing row so later events (ack, reject, cancel) update
      // only the status/ts while preserving instrument/side/quantity from the
      // original order_request (which carries the full order details).
      const incoming = action.payload;
      const existing = state.orders.find((o) => o.id === incoming.id);
      const merged: OrderRow = existing
        ? {
            ...existing,
            ts: incoming.ts,
            event_type: incoming.event_type,
            status: incoming.status,
          }
        : incoming;
      return { ...state, orders: capDedup(state.orders, merged, 100) };
    }

    case "FILL": {
      // Fills are recorded in the fills list; PnL sampling is driven by POSITION
      // updates which carry authoritative unrealized + realized figures.
      return { ...state, fills: cap(state.fills, action.payload, 100) };
    }

    case "ROUTING": {
      return { ...state, routings: cap(state.routings, action.payload, 100) };
    }

    case "OPEN_ORDERS_SNAPSHOT": {
      // Replace both views wholesale — snapshot semantics. Anything absent
      // from the new snapshot is no longer open and drops off. This is the
      // authoritative source, so a missed event can't leave a stale row.
      return {
        ...state,
        openExposures: action.payload.exposures,
        openOrders: action.payload.orders,
      };
    }

    case "POSITIONS_SNAPSHOT": {
      // Replace the positions map and venue-net list outright. Anything not
      // in the new snapshot has gone flat on the server side and should
      // disappear — that's exactly the snapshot semantics we want.
      const rows = action.payload.positions;
      const venueNet = action.payload.venueNet;
      const updatedPositions: Record<string, PositionRow> = {};
      for (const row of rows) {
        updatedPositions[row.instrument] = row;
      }

      const nowMs = Date.now();
      const shouldSample = nowMs - state._lastPnlSampleMs >= PNL_SAMPLE_INTERVAL_MS;
      if (!shouldSample) {
        return { ...state, positions: updatedPositions, venueNet };
      }

      const totalUnrealized = rows.reduce(
        (sum: number, p: PositionRow) => sum + (parseFloat(p.unrealized_pnl) || 0), 0
      );
      const totalRealized = rows.reduce(
        (sum: number, p: PositionRow) => sum + (parseFloat(p.realized_pnl) || 0), 0
      );
      const pnlPoint: PnlPoint = {
        ts: nowMs,
        unrealized_pnl: totalUnrealized,
        realized_pnl: totalRealized,
      };
      const nextPnlHistory = state.pnlHistory.length >= PNL_HISTORY_LIMIT
        ? [...state.pnlHistory.slice(-(PNL_HISTORY_LIMIT - 1)), pnlPoint]
        : [...state.pnlHistory, pnlPoint];

      return {
        ...state,
        positions: updatedPositions,
        venueNet,
        pnlHistory: nextPnlHistory,
        _lastPnlSampleMs: nowMs,
      };
    }

    case "ACCOUNT":
      return { ...state, account: action.payload };

    case "LOG":
      return { ...state, logs: cap(state.logs, action.payload, 500) };

    case "CLEAR_LOGS":
      return { ...state, logs: [] };

    default:
      return state;
  }
}
