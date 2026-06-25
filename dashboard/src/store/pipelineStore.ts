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

/** Flat analytics snapshot merged from two backend sources:
 *  - microstructure fields: always present when AnalyticsService runs
 *  - strategy_diagnostics fields: only present for strategies that opt in (e.g. AS)
 */
export interface AnalyticsSnapshot {
  ts: number;
  instrument: string;
  // --- Microstructure (always present) ---
  bid_price: number;
  ask_price: number;
  bid_size: number;
  ask_size: number;
  mid_price: number;
  microprice: number;
  sigma: number | null;
  obi: number | null;        // L1 OBI (top-of-book)
  ofi: number | null;
  vpin: number | null;
  // --- L2 depth metrics (null until depth stream bootstrapped) ---
  obi_l2: number | null;           // OBI across top-10 levels
  depth_bid_total: number | null;  // total bid size top-10
  depth_ask_total: number | null;  // total ask size top-10
  // --- Strategy diagnostics (null when strategy doesn't opt in) ---
  strategy_id: string | null;
  inventory: number | null;
  reservation_raw: number | null;
  reservation: number | null;
  half_spread_raw: number | null;
  half_spread: number | null;
  bid_quote: number | null;
  ask_quote: number | null;
  buy_guard: boolean | null;
  sell_guard: boolean | null;
  n_legs: number | null;
  vpin_widened: boolean;  // strategy threshold decision; false when no strategy diagnostics
}

export interface AnalyticsPoint {
  ts: number;
  microprice: number;
  mid_price: number;
  sigma: number | null;
  obi: number | null;
  ofi: number | null;
  vpin: number | null;
  obi_l2: number | null;
  depth_bid_total: number | null;
  depth_ask_total: number | null;
  reservation: number | null;
  half_spread: number | null;
  half_spread_raw: number | null;
  bid_quote: number | null;
  ask_quote: number | null;
  inventory: number | null;
  vpin_widened: boolean;
}

export interface BacktestMetrics {
  total_return: number;
  annualized_return: number;
  annualized_volatility: number;
  sharpe_ratio: number;
  sortino_ratio: number;
  max_drawdown: number;
  max_drawdown_pct: number;
  num_trades: number;
  win_rate: number;
  profit_factor: number;
}

export interface BacktestResult {
  config_path: string;
  num_fills: number;
  num_equity_points: number;
  first_fill_ts: number | null;
  last_fill_ts: number | null;
  metrics: BacktestMetrics | null;
  equity_curve: [number, number][]; // [ts_ns, total_pnl]
}

export interface BacktestConfigOption {
  name: string;
  path: string;
}

export type BacktestStatus = "idle" | "running" | "complete" | "error";

export interface BacktestState {
  status: BacktestStatus;
  result: BacktestResult | null;
  error: string | null;
  started_at: string | null;
  completed_at: string | null;
}

const _BACKTEST_INITIAL: BacktestState = {
  status: "idle", result: null, error: null, started_at: null, completed_at: null,
};

export interface StageLatencyData {
  p50_ms: number | null;
  p95_ms: number | null;
  p99_ms: number | null;
  count: number;
}

export interface LatencySnapshot {
  ts: number;
  tick_to_signal:     StageLatencyData | null;
  signal_to_decision: StageLatencyData | null;
  decision_to_order:  StageLatencyData | null;
  order_to_fill:      StageLatencyData | null;
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
  analytics: AnalyticsSnapshot | null;      // latest analytics snapshot (for gauges)
  analyticsHistory: AnalyticsPoint[];       // rolling 300, 250ms-sampled (for charts)
  _lastAnalyticsSampleMs: number;           // wall-clock ms of last analyticsHistory sample
  backtest: BacktestState;                  // event-driven pair backtest run state
  latency: LatencySnapshot | null;          // latest pipeline latency percentiles
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
  analytics: null,
  analyticsHistory: [],
  _lastAnalyticsSampleMs: 0,
  backtest: _BACKTEST_INITIAL,
  latency: null,
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
  | { type: "LOGS_BATCH"; payload: LogRow[] }
  | { type: "CLEAR_LOGS" }
  | { type: "ANALYTICS"; payload: AnalyticsSnapshot }
  | { type: "BACKTEST_RESULT"; payload: { status: BacktestStatus; result: BacktestResult | null; error: string | null } }
  | { type: "LATENCY_SNAPSHOT"; payload: { ts: number; stages: Record<string, StageLatencyData | undefined> } };

/** Minimum wall-clock interval between PnL chart samples (1 second). */
const PNL_SAMPLE_INTERVAL_MS = 1_000;
/** Maximum PnL history points retained in memory. */
const PNL_HISTORY_LIMIT = 500;
/** Minimum wall-clock interval between analytics chart samples (250ms → 4 Hz). */
const ANALYTICS_SAMPLE_INTERVAL_MS = 250;
/** Maximum analytics history points retained in memory (~75s window at 4 Hz). */
const ANALYTICS_HISTORY_LIMIT = 300;

// Prepend item and trim to limit in one allocation.
function cap<T>(arr: T[], item: T, limit: number): T[] {
  if (arr.length < limit) return [item, ...arr];
  const next = new Array<T>(limit);
  next[0] = item;
  for (let i = 1; i < limit; i++) next[i] = arr[i - 1];
  return next;
}

function capDedup<T extends { id: string }>(arr: T[], item: T, limit: number): T[] {
  const filtered = arr.filter((x) => x.id !== item.id);
  if (filtered.length < limit) return [item, ...filtered];
  const next = new Array<T>(limit);
  next[0] = item;
  for (let i = 1; i < limit; i++) next[i] = filtered[i - 1];
  return next;
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

    case "LOGS_BATCH": {
      if (action.payload.length === 0) return state;
      // Prepend the batch (newest first) then trim to limit in one pass.
      const combined = [...action.payload, ...state.logs];
      return { ...state, logs: combined.length > 500 ? combined.slice(0, 500) : combined };
    }

    case "CLEAR_LOGS":
      return { ...state, logs: [] };

    case "ANALYTICS": {
      const snap = action.payload;
      const nowMs = Date.now();
      const shouldSample = nowMs - state._lastAnalyticsSampleMs >= ANALYTICS_SAMPLE_INTERVAL_MS;

      const point: AnalyticsPoint = {
        ts: snap.ts,
        microprice: snap.microprice,
        mid_price: snap.mid_price,
        sigma: snap.sigma,
        obi: snap.obi,
        ofi: snap.ofi,
        vpin: snap.vpin,
        obi_l2: snap.obi_l2,
        depth_bid_total: snap.depth_bid_total,
        depth_ask_total: snap.depth_ask_total,
        reservation: snap.reservation,
        half_spread: snap.half_spread,
        half_spread_raw: snap.half_spread_raw,
        bid_quote: snap.bid_quote,
        ask_quote: snap.ask_quote,
        inventory: snap.inventory,
        vpin_widened: snap.vpin_widened,
      };

      const nextHistory = shouldSample
        ? (state.analyticsHistory.length >= ANALYTICS_HISTORY_LIMIT
            ? [...state.analyticsHistory.slice(-(ANALYTICS_HISTORY_LIMIT - 1)), point]
            : [...state.analyticsHistory, point])
        : state.analyticsHistory;

      return {
        ...state,
        analytics: snap,
        analyticsHistory: nextHistory,
        _lastAnalyticsSampleMs: shouldSample ? nowMs : state._lastAnalyticsSampleMs,
      };
    }

    case "BACKTEST_RESULT":
      return {
        ...state,
        backtest: {
          ...state.backtest,
          status: action.payload.status,
          result: action.payload.result,
          error: action.payload.error,
          completed_at: new Date().toISOString(),
        },
      };

    case "LATENCY_SNAPSHOT": {
      const { ts, stages } = action.payload;
      return {
        ...state,
        latency: {
          ts,
          tick_to_signal:     stages["tick_to_signal"]     ?? null,
          signal_to_decision: stages["signal_to_decision"] ?? null,
          decision_to_order:  stages["decision_to_order"]  ?? null,
          order_to_fill:      stages["order_to_fill"]      ?? null,
        },
      };
    }

    default:
      return state;
  }
}
