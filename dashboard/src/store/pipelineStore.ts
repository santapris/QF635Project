/**
 * Central pipeline state store using React's useReducer pattern.
 * Each panel reads from this store via context; the WebSocket hook writes to it.
 */

export type ConnectionStatus = "connecting" | "connected" | "reconnecting" | "disconnected";

export interface TickData {
  instrument: string;
  bid_price: string;
  ask_price: string;
  bid_size: string;
  ask_size: string;
  ts: string;
}

export interface TradeData {
  instrument: string;
  price: string;
  quantity: string;
  aggressor_side: string | null;
  ts: string;
}

export interface SignalRow {
  id: string;
  ts: string;
  strategy_id: string;
  instrument: string;
  side: string;
  target_quantity: string;
  order_type: string;
  rationale: string;
}

export interface RiskRow {
  id: string;
  ts: string;
  strategy_id: string;
  approved: boolean;
  rule_name: string | null;
  reason: string;
  approved_quantity: string | null;
}

export interface OrderRow {
  id: string;
  ts: string;
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
  ts: string;
  order_id: string;
  strategy_id: string;
  instrument: string;
  side: string;
  fill_price: string;
  fill_quantity: string;
  fee: string;
  is_maker: boolean | null;
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
  ts: string;
}

export interface PnlPoint {
  ts: string;
  unrealized_pnl: number;
  realized_pnl: number;
}

export interface LogRow {
  id: string;
  ts: string;
  level: string;
  logger: string;
  message: string;
  extra: Record<string, string>;
}

export interface PipelineState {
  status: ConnectionStatus;
  ticks: Record<string, TickData>;          // instrument -> latest tick
  recentTrades: TradeData[];                // rolling 50
  signals: SignalRow[];                     // rolling 100
  riskDecisions: RiskRow[];                 // rolling 100
  orders: OrderRow[];                       // rolling 100
  fills: FillRow[];                         // rolling 100
  positions: Record<string, PositionRow>;   // instrument -> latest position
  pnlHistory: PnlPoint[];                   // time-series for chart
  logs: LogRow[];                           // rolling 500
}

export const initialState: PipelineState = {
  status: "connecting",
  ticks: {},
  recentTrades: [],
  signals: [],
  riskDecisions: [],
  orders: [],
  fills: [],
  positions: {},
  pnlHistory: [],
  logs: [],
};

export type PipelineAction =
  | { type: "SET_STATUS"; payload: ConnectionStatus }
  | { type: "TICK"; payload: TickData }
  | { type: "TRADE"; payload: TradeData }
  | { type: "SIGNAL"; payload: SignalRow }
  | { type: "RISK"; payload: RiskRow }
  | { type: "ORDER"; payload: OrderRow }
  | { type: "FILL"; payload: FillRow }
  | { type: "POSITION"; payload: PositionRow }
  | { type: "LOG"; payload: LogRow }
  | { type: "CLEAR_LOGS" };

function cap<T>(arr: T[], item: T, limit: number): T[] {
  const next = [item, ...arr];
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
      };

    case "TRADE":
      return { ...state, recentTrades: cap(state.recentTrades, action.payload, 50) };

    case "SIGNAL":
      return { ...state, signals: cap(state.signals, action.payload, 100) };

    case "RISK":
      return { ...state, riskDecisions: cap(state.riskDecisions, action.payload, 100) };

    case "ORDER":
      return { ...state, orders: cap(state.orders, action.payload, 100) };

    case "FILL": {
      const fill = action.payload;
      const pnlPoint: PnlPoint = {
        ts: fill.ts,
        unrealized_pnl: 0,
        realized_pnl: parseFloat(fill.fee) || 0,
      };
      return {
        ...state,
        fills: cap(state.fills, fill, 100),
        pnlHistory: [...state.pnlHistory, pnlPoint].slice(-200),
      };
    }

    case "POSITION": {
      const pos = action.payload;
      const pnlPoint: PnlPoint = {
        ts: pos.ts,
        unrealized_pnl: parseFloat(pos.unrealized_pnl) || 0,
        realized_pnl: parseFloat(pos.realized_pnl) || 0,
      };
      return {
        ...state,
        positions: { ...state.positions, [pos.instrument]: pos },
        pnlHistory: [...state.pnlHistory, pnlPoint].slice(-200),
      };
    }

    case "LOG":
      return { ...state, logs: cap(state.logs, action.payload, 500) };

    case "CLEAR_LOGS":
      return { ...state, logs: [] };

    default:
      return state;
  }
}
