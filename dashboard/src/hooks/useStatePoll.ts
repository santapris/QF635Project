/**
 * Polls REST snapshot endpoints for state-of-the-world data.
 *
 * Positions and account balances are *state*, not events — they exist in
 * memory in the trading process regardless of when the dashboard opens.
 * Streaming them on the WS would couple state freshness to the WS
 * lifecycle (initial connect, refresh, reconnects) and force the server
 * to replay snapshots. REST is the simpler model: the dashboard asks for
 * what's there, on a schedule.
 *
 * Usage:
 *   useStatePoll(dispatch);  // mounts once near the WS hook
 */
import { useEffect } from "react";
import { PipelineAction } from "../store/pipelineStore";

// VITE_API_BASE lets a non-localhost deploy point the dashboard at a
// different host/port without rebuilding. Falls back to localhost for dev.
export const HTTP_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8765";

const POSITIONS_INTERVAL_MS   = 2_000;
const ACCOUNT_INTERVAL_MS     = 5_000;
const OPEN_ORDERS_INTERVAL_MS = 2_000;
const ANALYTICS_INTERVAL_MS   = 500;
const LATENCY_INTERVAL_MS     = 1_000;
const KILLSWITCH_INTERVAL_MS  = 3_000;
const STRATEGIES_INTERVAL_MS  = 2_000;

interface KillSwitchResponse {
  timestamp: string;
  available: boolean;
  engaged: boolean;
  triggered_by?: string;
  reason?: string;
  triggered_at_ns?: number;
}

interface PositionsResponse {
  timestamp: string;
  positions: Array<{
    strategy_id: string;
    instrument: string;
    quantity: string;
    average_entry_price: string;
    realized_pnl: string;
    unrealized_pnl: string;
    mark_price: string;
    paused?: boolean;
  }>;
  venue_net: Array<{
    instrument: string;
    net_quantity: string;
    entry_price: string;
    mark_price: string;
    unrealized_pnl: string;
  }>;
}

interface AccountResponse {
  timestamp: string;
  balances: Array<{ asset: string; free: string; locked: string }>;
}

interface StrategiesResponse {
  timestamp: string;
  available: boolean;
  strategies: Array<{ strategy_id: string; paused: boolean }>;
}

interface OpenOrdersResponse {
  timestamp: string;
  exposures: Array<{
    strategy_id: string;
    instrument: string;
    working_buy: string;
    working_sell: string;
    open_order_count: number;
  }>;
  orders: Array<{
    order_id: string;
    client_order_id: string;
    strategy_id: string;
    instrument: string;
    side: string;
    order_type: string;
    quantity: string;
    leaves_quantity: string;
    price: string | null;
    status: string;
    created_at_ns: number;
  }>;
}

async function fetchJSON<T>(path: string, signal: AbortSignal): Promise<T | null> {
  try {
    const res = await fetch(`${HTTP_BASE}${path}`, { signal });
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    // Network errors or aborts — caller decides what to do.
    return null;
  }
}

export function useStatePoll(dispatch: React.Dispatch<PipelineAction>): void {
  useEffect(() => {
    const ctrl = new AbortController();
    let positionsTimer:  ReturnType<typeof setTimeout> | null = null;
    let accountTimer:    ReturnType<typeof setTimeout> | null = null;
    let openOrdersTimer: ReturnType<typeof setTimeout> | null = null;
    let analyticsTimer:  ReturnType<typeof setTimeout> | null = null;
    let latencyTimer:    ReturnType<typeof setTimeout> | null = null;
    let killSwitchTimer: ReturnType<typeof setTimeout> | null = null;
    let strategiesTimer: ReturnType<typeof setTimeout> | null = null;

    const tickPositions = async () => {
      const data = await fetchJSON<PositionsResponse>("/state/positions", ctrl.signal);
      if (data && !ctrl.signal.aborted) {
        dispatch({
          type: "POSITIONS_SNAPSHOT",
          payload: {
            positions: data.positions.map((p) => ({
              id: `${p.strategy_id}:${p.instrument}`,
              ts: Date.parse(data.timestamp) || Date.now(),
              strategy_id: p.strategy_id,
              instrument: p.instrument,
              quantity: p.quantity,
              average_entry_price: p.average_entry_price,
              unrealized_pnl: p.unrealized_pnl,
              realized_pnl: p.realized_pnl,
              mark_price: p.mark_price,
              paused: p.paused ?? false,
            })),
            venueNet: (data.venue_net ?? []).map((v) => ({
              id: v.instrument,
              instrument: v.instrument,
              net_quantity: v.net_quantity,
              entry_price: v.entry_price,
              mark_price: v.mark_price,
              unrealized_pnl: v.unrealized_pnl,
            })),
          },
        });
      }
      if (!ctrl.signal.aborted) {
        positionsTimer = setTimeout(tickPositions, POSITIONS_INTERVAL_MS);
      }
    };

    const tickAccount = async () => {
      const data = await fetchJSON<AccountResponse>("/state/account", ctrl.signal);
      if (data && !ctrl.signal.aborted) {
        dispatch({
          type: "ACCOUNT",
          payload: {
            ts: Date.parse(data.timestamp) || Date.now(),
            balances: data.balances,
          },
        });
      }
      if (!ctrl.signal.aborted) {
        accountTimer = setTimeout(tickAccount, ACCOUNT_INTERVAL_MS);
      }
    };

    const tickOpenOrders = async () => {
      const data = await fetchJSON<OpenOrdersResponse>("/state/open_orders", ctrl.signal);
      if (data && !ctrl.signal.aborted) {
        dispatch({
          type: "OPEN_ORDERS_SNAPSHOT",
          payload: {
            exposures: data.exposures.map((e) => ({
              id: `${e.strategy_id}:${e.instrument}`,
              strategy_id: e.strategy_id,
              instrument: e.instrument,
              working_buy: e.working_buy,
              working_sell: e.working_sell,
              open_order_count: e.open_order_count,
            })),
            orders: data.orders.map((o) => ({
              id: o.order_id,
              ts: o.created_at_ns / 1_000_000,  // ns -> ms
              order_id: o.order_id,
              strategy_id: o.strategy_id,
              instrument: o.instrument,
              side: o.side,
              order_type: o.order_type,
              quantity: o.quantity,
              leaves_quantity: o.leaves_quantity,
              price: o.price,
              status: o.status,
            })),
          },
        });
      }
      if (!ctrl.signal.aborted) {
        openOrdersTimer = setTimeout(tickOpenOrders, OPEN_ORDERS_INTERVAL_MS);
      }
    };

    const tickAnalytics = async () => {
      const data = await fetchJSON<{
        timestamp: string;
        microstructure: Record<string, unknown> | null;
        strategy_diagnostics: Record<string, unknown> | null;
      }>("/state/analytics", ctrl.signal);

      if (data?.microstructure && !ctrl.signal.aborted) {
        const m = data.microstructure;
        const d = data.strategy_diagnostics;

        // Skip if prices are zero/missing — guards history from poisoning the chart y-axis domain.
        const midPrice = Number(m.mid_price ?? 0);
        if (midPrice === 0) return;

        const instrument = (m.instrument as Record<string, unknown>)?.symbol
          ?? String(m.instrument ?? "");

        dispatch({
          type: "ANALYTICS",
          payload: {
            ts: Date.parse(data.timestamp) || Date.now(),
            instrument: String(instrument),
            // Microstructure fields — always present
            bid_price: Number(m.bid_price ?? 0),
            ask_price: Number(m.ask_price ?? 0),
            bid_size: Number(m.bid_size ?? 0),
            ask_size: Number(m.ask_size ?? 0),
            mid_price: midPrice,
            microprice: Number(m.microprice ?? 0),
            sigma: m.sigma != null ? Number(m.sigma) : null,
            obi: m.obi != null ? Number(m.obi) : null,
            ofi: m.ofi != null ? Number(m.ofi) : null,
            vpin: m.vpin != null ? Number(m.vpin) : null,
            // L2 depth metrics — null until BinanceL2Feed bootstrapped
            obi_l2: m.obi_l2 != null ? Number(m.obi_l2) : null,
            depth_bid_total: m.depth_bid_total != null ? Number(m.depth_bid_total) : null,
            depth_ask_total: m.depth_ask_total != null ? Number(m.depth_ask_total) : null,
            // Strategy diagnostics fields — null when strategy doesn't opt in
            strategy_id: d ? String(d.strategy_id ?? "") : null,
            inventory: d?.inventory != null ? Number(d.inventory) : null,
            reservation_raw: d?.reservation_raw != null ? Number(d.reservation_raw) : null,
            reservation: d?.reservation != null ? Number(d.reservation) : null,
            half_spread_raw: d?.half_spread_raw != null ? Number(d.half_spread_raw) : null,
            half_spread: d?.half_spread != null ? Number(d.half_spread) : null,
            bid_quote: d?.bid_quote != null ? Number(d.bid_quote) : null,
            ask_quote: d?.ask_quote != null ? Number(d.ask_quote) : null,
            buy_guard: d ? Boolean(d.buy_guard) : null,
            sell_guard: d ? Boolean(d.sell_guard) : null,
            n_legs: d?.n_legs != null ? Number(d.n_legs) : null,
            vpin_widened: d ? Boolean(d.vpin_widened) : false,
          },
        });
      }
      if (!ctrl.signal.aborted) {
        analyticsTimer = setTimeout(tickAnalytics, ANALYTICS_INTERVAL_MS);
      }
    };

    const tickLatency = async () => {
      const data = await fetchJSON<{
        timestamp: string;
        stages: Record<string, { p50_ms: number | null; p95_ms: number | null; p99_ms: number | null; count: number } | undefined>;
      }>("/state/latency", ctrl.signal);
      if (data && !ctrl.signal.aborted) {
        dispatch({ type: "LATENCY_SNAPSHOT", payload: { ts: Date.parse(data.timestamp) || Date.now(), stages: data.stages } });
      }
      if (!ctrl.signal.aborted) {
        latencyTimer = setTimeout(tickLatency, LATENCY_INTERVAL_MS);
      }
    };

    const tickKillSwitch = async () => {
      const data = await fetchJSON<KillSwitchResponse>("/state/killswitch", ctrl.signal);
      if (data && !ctrl.signal.aborted) {
        dispatch({
          type: "KILL_SWITCH_SNAPSHOT",
          payload: {
            available: data.available,
            engaged: data.engaged,
            triggered_by: data.triggered_by ?? "",
            reason: data.reason ?? "",
            ts: data.triggered_at_ns ? data.triggered_at_ns / 1_000_000 : null,  // ns -> ms
          },
        });
      }
      if (!ctrl.signal.aborted) {
        killSwitchTimer = setTimeout(tickKillSwitch, KILLSWITCH_INTERVAL_MS);
      }
    };

    const tickStrategies = async () => {
      const data = await fetchJSON<StrategiesResponse>("/state/strategies", ctrl.signal);
      if (data && !ctrl.signal.aborted) {
        dispatch({ type: "STRATEGIES_SNAPSHOT", payload: data.strategies ?? [] });
      }
      if (!ctrl.signal.aborted) {
        strategiesTimer = setTimeout(tickStrategies, STRATEGIES_INTERVAL_MS);
      }
    };

    // Kick all polls immediately so the panels populate on first paint.
    tickPositions();
    tickAccount();
    tickOpenOrders();
    tickAnalytics();
    tickLatency();
    tickKillSwitch();
    tickStrategies();

    return () => {
      ctrl.abort();
      if (positionsTimer)  clearTimeout(positionsTimer);
      if (accountTimer)    clearTimeout(accountTimer);
      if (openOrdersTimer) clearTimeout(openOrdersTimer);
      if (analyticsTimer)  clearTimeout(analyticsTimer);
      if (latencyTimer)    clearTimeout(latencyTimer);
      if (killSwitchTimer) clearTimeout(killSwitchTimer);
      if (strategiesTimer) clearTimeout(strategiesTimer);
    };
  }, [dispatch]);
}
