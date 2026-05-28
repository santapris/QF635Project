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
const HTTP_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8765";

const POSITIONS_INTERVAL_MS   = 2_000;
const ACCOUNT_INTERVAL_MS     = 5_000;
const OPEN_ORDERS_INTERVAL_MS = 2_000;

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
  }>;
}

interface AccountResponse {
  timestamp: string;
  balances: Array<{ asset: string; free: string; locked: string }>;
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

    const tickPositions = async () => {
      const data = await fetchJSON<PositionsResponse>("/state/positions", ctrl.signal);
      if (data && !ctrl.signal.aborted) {
        dispatch({
          type: "POSITIONS_SNAPSHOT",
          payload: data.positions.map((p) => ({
            id: `${p.strategy_id}:${p.instrument}`,
            ts: Date.parse(data.timestamp) || Date.now(),
            strategy_id: p.strategy_id,
            instrument: p.instrument,
            quantity: p.quantity,
            average_entry_price: p.average_entry_price,
            unrealized_pnl: p.unrealized_pnl,
            realized_pnl: p.realized_pnl,
            mark_price: p.mark_price,
          })),
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
          payload: data.exposures.map((e) => ({
            id: `${e.strategy_id}:${e.instrument}`,
            strategy_id: e.strategy_id,
            instrument: e.instrument,
            working_buy: e.working_buy,
            working_sell: e.working_sell,
            open_order_count: e.open_order_count,
          })),
        });
      }
      if (!ctrl.signal.aborted) {
        openOrdersTimer = setTimeout(tickOpenOrders, OPEN_ORDERS_INTERVAL_MS);
      }
    };

    // Kick all polls immediately so the panels populate on first paint.
    tickPositions();
    tickAccount();
    tickOpenOrders();

    return () => {
      ctrl.abort();
      if (positionsTimer)  clearTimeout(positionsTimer);
      if (accountTimer)    clearTimeout(accountTimer);
      if (openOrdersTimer) clearTimeout(openOrdersTimer);
    };
  }, [dispatch]);
}
