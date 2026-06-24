/**
 * WebSocket hook: connects to the DashboardServer, dispatches incoming
 * messages to the pipeline store, and reconnects with exponential backoff.
 */
import { useEffect, useRef, useCallback } from "react";
import { PipelineAction } from "../store/pipelineStore";

const WS_URL = "ws://localhost:8765/ws";
const MAX_BACKOFF_MS = 30_000;
// Minimum ms between TICK dispatches per instrument. Ticks arriving faster
// than this are coalesced (latest wins) so the reducer doesn't run at the
// full market-data rate (can be 100s/sec) and cause heap churn.
const TICK_THROTTLE_MS = 100;
// Log records are batched and dispatched as a single LOGS_BATCH action to
// avoid running the reducer (and triggering a re-render) per log line.
const LOG_BATCH_MS = 200;
const LOG_BATCH_MAX = 50; // flush early if batch grows large

/**
 * Normalise any timestamp the backend may send to milliseconds since epoch.
 *
 * The dashboard_server.py envelope uses ``_now_iso()`` which emits an
 * ISO-8601 string (e.g. "2026-05-23T12:34:56.789+00:00").  Some internal
 * event fields carry nanosecond integers as strings.  Both forms are accepted
 * here so every consumer in the store gets a consistent numeric ms value.
 */
function toMs(raw: string | number | undefined): number {
  if (raw == null) return Date.now();
  const n = Number(raw);
  if (!isNaN(n)) {
    // Heuristic: nanoseconds > year-2000 in ms (9.46e11), so values above
    // 1e15 are treated as nanoseconds, between 1e12..1e15 as microseconds,
    // otherwise as milliseconds.
    if (n > 1e15) return Math.floor(n / 1_000_000);
    if (n > 1e12) return Math.floor(n / 1_000);
    return n;
  }
  // ISO string
  const d = Date.parse(String(raw));
  return isNaN(d) ? Date.now() : d;
}

function parseMessage(raw: string): PipelineAction | null {
  let msg: { topic: string; event_type: string; timestamp: string; data: Record<string, unknown> };
  try {
    msg = JSON.parse(raw);
  } catch {
    return null;
  }

  const { topic, event_type, timestamp: rawTs, data } = msg;
  const ts = toMs(rawTs);                         // normalised ms epoch
  const id = `${ts}-${Math.random()}`;

  switch (topic) {
    case "market-data":
      if (event_type === "tick") {
        return {
          type: "TICK",
          payload: {
            id,
            instrument: String((data.instrument as Record<string,unknown>)?.symbol ?? data.instrument_id ?? ""),
            bid_price: String(data.bid_price ?? ""),
            ask_price: String(data.ask_price ?? ""),
            bid_size: String(data.bid_size ?? ""),
            ask_size: String(data.ask_size ?? ""),
            ts,
          },
        };
      }
      if (event_type === "trade") {
        return {
          type: "TRADE",
          payload: {
            id,
            instrument: String((data.instrument as Record<string,unknown>)?.symbol ?? ""),
            price: String(data.price ?? ""),
            quantity: String(data.quantity ?? ""),
            aggressor_side: data.aggressor_side ? String(data.aggressor_side) : null,
            ts,
          },
        };
      }
      return null;

    case "signals":
      return {
        type: "SIGNAL",
        payload: {
          id,
          ts,
          strategy_id: String(data.strategy_id ?? ""),
          instrument: String((data.instrument as Record<string,unknown>)?.symbol ?? ""),
          legs: ((data.legs as Record<string,unknown>[]) ?? []).map((leg) => ({
            leg_id: String(leg.leg_id ?? ""),
            side: String(leg.side ?? ""),
            quantity: String(leg.quantity ?? ""),
            price: leg.price != null ? String(leg.price) : null,
            order_type: String(leg.order_type ?? ""),
          })),
          rationale: String(data.rationale ?? ""),
        },
      };

    case "risk-decisions":
      return {
        type: "RISK",
        payload: {
          id,
          ts,
          strategy_id: String(data.strategy_id ?? ""),
          approved: Boolean(data.approved),
          rule_name: data.rule_name ? String(data.rule_name) : null,
          reason: String(data.reason ?? ""),
          approved_quantity: data.approved_quantity ? String(data.approved_quantity) : null,
        },
      };

    case "orders": {
      // ExecutionRoutedEvent rides the orders topic but is a routing-decision
      // audit record, not an order lifecycle event — route it to its own row.
      if (event_type === "execution_routed") {
        return {
          type: "ROUTING",
          payload: {
            id,
            ts,
            strategy_id: String(data.strategy_id ?? ""),
            instrument: String((data.instrument as Record<string,unknown>)?.symbol ?? ""),
            leg_id: String(data.leg_id ?? ""),
            side: String(data.side ?? ""),
            intent: String(data.intent ?? ""),
            quantity: String(data.quantity ?? ""),
            algo: String(data.algo ?? ""),
            reason: String(data.reason ?? ""),
          },
        };
      }
      // Map all order lifecycle event_types to a human-readable status label.
      const STATUS_MAP: Record<string, string> = {
        order_request:      "pending",
        order_acknowledged: "open",
        order_rejected:     "rejected",
        order_cancelled:    "cancelled",
        order_filled:       "filled",
        order_partially_filled: "partial",
      };
      return {
        type: "ORDER",
        payload: {
          // One row per order_id — later events (ack, reject, cancel) update
          // the status on the same row via capDedup in the reducer.
          id: String(data.order_id ?? id),
          ts,
          event_type,
          order_id: String(data.order_id ?? ""),
          strategy_id: String(data.strategy_id ?? ""),
          instrument: String((data.instrument as Record<string,unknown>)?.symbol ?? ""),
          side: String(data.side ?? ""),
          order_type: String(data.order_type ?? ""),
          quantity: String(data.quantity ?? ""),
          price: data.price ? String(data.price) : null,
          status: STATUS_MAP[event_type] ?? event_type,
        },
      };
    }

    case "fills":
      return {
        type: "FILL",
        payload: {
          id,
          ts,
          order_id: String(data.order_id ?? ""),
          strategy_id: String(data.strategy_id ?? ""),
          instrument: String((data.instrument as Record<string,unknown>)?.symbol ?? ""),
          side: String(data.side ?? ""),
          fill_price: String(data.fill_price ?? ""),
          fill_quantity: String(data.fill_quantity ?? ""),
          fee: String(data.fee ?? "0"),
          is_maker: data.is_maker != null ? Boolean(data.is_maker) : null,
        },
      };

    // Positions and account state are no longer streamed on the WS —
    // they are served via REST /state/positions and /state/account
    // (see useStatePoll). Any stray events on these topics are ignored.

    case "analytics": {
      const instrument = (data.instrument as Record<string, unknown>)?.symbol
        ?? String(data.instrument ?? "");
      return {
        type: "ANALYTICS",
        payload: {
          ts,
          strategy_id: String(data.strategy_id ?? ""),
          instrument: String(instrument),
          bid_price: Number(data.bid_price ?? 0),
          ask_price: Number(data.ask_price ?? 0),
          bid_size: Number(data.bid_size ?? 0),
          ask_size: Number(data.ask_size ?? 0),
          mid_price: Number(data.mid_price ?? 0),
          microprice: Number(data.microprice ?? 0),
          sigma: Number(data.sigma ?? 0),
          obi: data.obi != null ? Number(data.obi) : null,
          ofi: data.ofi != null ? Number(data.ofi) : null,
          vpin: data.vpin != null ? Number(data.vpin) : null,
          // L2 fields not yet available from WS analytics event (split format); REST poll provides these
          obi_l2: data.obi_l2 != null ? Number(data.obi_l2) : null,
          depth_bid_total: data.depth_bid_total != null ? Number(data.depth_bid_total) : null,
          depth_ask_total: data.depth_ask_total != null ? Number(data.depth_ask_total) : null,
          vpin_widened: Boolean(data.vpin_widened),
          inventory: Number(data.inventory ?? 0),
          reservation_raw: Number(data.reservation_raw ?? 0),
          reservation: Number(data.reservation ?? 0),
          half_spread_raw: Number(data.half_spread_raw ?? 0),
          half_spread: Number(data.half_spread ?? 0),
          bid_quote: data.bid_quote != null ? Number(data.bid_quote) : null,
          ask_quote: data.ask_quote != null ? Number(data.ask_quote) : null,
          buy_guard: Boolean(data.buy_guard),
          sell_guard: Boolean(data.sell_guard),
          n_legs: Number(data.n_legs ?? 0),
        },
      };
    }

    case "alerts": {
      // The alerts topic carries both RiskAlertEvent (WARN/BLOCK — already
      // surfaced via risk-decisions) and KillSwitchEvent. Only the latter
      // drives the kill-switch tab; ignore the rest here.
      if (event_type === "kill_switch") {
        return {
          type: "KILL_SWITCH",
          payload: {
            ts,
            triggered_by: String(data.triggered_by ?? ""),
            reason: String(data.reason ?? ""),
          },
        };
      }
      return null;
    }

    case "logs": {
      const d = data as { level?: string; logger?: string; message?: string; extra?: Record<string,string> };
      return {
        type: "LOG",
        payload: {
          id,
          ts,
          level: d.level ?? "info",
          logger: d.logger ?? "",
          message: d.message ?? "",
          extra: d.extra ?? {},
        },
      };
    }

    case "backtest":
      return {
        type: "BACKTEST_RESULT",
        payload: {
          status: String(data.status ?? "error") as import("../store/pipelineStore").BacktestStatus,
          result: (data.result as import("../store/pipelineStore").BacktestResult) ?? null,
          error: data.error != null ? String(data.error) : null,
        },
      };

    default:
      return null;
  }
}

export function usePipelineSocket(dispatch: React.Dispatch<PipelineAction>): void {
  const wsRef = useRef<WebSocket | null>(null);
  const backoffRef = useRef(1000);
  const unmountedRef = useRef(false);
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Per-instrument throttle: maps instrument -> { lastDispatchMs, pendingTimer, pendingAction }
  const tickThrottleRef = useRef<Map<string, { lastMs: number; timer: ReturnType<typeof setTimeout> | null; pending: PipelineAction }>>(new Map());
  // Log batching: accumulate log rows and flush as a single LOGS_BATCH dispatch.
  const logBatchRef = useRef<{ rows: import("../store/pipelineStore").LogRow[]; timer: ReturnType<typeof setTimeout> | null }>({ rows: [], timer: null });

  const connect = useCallback(() => {
    if (unmountedRef.current) return;

    dispatch({ type: "SET_STATUS", payload: "connecting" });
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      backoffRef.current = 1000;
      dispatch({ type: "SET_STATUS", payload: "connected" });
    };

    ws.onmessage = (e) => {
      const action = parseMessage(e.data as string);
      if (!action) return;

      if (action.type === "TICK") {
        const instrument = action.payload.instrument;
        const now = Date.now();
        const entry = tickThrottleRef.current.get(instrument);

        if (!entry) {
          // First tick for this instrument — dispatch immediately.
          tickThrottleRef.current.set(instrument, { lastMs: now, timer: null, pending: action });
          dispatch(action);
        } else if (now - entry.lastMs >= TICK_THROTTLE_MS) {
          // Enough time has passed — dispatch and clear any pending coalesce timer.
          if (entry.timer !== null) {
            clearTimeout(entry.timer);
            entry.timer = null;
          }
          entry.lastMs = now;
          entry.pending = action;
          dispatch(action);
        } else {
          // Too soon — coalesce: replace pending, schedule a flush if not already pending.
          entry.pending = action;
          if (entry.timer === null) {
            const delay = TICK_THROTTLE_MS - (now - entry.lastMs);
            entry.timer = setTimeout(() => {
              if (unmountedRef.current) return;
              entry.timer = null;
              entry.lastMs = Date.now();
              dispatch(entry.pending);
            }, delay);
          }
        }
        return;
      }

      if (action.type === "LOG") {
        const batch = logBatchRef.current;
        batch.rows.push(action.payload);
        if (batch.rows.length >= LOG_BATCH_MAX) {
          // Batch full — flush immediately.
          if (batch.timer !== null) { clearTimeout(batch.timer); batch.timer = null; }
          const rows = batch.rows;
          batch.rows = [];
          dispatch({ type: "LOGS_BATCH", payload: rows });
        } else if (batch.timer === null) {
          batch.timer = setTimeout(() => {
            if (unmountedRef.current) return;
            batch.timer = null;
            const rows = batch.rows;
            batch.rows = [];
            if (rows.length > 0) dispatch({ type: "LOGS_BATCH", payload: rows });
          }, LOG_BATCH_MS);
        }
        return;
      }

      dispatch(action);
    };

    ws.onclose = () => {
      if (unmountedRef.current || wsRef.current !== ws) return;
      dispatch({ type: "SET_STATUS", payload: "reconnecting" });
      const delay = Math.min(backoffRef.current, MAX_BACKOFF_MS);
      backoffRef.current = Math.min(backoffRef.current * 2, MAX_BACKOFF_MS);
      retryTimerRef.current = setTimeout(connect, delay);
    };

    ws.onerror = () => {
      ws.close();
    };
  }, [dispatch]);

  useEffect(() => {
    unmountedRef.current = false;
    connect();
    return () => {
      unmountedRef.current = true;
      // Cancel any pending reconnect timer so it doesn't fire after unmount.
      if (retryTimerRef.current !== null) {
        clearTimeout(retryTimerRef.current);
        retryTimerRef.current = null;
      }
      // Cancel any pending tick coalesce timers.
      for (const entry of tickThrottleRef.current.values()) {
        if (entry.timer !== null) clearTimeout(entry.timer);
      }
      tickThrottleRef.current.clear();
      // Cancel pending log batch timer.
      if (logBatchRef.current.timer !== null) {
        clearTimeout(logBatchRef.current.timer);
        logBatchRef.current.timer = null;
      }
      logBatchRef.current.rows = [];
      const ws = wsRef.current;
      wsRef.current = null;
      if (ws) {
        ws.onopen = null;
        ws.onmessage = null;
        ws.onclose = null;
        ws.onerror = null;
        ws.close();
      }
    };
  }, [connect]);
}
