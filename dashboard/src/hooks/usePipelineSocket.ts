/**
 * WebSocket hook: connects to the DashboardServer, dispatches incoming
 * messages to the pipeline store, and reconnects with exponential backoff.
 */
import { useEffect, useRef, useCallback } from "react";
import { PipelineAction } from "../store/pipelineStore";

const WS_URL = "ws://localhost:8765/ws";
const MAX_BACKOFF_MS = 30_000;

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
          side: String(data.side ?? ""),
          target_quantity: String(data.target_quantity ?? ""),
          order_type: String(data.order_type ?? ""),
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

    default:
      return null;
  }
}

export function usePipelineSocket(dispatch: React.Dispatch<PipelineAction>): void {
  const wsRef = useRef<WebSocket | null>(null);
  const backoffRef = useRef(1000);
  const unmountedRef = useRef(false);
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

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
      if (action) dispatch(action);
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
