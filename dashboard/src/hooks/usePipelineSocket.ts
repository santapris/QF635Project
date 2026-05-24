/**
 * WebSocket hook: connects to the DashboardServer, dispatches incoming
 * messages to the pipeline store, and reconnects with exponential backoff.
 */
import { useEffect, useRef, useCallback } from "react";
import { PipelineAction } from "../store/pipelineStore";

const WS_URL = "ws://localhost:8765/ws";
const MAX_BACKOFF_MS = 30_000;

function parseMessage(raw: string): PipelineAction | null {
  let msg: { topic: string; event_type: string; timestamp: string; data: Record<string, unknown> };
  try {
    msg = JSON.parse(raw);
  } catch {
    return null;
  }

  const { topic, event_type, timestamp: ts, data } = msg;
  const id = `${ts}-${Math.random()}`;

  switch (topic) {
    case "market-data":
      if (event_type === "tick") {
        return {
          type: "TICK",
          payload: {
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

    case "orders":
      return {
        type: "ORDER",
        payload: {
          id,
          ts,
          event_type,
          order_id: String(data.order_id ?? ""),
          strategy_id: String(data.strategy_id ?? ""),
          instrument: String((data.instrument as Record<string,unknown>)?.symbol ?? ""),
          side: String(data.side ?? ""),
          order_type: String(data.order_type ?? ""),
          quantity: String(data.quantity ?? ""),
          price: data.price ? String(data.price) : null,
          status: event_type,
        },
      };

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

    case "positions":
      return {
        type: "POSITION",
        payload: {
          id,
          ts,
          strategy_id: String(data.strategy_id ?? ""),
          instrument: String((data.instrument as Record<string,unknown>)?.symbol ?? ""),
          quantity: String(data.quantity ?? ""),
          average_entry_price: String(data.average_entry_price ?? ""),
          unrealized_pnl: String(data.unrealized_pnl ?? "0"),
          realized_pnl: String(data.realized_pnl ?? "0"),
          mark_price: String(data.mark_price ?? ""),
        },
      };

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
      if (unmountedRef.current) return;
      dispatch({ type: "SET_STATUS", payload: "reconnecting" });
      const delay = Math.min(backoffRef.current, MAX_BACKOFF_MS);
      backoffRef.current = Math.min(backoffRef.current * 2, MAX_BACKOFF_MS);
      setTimeout(connect, delay);
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
      wsRef.current?.close();
    };
  }, [connect]);
}
