/**
 * Control-plane calls for per-strategy pause/resume.
 *
 * These POST to the dashboard server's /command/strategy/* endpoints, which
 * marshal the action onto the trading loop (registry pause + OMS order
 * cancellation on pause). The caller is responsible for any confirmation UX;
 * these helpers just issue the request and surface success/failure.
 */
import { HTTP_BASE } from "./useStatePoll";

async function postStrategyCommand(
  action: "pause" | "resume",
  strategyId: string,
): Promise<void> {
  const res = await fetch(`${HTTP_BASE}/command/strategy/${action}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ strategy_id: strategyId }),
  });
  if (!res.ok) {
    let detail = `${res.status}`;
    try {
      const body = await res.json();
      if (body?.error) detail = String(body.error);
    } catch {
      // non-JSON error body — keep the status code
    }
    throw new Error(detail);
  }
}

export const pauseStrategy = (strategyId: string) =>
  postStrategyCommand("pause", strategyId);

export const resumeStrategy = (strategyId: string) =>
  postStrategyCommand("resume", strategyId);
