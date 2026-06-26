/**
 * Derives architecture-graph activity from the live pipeline store.
 *
 * The store keeps a monotonic per-stream tally (`state.eventCounts`) that is
 * bumped once per event and NEVER capped. This hook turns that into the two
 * things the architecture page needs:
 *
 *   - `activity`: a per-driver counter that rises by one visual packet per
 *     `packetEvery` real events. The store's tally is the source of truth, so
 *     this counts EXACTLY how many events flowed — even on fast streams whose
 *     capped display lists evict rows between renders.
 *   - `lastSeen`: per-driver ms-epoch of the most recent event, used to render a
 *     freshness ring (live / idle / stale).
 *
 * Nothing here mutates the store; it is a pure projection.
 */
import { useEffect, useRef, useState } from "react";
import type { PipelineState, EventCounts } from "../store/pipelineStore";

export type NodeId =
  | "exchange"
  | "feed"
  | "bus"
  | "strategy"
  | "risk"
  | "oms"
  | "gateway"
  | "position"
  | "analytics"
  | "killswitch";

/**
 * Edges are driven by a flow, which is usually a node but can be a finer-
 * grained sub-stream. The gateway sits on TWO independent flows — orders going
 * out and fills coming back — so it gets two drivers that pulse separately
 * (an order doesn't imply a fill). These extra ids drive edges only; they are
 * not rendered as nodes.
 */
export type DriverId = NodeId | "gateway-orders" | "gateway-fills";

export interface NodePulse {
  /** Rises by 1 each time this node sees a new event. Drives node-box pulses. */
  activity: number;
  /**
   * Flow INTENSITY in [0,1] — smoothed throughput normalised against a reference
   * "busy" rate (REF_RATE). 0 = idle, 1 = at or above REF_RATE. The edge renders
   * a continuous dot stream whose speed + density scale with this, so a busy edge
   * reads as fast/dense and a quiet one slow/sparse — legible at any rate, unlike
   * one-dot-per-event which overlaps into a blob past ~10/s. Unit-free so all
   * downstream tuning (edge speed/density, the flow-gain slider) is in 0–1 terms.
   */
  rate: number;
  /** ms-epoch of the most recent event, or null if never seen. */
  lastSeen: number | null;
}

export type FlowPulse = Record<DriverId, NodePulse>;

/** ms after which a node is considered "idle" then "stale". */
export const FRESH_MS = 1_500;
export const STALE_MS = 8_000;

/**
 * Default for how many REAL events map to one visual packet. Real events can
 * arrive 100s/sec; instead of throttling by time, we emit one packet per this
 * many events per node, so a busy edge visibly pulses faster than a quiet one —
 * the animation rate tracks actual throughput. The unexpressed remainder is
 * carried across renders, so the cumulative packet count is exactly
 * floor(total events / packetEvery) — the true event rate divided by the ratio,
 * with no grouping or dedup beyond that. `lastSeen` still updates on every
 * event, so node freshness and edge "live" state stay accurate. The page
 * exposes this as a live slider (see useFlowPulse's packetEvery argument).
 */
export const PACKET_EVERY = 10;

interface NodeSignal {
  /** Monotonic total of real events on this driver's stream(s), from the store. */
  count: number;
  /** ms-epoch of the most recent event, or null if none. */
  ts: number | null;
}

function latestTs(rows: { ts: number }[]): number | null {
  // Lists are stored newest-first, so element 0 is the most recent.
  return rows.length > 0 ? rows[0].ts : null;
}

/**
 * Map the store's monotonic event tally to per-driver signals. Each driver's
 * `count` is the sum of its underlying stream tallies — fast streams (ticks)
 * and slow ones (fills) alike are counted exactly, because the tally is never
 * capped or deduped. The Event Bus is synthetic: it carries every event, so its
 * count is the sum of all stream counts that flow across the hub.
 *
 * `ts` still comes from the display lists (newest-first), which is fine — we
 * only need the most-recent timestamp for the freshness ring, not a count.
 */
function project(state: PipelineState): Record<DriverId, NodeSignal> {
  const c: EventCounts = state.eventCounts;

  const feedTs = Math.max(latestTs(state.tickHistory) ?? 0, latestTs(state.recentTrades) ?? 0) || null;
  const signalTs = latestTs(state.signals);
  const riskTs = latestTs(state.riskDecisions);
  const orderTs = latestTs(state.orders);
  const fillTs = latestTs(state.fills);
  const routingTs = latestTs(state.routings);
  const posRows = Object.values(state.positions);
  const posTs = posRows.length > 0 ? Math.max(...posRows.map((p) => p.ts)) : null;
  const analyticsTs = state.analytics ? state.analytics.ts : null;

  return {
    exchange:  { count: c.tick + c.fill, ts: Math.max(feedTs ?? 0, fillTs ?? 0) || null },
    feed:      { count: c.tick + c.trade, ts: feedTs },
    bus:       { count: c.tick + c.signal + c.risk + c.order + c.fill, ts: Math.max(feedTs ?? 0, signalTs ?? 0, riskTs ?? 0, orderTs ?? 0, fillTs ?? 0) || null },
    strategy:  { count: c.signal, ts: signalTs },
    risk:      { count: c.risk, ts: riskTs },
    // OMS's own decision stream: routing/execution-routed events. The order
    // publish edge (e-oms-bus) is driven by `gateway-orders` so it matches the
    // OMS→Gateway leg 1:1 — orders are NOT counted here, or they'd inflate the
    // OMS's other (open-orders) edge with the whole order lifecycle.
    oms:       { count: c.routing, ts: Math.max(routingTs ?? 0, orderTs ?? 0) || null },
    gateway:   { count: c.order + c.fill, ts: Math.max(orderTs ?? 0, fillTs ?? 0) || null },
    // Gateway's two independent flows: orders out, fills back. Counted
    // separately so an order packet (OMS→Gateway→Exchange) doesn't imply a fill.
    "gateway-orders": { count: c.order, ts: orderTs },
    "gateway-fills":  { count: c.fill, ts: fillTs },
    position:  { count: c.position, ts: posTs },
    analytics: { count: c.analytics, ts: analyticsTs },
    // Kill switch isn't a high-rate flow — one pulse on engage. No tally; we
    // derive a 0/1 count from the latched state.
    killswitch:{ count: state.killSwitch?.engaged ? 1 : 0, ts: state.killSwitch?.ts ?? null },
  };
}

// Every flow driver (nodes + the gateway sub-flows). Iterated to compute pulses.
const DRIVER_IDS: DriverId[] = [
  "exchange", "feed", "bus", "strategy", "risk",
  "oms", "gateway", "gateway-orders", "gateway-fills", "position", "analytics", "killswitch",
];

function emptyPulse(): FlowPulse {
  return DRIVER_IDS.reduce((acc, id) => {
    acc[id] = { activity: 0, rate: 0, lastSeen: null };
    return acc;
  }, {} as FlowPulse);
}

// Time constant (seconds) for the per-driver rate low-pass. Events arrive in
// ~100ms bursts (one signal/order cycle), so a per-render EMA oscillates at the
// burst period — fast on a burst render, ~0 on the quiet renders between. We
// instead integrate events over CONTINUOUS time with exponential decay, which
// depends only on how many events landed per second, NOT on when renders fire.
// τ ≈ 1s averages over several bursts → steady flow. Larger = smoother/laggier.
const RATE_TAU_SEC = 1.0;

// Reference throughput (events/sec) that maps to intensity 1.0. The smoothed
// per-driver events/sec is divided by this and clamped to [0,1], so the edge
// and slider work in unit-free intensity. ~40/s ≈ a busy signal/order stream;
// anything faster simply saturates at full flow.
const REF_RATE = 40;

export function useFlowPulse(state: PipelineState, packetEvery: number = PACKET_EVERY): FlowPulse {
  const [pulse, setPulse] = useState<FlowPulse>(emptyPulse);
  // Mirror of the latest pulse, read inside the effect so the accounting stays
  // a pure computation (no setState updater function — see the effect comment).
  const pulseRef = useRef<FlowPulse>(pulse);
  // Previous monotonic event count per driver. `undefined` = never seen, so a
  // fresh mount seeds at the current total and counts no backlog (switching
  // tabs away and back doesn't replay every accumulated event as one burst).
  const lastCount = useRef<Record<DriverId, number | undefined>>(
    DRIVER_IDS.reduce((a, id) => ({ ...a, [id]: undefined }), {} as Record<DriverId, number | undefined>)
  );
  // Running remainder of real events not yet expressed as a packet, per driver.
  // We pulse once each time the accumulated event count crosses packetEvery.
  const accrued = useRef<Record<DriverId, number>>(
    DRIVER_IDS.reduce((a, id) => ({ ...a, [id]: 0 }), {} as Record<DriverId, number>)
  );
  // Raw smoothed events/sec per driver (the low-pass state). Kept separate from
  // the exposed pulse, which carries the NORMALISED 0–1 intensity — feeding the
  // normalised value back into the low-pass would corrupt the average.
  const rawRate = useRef<Record<DriverId, number>>(
    DRIVER_IDS.reduce((a, id) => ({ ...a, [id]: 0 }), {} as Record<DriverId, number>)
  );
  // Wall-clock ms of the previous render, for converting fresh-events → rate.
  const lastTickMs = useRef<number>(0);

  useEffect(() => {
    // All accounting happens HERE in the effect body, not inside the setPulse
    // updater. Mutating the lastCount/accrued refs from within a state updater
    // is a side effect, and React StrictMode (dev) invokes updaters twice — the
    // second pass would re-advance the cursors and double-subtract the
    // accumulator, silently eating packets. An effect commits once, so doing the
    // work here is correct. We read the previous pulse from a ref to stay pure.
    const proj = project(state);
    const prev = pulseRef.current;
    const next: FlowPulse = { ...prev };
    let changed = false;

    const nowMs = Date.now();
    // Elapsed since last render, clamped: too-small → div blow-up; too-large
    // (tab was backgrounded) → ignore the gap and just measure this render.
    const elapsedSec = lastTickMs.current === 0
      ? 0
      : Math.min(1, Math.max(0.001, (nowMs - lastTickMs.current) / 1000));
    lastTickMs.current = nowMs;

    for (const id of DRIVER_IDS) {
      const { count, ts } = proj[id];
      // How many genuinely new events arrived since last render — a plain
      // difference of the store's monotonic tally. First sight counts zero.
      const prevCount = lastCount.current[id];
      const fresh = prevCount === undefined ? 0 : Math.max(0, count - prevCount);
      lastCount.current[id] = count;

      // Continuous-time low-pass of events/sec: decay the running raw rate over
      // the ACTUAL elapsed time, then inject this render's events as an impulse.
      // The result is the exponentially-weighted average events-per-second over
      // ~τ seconds — identical whether the events came as one burst or spread
      // out, so the flow no longer alternates fast/slow with the render bursts.
      const decay = elapsedSec > 0 ? Math.exp(-elapsedSec / RATE_TAU_SEC) : 1;
      const raw = rawRate.current[id] * decay + fresh / RATE_TAU_SEC;
      rawRate.current[id] = raw;
      // Expose a unit-free 0–1 intensity: raw events/sec ÷ the reference busy
      // rate, clamped. Quiet edges sit near 0, busy ones saturate at 1.
      const rate = Math.min(1, raw / REF_RATE);

      // `activity` still drives node-box pulses: one pulse per `packetEvery`
      // real events. The edge no longer launches a dot per increment — it uses
      // `rate` for a continuous stream — but the node flash stays event-driven.
      accrued.current[id] += fresh;
      const packets = Math.floor(accrued.current[id] / packetEvery);
      accrued.current[id] -= packets * packetEvery;

      const rateChanged = Math.abs(rate - prev[id].rate) > 1e-3;
      if (packets > 0 || rateChanged || ts !== prev[id].lastSeen) {
        next[id] = { activity: prev[id].activity + packets, rate, lastSeen: ts };
        changed = true;
      }
    }

    if (changed) {
      pulseRef.current = next;
      setPulse(next);
    }
  }, [state, packetEvery]);

  return pulse;
}
