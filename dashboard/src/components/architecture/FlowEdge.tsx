/**
 * A pipeline edge that animates a packet travelling along its path each time
 * its destination node sees a new event.
 *
 * The packet is driven by requestAnimationFrame rather than SMIL: a fresh
 * traversal is started every time `activity` increments. We tried SMIL
 * <animateMotion> first, but re-arming it on each event (even via a keyed
 * remount) is unreliable across React/browsers — only the first run played.
 * RAF + getPointAtLength gives a guaranteed restart per event.
 */
import { memo, useEffect, useRef, useState } from "react";
import { BaseEdge, getSmoothStepPath, type EdgeProps } from "@xyflow/react";

export interface FlowEdgeData {
  accent: string;
  /** Smoothed throughput in events/sec — drives the flowing-dot stream. */
  rate: number;
  /** True while data has flowed in this edge's direction recently. */
  active: boolean;
  /** True when a topic filter is on and this edge isn't part of it. */
  dimmed: boolean;
  /** Optional latency label rendered mid-edge (e.g. "p50 0.8ms"). */
  latency: string | null;
  [key: string]: unknown;
}

// ── Flow-stream tuning ─────────────────────────────────────────────────────
// Dots flow along the path as independent flights. `rate` (a unit-free 0–1 flow
// intensity, see useFlowPulse) controls how OFTEN a new dot is spawned and how
// FAST it travels — but NOT whether an already-launched dot continues. Once
// spawned, every dot marches from 0→1 and only disappears at the far end, so
// slowing or stopping the flow never strands a dot mid-line; it just stops new
// ones from appearing while the ones already travelling finish.
const SLOW_MS = 1600;        // traversal time at intensity ~0 (a lone slow dot)
const FAST_MS = 650;         // traversal time at intensity 1
const SPAWN_SLOW_MS = 1400;  // ms between spawns at the lowest live intensity
const SPAWN_FAST_MS = 130;   // ms between spawns at intensity 1 (dense stream)
const IDLE_INTENSITY = 0.02; // below this, spawn nothing (live dots still finish)

/**
 * A continuous stream of evenly-spaced dots travelling `path`. Visual speed and
 * dot count are derived from `rate`; the dots are phase-offset so they appear as
 * a steady marching flow. A single shared RAF advances a global phase — there is
 * no per-event launch, so the stream stays smooth and legible no matter how many
 * events/sec the edge actually carries.
 */
interface Flight {
  key: number;     // unique id for React keys
  progress: number; // 0→1 along the path
  speed: number;    // progress per ms, captured at spawn so rate changes don't strand it
}

function Packet({ path, rate, color }: { path: string; rate: number; color: string }) {
  const pathRef = useRef<SVGPathElement | null>(null);
  const rafRef = useRef(0);
  const lastTRef = useRef(0);          // performance.now() of previous frame
  const sinceSpawnRef = useRef(0);     // ms accumulated since the last spawn
  const seqRef = useRef(0);            // monotonic key source
  const flightsRef = useRef<Flight[]>([]);
  const rateRef = useRef(rate);        // latest rate, read inside the RAF loop
  rateRef.current = rate;
  const [points, setPoints] = useState<{ key: number; x: number; y: number }[]>([]);

  useEffect(() => {
    const tick = (t: number) => {
      const dtMs = lastTRef.current === 0 ? 0 : t - lastTRef.current;
      lastTRef.current = t;

      const r = Math.min(1, rateRef.current);
      const el = pathRef.current;
      const len = el ? el.getTotalLength() : 0;

      // Spawn a new dot once enough time has passed — interval shrinks as the
      // flow gets busier. At idle intensity we simply DON'T spawn; we never
      // touch the dots already travelling.
      if (r >= IDLE_INTENSITY && len > 0) {
        sinceSpawnRef.current += dtMs;
        const spawnEveryMs = SPAWN_SLOW_MS + (SPAWN_FAST_MS - SPAWN_SLOW_MS) * r;
        if (sinceSpawnRef.current >= spawnEveryMs) {
          sinceSpawnRef.current = 0;
          const traversalMs = SLOW_MS + (FAST_MS - SLOW_MS) * r;
          flightsRef.current.push({ key: seqRef.current++, progress: 0, speed: 1 / traversalMs });
        }
      }

      // Advance every live flight to completion. A flight is removed ONLY when it
      // reaches the end (progress ≥ 1) — never because the rate dropped — so dots
      // always finish the line instead of stopping or vanishing halfway.
      const live: Flight[] = [];
      const pts: { key: number; x: number; y: number }[] = [];
      for (const fl of flightsRef.current) {
        fl.progress += fl.speed * dtMs;
        if (fl.progress >= 1) continue;          // arrived — retire it
        live.push(fl);
        if (len > 0 && el) {
          const p = el.getPointAtLength(fl.progress * len);
          pts.push({ key: fl.key, x: p.x, y: p.y });
        }
      }
      flightsRef.current = live;
      setPoints(pts);
      rafRef.current = requestAnimationFrame(tick);
    };
    rafRef.current = requestAnimationFrame(tick);
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current); lastTRef.current = 0; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <>
      {/* Hidden measuring path — getPointAtLength source of truth. */}
      <path ref={pathRef} d={path} fill="none" stroke="none" />
      {points.map((p) => (
        <circle key={p.key} cx={p.x} cy={p.y} r={4} fill={color} style={{ filter: `drop-shadow(0 0 4px ${color})` }} />
      ))}
    </>
  );
}

function FlowEdgeImpl(props: EdgeProps) {
  const { id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data } = props;
  const d = (data ?? {}) as FlowEdgeData;
  const [path, labelX, labelY] = getSmoothStepPath({
    sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition,
    borderRadius: 12,
  });

  const accent = d.accent;

  // Three visual states tame the hub:
  //   dimmed      → topic filtered out: barely there, recedes to background.
  //   focused-idle→ topic in view but quiet: solid topic colour so the flow is
  //                 traceable across the bus even with no live data.
  //   active      → recent event: bright, thick, and a travelling packet.
  const strokeWidth = d.dimmed ? 1 : d.active ? 2.5 : 1.75;
  const opacity = d.dimmed ? 0.12 : d.active ? 1 : 0.55;

  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        style={{
          stroke: accent,
          strokeWidth,
          opacity,
          transition: "stroke-width 0.4s ease, opacity 0.4s ease",
        }}
      />
      {/* Always mounted (never gated on `active`) so its RAF loop survives idle
          periods and resumes instantly when the rate returns. */}
      {!d.dimmed && <Packet path={path} rate={d.rate} color={accent} />}
      {d.latency && (
        <text
          x={labelX}
          y={labelY - 6}
          textAnchor="middle"
          style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.5px",
            fill: accent,
            paintOrder: "stroke",
            stroke: "#000",
            strokeWidth: 3,
          }}
        >
          {d.latency}
        </text>
      )}
    </>
  );
}

export default memo(FlowEdgeImpl);
