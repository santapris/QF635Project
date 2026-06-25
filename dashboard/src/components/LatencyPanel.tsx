import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import SpecCell from "./bmw/SpecCell";
import type { LatencySnapshot, StageLatencyData } from "../store/pipelineStore";

interface Props {
  latency: LatencySnapshot | null;
}

// Per-stage color functions calibrated to this system (Python asyncio, non-colocated).
// Threshold aims to answer "is the event loop stalling?" not "am I competitive with C++/co-lo HFT firms?"
// Exchange REST RTT is 15–50ms and dominates total latency — internal thresholds are
// set relative to that floor, not to HFT benchmarks.

// Tick → Signal: pure analytics compute + strategy logic.
// Green < 3ms — healthy; depth cap + sampled logging keeps this at ~772µs p95.
// Orange 3–8ms — analytics accumulating or VPIN bucket fills growing.
// Red ≥ 8ms — something is blocking the loop synchronously (e.g. logging every tick).
function tickToSignalColor(ms: number | null): string | undefined {
  if (ms == null) return undefined;
  if (ms < 3)  return "success.main";
  if (ms < 8)  return "warning.main";
  return "error.main";
}

// Signal → Risk: 6 rules × N legs, alert batch flush after decision.
// Green < 5ms — healthy with alert deferral in place.
// Orange 5–12ms — alerts may be back on the critical path, or new heavy rules added.
// Red ≥ 12ms — rule evaluation is blocking; regression to pre-fix behaviour.
function signalToRiskColor(ms: number | null): string | undefined {
  if (ms == null) return undefined;
  if (ms < 5)  return "success.main";
  if (ms < 12) return "warning.main";
  return "error.main";
}

// Risk → OMS: reconciliation across 5 strategies + order state lookup.
// Green < 8ms — healthy; OMS state is manageable.
// Orange 8–15ms — order accumulation building (cancel-replace debt growing).
// Red ≥ 15ms — OMS state large; likely approaching Binance 200-order limit.
function riskToOmsColor(ms: number | null): string | undefined {
  if (ms == null) return undefined;
  if (ms < 8)  return "success.main";
  if (ms < 15) return "warning.main";
  return "error.main";
}

// Hero total (sum of three internal stages).
// Green < 10ms — pipeline not contending with the event loop.
// Orange 10–25ms — elevated; investigate which stage regressed.
// Red ≥ 25ms — event loop stalling; strategy quotes are significantly stale.
function totalColor(ms: number | null): string | undefined {
  if (ms == null) return undefined;
  if (ms < 10) return "success.main";
  if (ms < 25) return "warning.main";
  return "error.main";
}

function fmtMs(ms: number | null | undefined): string {
  if (ms == null) return "—";
  if (ms < 0.001) return `${Math.round(ms * 1_000_000)} ns`;
  if (ms < 1)     return `${Math.round(ms * 1000)} µs`;
  if (ms < 1000)  return `${ms.toFixed(1)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

// Threshold bar: fills proportionally on a log-ish scale capped at maxMs.
// Uses two segments — green zone (0→threshold1) and orange zone (threshold1→threshold2) —
// so the visual position of the fill makes the health zone immediately readable.
interface ThresholdBarProps {
  ms: number | null;
  // thresholds where color changes, e.g. [1, 5] for internal stages
  thresholds: [number, number];
  // display max — bar saturates here; 2× the red threshold is a sensible cap
  maxMs: number;
  colorFn: (ms: number | null) => string | undefined;
}

function ThresholdBar({ ms, thresholds, maxMs, colorFn }: ThresholdBarProps) {
  const fillPct = ms == null ? 0 : Math.min(ms / maxMs, 1) * 100;
  const color = colorFn(ms);
  // threshold marker positions as % of maxMs
  const t1Pct = (thresholds[0] / maxMs) * 100;
  const t2Pct = (thresholds[1] / maxMs) * 100;

  return (
    <Box sx={{ mt: 1, height: 4, width: "100%", bgcolor: "action.hover", borderRadius: 0.5, position: "relative", overflow: "hidden" }}>
      {/* threshold tick marks */}
      <Box sx={{ position: "absolute", left: `${t1Pct}%`, top: 0, bottom: 0, width: "1px", bgcolor: "divider", zIndex: 1 }} />
      <Box sx={{ position: "absolute", left: `${t2Pct}%`, top: 0, bottom: 0, width: "1px", bgcolor: "divider", zIndex: 1 }} />
      {/* fill */}
      {ms != null && (
        <Box
          sx={{
            position: "absolute",
            left: 0,
            top: 0,
            bottom: 0,
            width: `${fillPct}%`,
            bgcolor: color ?? "text.disabled",
            borderRadius: 0.5,
            transition: "width 0.3s ease, background-color 0.3s ease",
          }}
        />
      )}
    </Box>
  );
}

interface StageRowProps {
  label: string;
  data: StageLatencyData | null;
  colorFn: (ms: number | null) => string | undefined;
  thresholds: [number, number];
  maxMs: number;
  last?: boolean;
}

function StageRow({ label, data, colorFn, thresholds, maxMs, last }: StageRowProps) {
  const p95 = data?.p95_ms ?? null;
  const p50 = data?.p50_ms ?? null;

  return (
    <Box
      sx={{
        px: 2,
        py: 1.5,
        borderBottom: last ? "none" : "1px solid",
        borderColor: "divider",
      }}
    >
      <Box sx={{ display: "flex", alignItems: "flex-start" }}>
        {/* Label + bar */}
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Typography
            sx={{
              fontSize: 10,
              fontWeight: 700,
              letterSpacing: "1.5px",
              textTransform: "uppercase",
              color: "text.secondary",
              lineHeight: 1,
            }}
          >
            {label}
          </Typography>
          <ThresholdBar ms={p95} thresholds={thresholds} maxMs={maxMs} colorFn={colorFn} />
        </Box>

        {/* p95 — hero number */}
        <Box sx={{ ml: 3, textAlign: "right", flexShrink: 0 }}>
          <Typography sx={{ fontSize: 9, color: "text.disabled", textTransform: "uppercase", letterSpacing: "1px", lineHeight: 1 }}>
            p95
          </Typography>
          <Typography
            sx={{ fontSize: 18, fontWeight: 700, fontVariantNumeric: "tabular-nums", lineHeight: 1.2, color: colorFn(p95) }}
          >
            {fmtMs(p95)}
          </Typography>
          {/* p50 as a subtle sub-line */}
          <Typography
            sx={{ fontSize: 10, fontVariantNumeric: "tabular-nums", color: "text.disabled", lineHeight: 1 }}
          >
            {fmtMs(p50)} p50
          </Typography>
        </Box>

        {/* count */}
        <Box sx={{ ml: 2, width: 40, textAlign: "right", flexShrink: 0 }}>
          <Typography sx={{ fontSize: 9, color: "text.disabled", textTransform: "uppercase", letterSpacing: "1px", lineHeight: 1 }}>
            n
          </Typography>
          <Typography
            sx={{ fontSize: 14, fontWeight: 700, fontVariantNumeric: "tabular-nums", lineHeight: 1.3, color: "text.secondary" }}
          >
            {data?.count ?? 0}
          </Typography>
        </Box>
      </Box>
    </Box>
  );
}

export default function LatencyPanel({ latency }: Props) {
  if (!latency) {
    return (
      <Box sx={{ py: 4, textAlign: "center" }}>
        <Typography sx={{ color: "text.disabled", fontSize: 13, letterSpacing: "0.5px" }}>
          No latency data yet — waiting for first signal cycle
        </Typography>
      </Box>
    );
  }

  const internalStages = [latency.tick_to_signal, latency.signal_to_decision, latency.decision_to_order];
  const internalP95ms: number | null = internalStages.every((s) => s?.p95_ms != null)
    ? internalStages.reduce((acc, s) => acc! + s!.p95_ms!, 0 as number)
    : null;

  return (
    <Box>
      {/* Hero KPI — full-width internal p95 */}
      <Box sx={{ mb: 1 }}>
        <SpecCell
          label="Internal p95 · tick → order"
          value={fmtMs(internalP95ms)}
          valueColor={totalColor(internalP95ms)}
          accent
        />
        <Typography sx={{px: 2, pb: 1, fontSize:15, fontWeight:600, color: "text.secondary", letterSpacing:"0.5px"}}>
          System specific Observability to indicate if Event loop is stalling (not competing with coloc HFT) green &lt; 10 ms  ·  orange 10–25 ms  ·  red ≥ 25 ms
        </Typography>
      </Box>

      {/* Internal pipeline stages — per-stage thresholds calibrated to this system */}
      <Box sx={{ border: "1px solid", borderColor: "divider", bgcolor: "background.paper" }}>
        <StageRow label="Tick → Signal" data={latency.tick_to_signal}    colorFn={tickToSignalColor} thresholds={[3, 8]}   maxMs={16} />
        <StageRow label="Signal → Risk" data={latency.signal_to_decision} colorFn={signalToRiskColor} thresholds={[5, 12]}  maxMs={25} />
        <StageRow label="Risk → OMS"    data={latency.decision_to_order}  colorFn={riskToOmsColor}    thresholds={[8, 15]}  maxMs={30} last />
      </Box>
    </Box>
  );
}
