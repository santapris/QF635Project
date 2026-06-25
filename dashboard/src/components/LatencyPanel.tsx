import Box from "@mui/material/Box";
import Grid from "@mui/material/Grid";
import Typography from "@mui/material/Typography";
import SpecCell from "./bmw/SpecCell";
import type { LatencySnapshot, StageLatencyData } from "../store/pipelineStore";

interface Props {
  latency: LatencySnapshot | null;
}

// Internal asyncio stages (tick→signal, signal→risk, risk→OMS).
// Green  < 1 ms   — healthy event loop, no blocking
// Orange 1–5 ms   — event loop contention; something is holding the loop
// Red    ≥ 5 ms   — event loop stalled; strategy or rule doing heavy sync work
function internalColor(ms: number | null): string | undefined {
  if (ms == null) return undefined;
  if (ms < 1)  return "success.main";
  if (ms < 5)  return "warning.main";
  return "error.main";
}

// Venue WS delivery (ts_ingest − ts_event on FillEvent).
// Measures how long after Binance's fill timestamp we received the WS notification.
// Green  < 50 ms  — normal for a well-connected node
// Orange 50–150 ms — elevated; check WS backpressure or geographic distance
// Red    ≥ 150 ms — poor WS delivery; possible clock drift vs Binance servers
function venueColor(ms: number | null): string | undefined {
  if (ms == null) return undefined;
  if (ms < 50)  return "success.main";
  if (ms < 150) return "warning.main";
  return "error.main";
}

function fmtMs(ms: number | null | undefined): string {
  if (ms == null) return "—";
  if (ms < 1) return `${Math.round(ms * 1000)} µs`;
  return `${ms.toFixed(3)} ms`;
}

interface StageRowProps {
  label: string;
  data: StageLatencyData | null;
  colorFn: (ms: number | null) => string | undefined;
  badge?: string;
  last?: boolean;
}

function StageRow({ label, data, colorFn, badge, last }: StageRowProps) {
  return (
    <Box
      sx={{
        display: "flex",
        alignItems: "center",
        px: 2,
        py: 1.25,
        borderBottom: last ? "none" : "1px solid",
        borderColor: "divider",
      }}
    >
      {/* Label */}
      <Box sx={{ flex: 1 }}>
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
          {badge && (
            <Typography
              component="span"
              sx={{ ml: 1, fontSize: 9, fontWeight: 400, color: "text.disabled", letterSpacing: "1px" }}
            >
              {badge}
            </Typography>
          )}
        </Typography>
      </Box>

      {/* p50 */}
      <Box sx={{ width: 88, textAlign: "right" }}>
        <Typography sx={{ fontSize: 9, color: "text.disabled", textTransform: "uppercase", letterSpacing: "1px", lineHeight: 1 }}>
          p50
        </Typography>
        <Typography
          sx={{ fontSize: 14, fontWeight: 700, fontVariantNumeric: "tabular-nums", lineHeight: 1.3, color: colorFn(data?.p50_ms ?? null) }}
        >
          {fmtMs(data?.p50_ms)}
        </Typography>
      </Box>

      {/* p95 */}
      <Box sx={{ width: 88, textAlign: "right" }}>
        <Typography sx={{ fontSize: 9, color: "text.disabled", textTransform: "uppercase", letterSpacing: "1px", lineHeight: 1 }}>
          p95
        </Typography>
        <Typography
          sx={{ fontSize: 14, fontWeight: 700, fontVariantNumeric: "tabular-nums", lineHeight: 1.3, color: colorFn(data?.p95_ms ?? null) }}
        >
          {fmtMs(data?.p95_ms)}
        </Typography>
      </Box>

      {/* count */}
      <Box sx={{ width: 52, textAlign: "right" }}>
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

  // Sum of internal stage p95s — conservative upper bound for the headline.
  // NOTE: this is not the true E2E p95 (which would require per-event correlation),
  // but the sum of per-stage 95th percentiles gives a safe ceiling for operators.
  const internalStages = [latency.tick_to_signal, latency.signal_to_decision, latency.decision_to_order];
  const internalP95ms: number | null = internalStages.every((s) => s?.p95_ms != null)
    ? internalStages.reduce((acc, s) => acc! + s!.p95_ms!, 0 as number)
    : null;

  return (
    <Box>
      {/* Hero KPI row */}
      <Grid container spacing={0} sx={{ mb: 1 }}>
        <Grid size={{ xs: 12, sm: 6 }}>
          <SpecCell
            label="Internal p95 · tick → order"
            value={fmtMs(internalP95ms)}
            unit="green < 1 ms  ·  orange 1–5 ms  ·  red ≥ 5 ms"
            valueColor={internalColor(internalP95ms)}
            accent
          />
        </Grid>
        <Grid size={{ xs: 12, sm: 6 }}>
          <SpecCell
            label="Venue WS delivery p95"
            value={fmtMs(latency.order_to_fill?.p95_ms ?? null)}
            unit="green < 50 ms  ·  orange < 150 ms  ·  red ≥ 150 ms"
            valueColor={venueColor(latency.order_to_fill?.p95_ms ?? null)}
          />
        </Grid>
      </Grid>

      {/* Internal pipeline stages */}
      <Box sx={{ border: "1px solid", borderColor: "divider", bgcolor: "background.paper", mb: 1 }}>
        <StageRow label="Tick → Signal"  data={latency.tick_to_signal}     colorFn={internalColor} />
        <StageRow label="Signal → Risk"  data={latency.signal_to_decision}  colorFn={internalColor} />
        <StageRow label="Risk → OMS"     data={latency.decision_to_order}   colorFn={internalColor} last />
      </Box>

      {/* Venue stage — visually separated with a badge */}
      <Box sx={{ border: "1px solid", borderColor: "divider", bgcolor: "background.paper" }}>
        <StageRow
          label="Fill WS Delivery"
          data={latency.order_to_fill}
          colorFn={venueColor}
          badge="Binance ts → wire arrival · not our code"
          last
        />
      </Box>
    </Box>
  );
}
