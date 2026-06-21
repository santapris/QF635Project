import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import type { AnalyticsSnapshot } from "../../store/pipelineStore";

interface Props {
  analytics: AnalyticsSnapshot | null;
}

type Tone = "neutral" | "up" | "down" | "warn";

const TONE_COLOR: Record<Tone, string | undefined> = {
  neutral: undefined,
  up: "success.main",
  down: "error.main",
  warn: "warning.main",
};

function Gauge({ label, value, tone = "neutral" }: { label: string; value: string; tone?: Tone }) {
  return (
    <Box
      sx={{
        flex: "1 1 0",
        minWidth: 110,
        px: 2,
        py: 1.25,
        borderRight: "1px solid",
        borderColor: "divider",
        "&:last-of-type": { borderRight: "none" },
      }}
    >
      <Typography
        sx={{
          fontSize: 10,
          fontWeight: 700,
          letterSpacing: "1.5px",
          textTransform: "uppercase",
          color: "text.secondary",
          whiteSpace: "nowrap",
        }}
      >
        {label}
      </Typography>
      <Typography
        sx={{
          mt: 0.5,
          fontSize: 16,
          fontWeight: 700,
          letterSpacing: "-0.2px",
          fontVariantNumeric: "tabular-nums",
          color: TONE_COLOR[tone] ?? "text.primary",
          whiteSpace: "nowrap",
        }}
      >
        {value}
      </Typography>
    </Box>
  );
}

function arrow(v: number | null): string {
  if (v == null) return "";
  return v > 0.1 ? " ▲" : v < -0.1 ? " ▼" : " →";
}

/**
 * Compact horizontal microstructure strip for the dashboard. Surfaces the key
 * live analytics gauges (microprice drift, vol, OBI, OFI, VPIN, inventory,
 * half-spread) without leaving the dashboard for the Analytics page.
 */
export default function GaugeStrip({ analytics }: Props) {
  const a = analytics;
  const microDelta = a ? a.microprice - a.mid_price : null;

  return (
    <Box
      sx={{
        display: "flex",
        flexWrap: "wrap",
        border: "1px solid",
        borderColor: "divider",
        bgcolor: "background.paper",
      }}
    >
      <Gauge
        label="Microprice Δ"
        value={microDelta == null ? "—" : `${microDelta >= 0 ? "+" : ""}${microDelta.toFixed(4)}`}
        tone={microDelta == null ? "neutral" : microDelta > 0 ? "up" : microDelta < 0 ? "down" : "neutral"}
      />
      <Gauge
        label="σ Vol"
        value={a?.sigma != null ? a.sigma.toFixed(6) : "—"}
        tone={a?.sigma != null && a.sigma > 0.01 ? "warn" : "neutral"}
      />
      <Gauge
        label="OBI L1"
        value={a?.obi != null ? `${a.obi.toFixed(3)}${arrow(a.obi)}` : "—"}
        tone={a?.obi != null ? (a.obi > 0.2 ? "up" : a.obi < -0.2 ? "down" : "neutral") : "neutral"}
      />
      <Gauge
        label="OFI"
        value={a?.ofi != null ? `${a.ofi.toFixed(3)}${arrow(a.ofi)}` : "—"}
        tone={a?.ofi != null ? (a.ofi > 0 ? "up" : "down") : "neutral"}
      />
      <Gauge
        label="VPIN"
        value={a?.vpin != null ? `${a.vpin.toFixed(3)}${a.vpin_widened ? " ⚠" : ""}` : "—"}
        tone={a?.vpin != null ? (a.vpin >= 0.7 ? "down" : a.vpin >= 0.5 ? "warn" : "up") : "neutral"}
      />
      <Gauge
        label="Inventory"
        value={a?.inventory != null ? a.inventory.toFixed(6) : "—"}
      />
      <Gauge
        label="Half Spread"
        value={a?.half_spread != null ? a.half_spread.toFixed(4) : "—"}
        tone={
          a?.half_spread != null && a.half_spread_raw != null && a.half_spread !== a.half_spread_raw
            ? "warn"
            : "neutral"
        }
      />
    </Box>
  );
}
