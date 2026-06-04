import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import { useTheme } from "@mui/material/styles";
import type { AnalyticsSnapshot } from "../../store/pipelineStore";

interface Props {
  analytics: AnalyticsSnapshot | null;
}

function Gauge({ label, value, unit = "", color }: {
  label: string;
  value: string;
  unit?: string;
  color?: "success" | "warning" | "error" | "default";
}) {
  const theme = useTheme();
  const borderColor = color === "success" ? theme.palette.success.main
    : color === "warning" ? theme.palette.warning.main
    : color === "error" ? theme.palette.error.main
    : theme.palette.divider;

  return (
    <Box sx={{ p: 1, borderLeft: `3px solid ${borderColor}`, minWidth: 120 }}>
      <Typography variant="caption" color="text.secondary" sx={{ display: "block" }}>
        {label}
      </Typography>
      <Typography variant="body2" sx={{ fontWeight: 700, fontFamily: "monospace" }}>
        {value}{unit}
      </Typography>
    </Box>
  );
}

function vpinColor(v: number | null): "success" | "warning" | "error" | "default" {
  if (v == null) return "default";
  if (v < 0.5) return "success";
  if (v < 0.7) return "warning";
  return "error";
}

function invColor(inv: number | null, maxPos: number = 0.5): "success" | "warning" | "error" {
  if (inv == null) return "success";
  const ratio = Math.abs(inv) / maxPos;
  if (ratio < 0.5) return "success";
  if (ratio < 0.9) return "warning";
  return "error";
}

function dir(v: number | null): string {
  if (v == null) return "";
  return v > 0.1 ? " ▲" : v < -0.1 ? " ▼" : " →";
}

export default function LiveGaugesPanel({ analytics }: Props) {
  if (analytics == null) {
    return (
      <Paper sx={{ p: 2, height: "100%" }}>
        <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
          Live Gauges
        </Typography>
        <Typography variant="body2" color="text.disabled" sx={{ mt: 2, textAlign: "center" }}>
          Waiting for data…
        </Typography>
      </Paper>
    );
  }

  const a = analytics;
  const microDelta = a.microprice - a.mid_price;

  return (
    <Paper sx={{ p: 2, height: "100%" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Live Gauges
      </Typography>
      <Box sx={{ display: "flex", flexWrap: "wrap", gap: 1 }}>
        <Gauge
          label="Microprice"
          value={`${a.microprice.toFixed(2)} (${microDelta >= 0 ? "+" : ""}${microDelta.toFixed(4)})`}
          color={microDelta > 0 ? "success" : microDelta < 0 ? "error" : "default"}
        />
        <Gauge
          label="σ EWMA Vol"
          value={a.sigma != null ? a.sigma.toFixed(6) : "—"}
          color={a.sigma != null ? (a.sigma > 0.01 ? "warning" : "success") : "default"}
        />
        <Gauge
          label="OBI (L1)"
          value={a.obi != null ? `${a.obi.toFixed(3)}${dir(a.obi)}` : "—"}
          color={a.obi != null ? (a.obi > 0.2 ? "success" : a.obi < -0.2 ? "error" : "default") : "default"}
        />
        <Gauge
          label="OBI (L2)"
          value={a.obi_l2 != null ? `${a.obi_l2.toFixed(3)}${dir(a.obi_l2)}` : "—"}
          color={a.obi_l2 != null ? (a.obi_l2 > 0.2 ? "success" : a.obi_l2 < -0.2 ? "error" : "default") : "default"}
        />
        <Gauge
          label="OFI"
          value={a.ofi != null ? `${a.ofi.toFixed(3)}${dir(a.ofi)}` : "—"}
          color={a.ofi != null ? (a.ofi > 0 ? "success" : "error") : "default"}
        />
        <Box sx={{ p: 1, minWidth: 120 }}>
          <Typography variant="caption" color="text.secondary" sx={{ display: "block" }}>
            VPIN
          </Typography>
          <Chip
            label={a.vpin != null ? a.vpin.toFixed(3) : "—"}
            color={vpinColor(a.vpin)}
            size="small"
            sx={{ fontFamily: "monospace", fontWeight: 700 }}
          />
          {a.vpin_widened && (
            <Chip label="WIDENED" color="error" size="small" sx={{ ml: 0.5 }} />
          )}
        </Box>
        <Gauge
          label="Inventory"
          value={a.inventory != null ? a.inventory.toFixed(6) : "—"}
          color={invColor(a.inventory)}
        />
        <Gauge
          label="Reservation"
          value={a.reservation != null ? a.reservation.toFixed(4) : "—"}
          color="default"
        />
        <Box sx={{ p: 1, minWidth: 140 }}>
          <Typography variant="caption" color="text.secondary" sx={{ display: "block" }}>
            Half Spread
          </Typography>
          <Typography variant="body2" sx={{ fontFamily: "monospace", fontWeight: 700 }}>
            {a.half_spread != null ? a.half_spread.toFixed(4) : "—"}
            {a.half_spread != null && a.half_spread_raw != null && a.half_spread !== a.half_spread_raw && (
              <Typography component="span" variant="caption" color="error.main" sx={{ ml: 0.5 }}>
                (base {a.half_spread_raw.toFixed(4)})
              </Typography>
            )}
          </Typography>
        </Box>
      </Box>
    </Paper>
  );
}
