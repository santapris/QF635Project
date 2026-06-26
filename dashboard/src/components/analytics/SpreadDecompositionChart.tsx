import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import { useTheme } from "@mui/material/styles";
import {
  ResponsiveContainer,
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
} from "recharts";
import type { AnalyticsPoint } from "../../store/pipelineStore";
import { formatTs } from "../../utils/formatTs";
import { chartTooltipStyle } from "../../utils/chartTooltip";

interface Props {
  history: AnalyticsPoint[];
}

export default function SpreadDecompositionChart({ history }: Props) {
  const theme = useTheme();

  if (history.length === 0) {
    return (
      <Paper sx={{ p: 2, height: "100%" }}>
        <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
          Spread Decomposition
        </Typography>
        <Typography variant="body2" color="text.disabled" sx={{ mt: 2, textAlign: "center" }}>
          Waiting for data…
        </Typography>
      </Paper>
    );
  }

  const data = history
    .filter((p) => p.half_spread != null && p.half_spread_raw != null && p.reservation != null)
    .map((p) => ({
      ts: p.ts,
      base_spread: p.half_spread_raw as number,
      vpin_premium: Math.max(0, (p.half_spread as number) - (p.half_spread_raw as number)),
      inv_skew: (p.reservation as number) - p.microprice,
    }));

  if (data.length === 0) {
    return (
      <Paper sx={{ p: 2, height: "100%" }}>
        <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
          Spread Decomposition
        </Typography>
        <Typography variant="body2" color="text.disabled" sx={{ mt: 2, textAlign: "center" }}>
          No strategy diagnostics — run a strategy that implements get_strategy_diagnostics()
        </Typography>
      </Paper>
    );
  }

  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Spread Decomposition — base A-S + VPIN premium | inventory skew (right)
      </Typography>
      <Box sx={{ flex: 1, minHeight: 0 }}>
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={{ top: 4, right: 40, bottom: 4, left: 0 }}>
          <XAxis dataKey="ts" tickFormatter={(v: number) => formatTs(v)} tick={{ fontSize: 11 }} minTickGap={60} />
          <YAxis
            yAxisId="spread"
            orientation="left"
            tick={{ fontSize: 11 }}
            width={65}
            tickFormatter={(v: number) => v.toFixed(4)}
          />
          <YAxis
            yAxisId="skew"
            orientation="right"
            tick={{ fontSize: 11 }}
            width={45}
            tickFormatter={(v: number) => v.toFixed(4)}
          />
          <Tooltip
            {...chartTooltipStyle(theme)}
            labelFormatter={(v) => formatTs(Number(v))}
            formatter={(v: number) => v?.toFixed(5) ?? "—"}
          />
          <Legend />
          <Area
            yAxisId="spread"
            type="monotone"
            dataKey="base_spread"
            name="Base half-spread"
            stroke={theme.palette.primary.main}
            fill={theme.palette.primary.main}
            fillOpacity={0.15}
            dot={false}
            isAnimationActive={false}
            baseValue="dataMin"
          />
          <Area
            yAxisId="spread"
            type="monotone"
            dataKey="vpin_premium"
            name="VPIN premium"
            stroke={theme.palette.error.main}
            fill={theme.palette.error.main}
            fillOpacity={0.25}
            stackId="spread"
            dot={false}
            isAnimationActive={false}
            baseValue="dataMin"
          />
          <Line
            yAxisId="skew"
            type="monotone"
            dataKey="inv_skew"
            name="Inv skew"
            stroke={theme.palette.warning.main}
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
      </Box>
    </Paper>
  );
}
