import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import { useTheme } from "@mui/material/styles";
import {
  ResponsiveContainer,
  ComposedChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ReferenceLine,
} from "recharts";
import type { AnalyticsPoint } from "../../store/pipelineStore";
import { formatTs } from "../../utils/formatTs";

interface Props {
  history: AnalyticsPoint[];
}

const VPIN_THRESHOLD = 0.7;

export default function AnalyticsDriversChart({ history }: Props) {
  const theme = useTheme();

  if (history.length === 0) {
    return (
      <Paper sx={{ p: 2, height: "100%" }}>
        <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
          Analytics Drivers
        </Typography>
        <Typography variant="body2" color="text.disabled" sx={{ mt: 2, textAlign: "center" }}>
          Waiting for data…
        </Typography>
      </Paper>
    );
  }

  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Analytics Drivers — σ (left) | VPIN / OFI / OBI (right)
      </Typography>
      <Box sx={{ flex: 1, minHeight: 0 }}>
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={history} margin={{ top: 4, right: 40, bottom: 4, left: 0 }}>
          <XAxis dataKey="ts" tickFormatter={(v: number) => formatTs(v)} tick={{ fontSize: 11 }} minTickGap={60} />
          <YAxis
            yAxisId="sigma"
            orientation="left"
            tick={{ fontSize: 11 }}
            width={60}
            tickFormatter={(v: number) => v.toFixed(4)}
          />
          <YAxis
            yAxisId="right"
            orientation="right"
            tick={{ fontSize: 11 }}
            width={40}
            domain={[-1, 1]}
            tickFormatter={(v: number) => v.toFixed(2)}
          />
          <Tooltip
            labelFormatter={(v) => formatTs(Number(v))}
            formatter={(v: number) => v?.toFixed(5) ?? "—"}
          />
          <Legend />
          <ReferenceLine yAxisId="right" y={VPIN_THRESHOLD} stroke={theme.palette.error.main} strokeDasharray="3 3" />
          <Line yAxisId="sigma" type="monotone" dataKey="sigma" name="σ (vol)" stroke={theme.palette.warning.main} dot={false} isAnimationActive={false} />
          <Line yAxisId="right" type="monotone" dataKey="vpin" name="VPIN" stroke={theme.palette.error.main} strokeWidth={1.5} dot={false} isAnimationActive={false} />
          <Line yAxisId="right" type="monotone" dataKey="ofi" name="OFI" stroke={theme.palette.primary.main} dot={false} isAnimationActive={false} />
          <Line yAxisId="right" type="monotone" dataKey="obi" name="OBI" stroke={theme.palette.success.main} strokeDasharray="4 2" dot={false} isAnimationActive={false} />
        </ComposedChart>
      </ResponsiveContainer>
      </Box>
    </Paper>
  );
}
