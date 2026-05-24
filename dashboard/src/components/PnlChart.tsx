import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ReferenceLine,
} from "recharts";
import type { PnlPoint } from "../store/pipelineStore";

interface Props {
  pnlHistory: PnlPoint[];
}

function formatTs(ts: string): string {
  // ts is a nanosecond integer as string; convert to HH:MM:SS
  const ms = Math.floor(Number(ts) / 1_000_000);
  return new Date(ms).toLocaleTimeString();
}

export default function PnlChart({ pnlHistory }: Props) {
  return (
    <Paper sx={{ p: 2, height: "100%" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        PnL
      </Typography>
      {pnlHistory.length === 0 ? (
        <Typography variant="body2" color="text.disabled" sx={{ mt: 2, textAlign: "center" }}>
          Waiting for data…
        </Typography>
      ) : (
        <ResponsiveContainer width="100%" height="90%">
          <LineChart data={pnlHistory} margin={{ top: 4, right: 16, bottom: 4, left: 0 }}>
            <XAxis dataKey="ts" tickFormatter={formatTs} tick={{ fontSize: 11 }} minTickGap={60} />
            <YAxis tick={{ fontSize: 11 }} width={70} />
            <Tooltip
              labelFormatter={(v) => formatTs(String(v))}
              formatter={(v: number) => v.toFixed(4)}
            />
            <Legend />
            <ReferenceLine y={0} stroke="#888" strokeDasharray="3 3" />
            <Line
              type="monotone"
              dataKey="unrealized_pnl"
              name="Unrealized"
              stroke="#42a5f5"
              dot={false}
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="realized_pnl"
              name="Realized"
              stroke="#66bb6a"
              dot={false}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      )}
    </Paper>
  );
}
