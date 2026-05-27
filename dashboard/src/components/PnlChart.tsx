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
import { formatTs } from "../utils/formatTs";

interface Props {
  pnlHistory: PnlPoint[];
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
            <XAxis dataKey="ts" tickFormatter={(v: number) => formatTs(v)} tick={{ fontSize: 11 }} minTickGap={60} />
            <YAxis tick={{ fontSize: 11 }} width={70} />
            <Tooltip
              labelFormatter={(v) => formatTs(Number(v))}
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
