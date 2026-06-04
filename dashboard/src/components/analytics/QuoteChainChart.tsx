import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import { useTheme } from "@mui/material/styles";
import {
  ResponsiveContainer,
  ComposedChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ReferenceDot,
} from "recharts";
import type { AnalyticsPoint } from "../../store/pipelineStore";
import { formatTs } from "../../utils/formatTs";

interface Props {
  history: AnalyticsPoint[];
}

export default function QuoteChainChart({ history }: Props) {
  const theme = useTheme();

  if (history.length === 0) {
    return (
      <Paper sx={{ p: 2, height: "100%" }}>
        <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
          Quote Decision Chain
        </Typography>
        <Typography variant="body2" color="text.disabled" sx={{ mt: 2, textAlign: "center" }}>
          Waiting for data…
        </Typography>
      </Paper>
    );
  }

  // Only include fields this chart needs; null → undefined so Recharts
  // treats missing strategy fields as gaps, not as y=0 (which distorts domain).
  const data = history.map((p) => ({
    ts: p.ts,
    mid_price: p.mid_price,
    microprice: p.microprice,
    reservation: p.reservation ?? undefined,
    bid_quote: p.bid_quote ?? undefined,
    ask_quote: p.ask_quote ?? undefined,
  }));

  // Compute y-domain from only the values this chart renders — exclude undefined
  // gaps. Recharts auto-domain is unreliable when some data keys are undefined.
  const priceValues = data.flatMap((d) =>
    [d.mid_price, d.microprice, d.reservation, d.bid_quote, d.ask_quote]
      .filter((v): v is number => typeof v === "number" && isFinite(v))
  );
  const yMin = priceValues.length > 0 ? Math.min(...priceValues) : 0;
  const yMax = priceValues.length > 0 ? Math.max(...priceValues) : 100;
  const yPad = Math.max((yMax - yMin) * 0.5, 50);
  const yDomain: [number, number] = [yMin - yPad, yMax + yPad];

  const widenedPoints = history.filter((p) => p.vpin_widened && p.bid_quote != null);

  return (
    <Paper sx={{ p: 2, height: "100%" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Quote Decision Chain — microprice → reservation → bid/ask
      </Typography>
      <ResponsiveContainer width="100%" height="90%">
        <ComposedChart data={data} margin={{ top: 4, right: 16, bottom: 4, left: 0 }}>
          <XAxis dataKey="ts" tickFormatter={(v: number) => formatTs(v)} tick={{ fontSize: 11 }} minTickGap={60} />
          <YAxis
            tick={{ fontSize: 11 }}
            width={80}
            domain={yDomain}
            tickFormatter={(v: number) => v.toFixed(1)}
          />
          <Tooltip
            labelFormatter={(v) => formatTs(Number(v))}
            formatter={(v: number) => v.toFixed(4)}
          />
          <Legend />
          <Line
            type="monotone"
            dataKey="bid_quote"
            name="bid_quote"
            stroke={theme.palette.success.main}
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
            connectNulls={false}
          />
          <Line
            type="monotone"
            dataKey="ask_quote"
            name="ask_quote"
            stroke={theme.palette.error.main}
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
            connectNulls={false}
          />
          <Line
            type="monotone"
            dataKey="mid_price"
            name="mid"
            stroke={theme.palette.text.disabled}
            strokeDasharray="4 2"
            dot={false}
            isAnimationActive={false}
          />
          <Line
            type="monotone"
            dataKey="microprice"
            name="microprice"
            stroke={theme.palette.primary.main}
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
          <Line
            type="monotone"
            dataKey="reservation"
            name="reservation"
            stroke={theme.palette.warning.main}
            strokeWidth={1.5}
            dot={false}
            isAnimationActive={false}
          />
          {/* VPIN widening markers */}
          {widenedPoints.map((p, i) => (
            <ReferenceDot
              key={i}
              x={p.ts}
              y={p.bid_quote as number}
              r={3}
              fill={theme.palette.error.main}
              stroke="none"
            />
          ))}
        </ComposedChart>
      </ResponsiveContainer>
    </Paper>
  );
}
