import { useMemo } from "react";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import { useTheme } from "@mui/material/styles";
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ReferenceLine,
} from "recharts";
import type { TooltipProps } from "recharts";
import type { PnlPoint } from "../store/pipelineStore";
import { formatTs } from "../utils/formatTs";
import MStripe from "./bmw/MStripe";

interface Props {
  pnlHistory: PnlPoint[];
}

/** Short HH:MM:SS for axis ticks — full ms precision is reserved for the tooltip. */
function formatClock(ms: number): string {
  const d = new Date(ms);
  return [d.getHours(), d.getMinutes(), d.getSeconds()]
    .map((n) => String(n).padStart(2, "0"))
    .join(":");
}

function fmtSigned(n: number, digits = 2): string {
  return `${n > 0 ? "+" : ""}${n.toFixed(digits)}`;
}

function CustomTooltip({ active, payload, label }: TooltipProps<number, string>) {
  const theme = useTheme();
  if (!active || !payload?.length) return null;

  const pnlColor = (n: number) =>
    n > 0 ? theme.palette.success.main : n < 0 ? theme.palette.error.main : theme.palette.text.primary;

  return (
    <Box
      sx={{
        bgcolor: theme.palette.mode === "dark" ? "#161616" : "#ffffff",
        border: "1px solid",
        borderColor: "divider",
        px: 1.5,
        py: 1,
        minWidth: 150,
      }}
    >
      <Typography
        sx={{ fontSize: 10, letterSpacing: "1px", textTransform: "uppercase", color: "text.secondary", mb: 0.5 }}
      >
        {formatTs(Number(label))}
      </Typography>
      {payload.map((p) => {
        const v = Number(p.value);
        return (
          <Box key={p.dataKey} sx={{ display: "flex", justifyContent: "space-between", gap: 2 }}>
            <Box sx={{ display: "flex", alignItems: "center", gap: 0.75 }}>
              <Box sx={{ width: 8, height: 8, bgcolor: p.color }} />
              <Typography sx={{ fontSize: 11, color: "text.secondary" }}>{p.name}</Typography>
            </Box>
            <Typography
              sx={{ fontSize: 12, fontWeight: 700, fontVariantNumeric: "tabular-nums", color: pnlColor(v) }}
            >
              {fmtSigned(v, 4)}
            </Typography>
          </Box>
        );
      })}
    </Box>
  );
}

export default function PnlChart({ pnlHistory }: Props) {
  const theme = useTheme();
  const isDark = theme.palette.mode === "dark";

  // Derive total = unrealized + realized so the headline series tracks net P&L.
  const data = useMemo(
    () => pnlHistory.map((p) => ({ ...p, total_pnl: p.unrealized_pnl + p.realized_pnl })),
    [pnlHistory],
  );

  const latest = data.length ? data[data.length - 1] : null;
  const totalNow = latest ? latest.total_pnl : 0;
  const totalColor =
    totalNow > 0 ? theme.palette.success.main : totalNow < 0 ? theme.palette.error.main : theme.palette.text.primary;

  // Total area takes a green/red tint by sign; the two component lines are flat strokes.
  const totalStroke = totalColor;
  const unrealStroke = theme.palette.info.main;
  const realStroke = isDark ? "#e6e6e6" : "#555";

  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      {/* Header: title + live total readout */}
      <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", mb: 0.5 }}>
        <Box>
          <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
            Profit &amp; Loss
          </Typography>
          <MStripe width={28} height={3} sx={{ mt: 0.5 }} />
        </Box>
        <Box sx={{ textAlign: "right" }}>
          <Typography sx={{ fontSize: 10, letterSpacing: "1.5px", textTransform: "uppercase", color: "text.secondary" }}>
            Total
          </Typography>
          <Typography
            sx={{
              fontSize: 22,
              fontWeight: 800,
              lineHeight: 1,
              letterSpacing: "-0.5px",
              fontVariantNumeric: "tabular-nums",
              color: totalColor,
            }}
          >
            {fmtSigned(totalNow, 2)}
          </Typography>
        </Box>
      </Box>

      {/* Legend */}
      <Box sx={{ display: "flex", gap: 2, mb: 0.5 }}>
        {[
          { name: "Total", color: totalStroke },
          { name: "Unrealized", color: unrealStroke },
          { name: "Realized", color: realStroke },
        ].map((s) => (
          <Box key={s.name} sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
            <Box sx={{ width: 10, height: 3, bgcolor: s.color }} />
            <Typography sx={{ fontSize: 10, letterSpacing: "0.5px", textTransform: "uppercase", color: "text.secondary" }}>
              {s.name}
            </Typography>
          </Box>
        ))}
      </Box>

      <Box sx={{ flex: 1, minHeight: 0 }}>
        {data.length === 0 ? (
          <Typography variant="body2" color="text.disabled" sx={{ mt: 4, textAlign: "center" }}>
            Waiting for data…
          </Typography>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
              <defs>
                <linearGradient id="pnlTotalFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={totalStroke} stopOpacity={0.28} />
                  <stop offset="100%" stopColor={totalStroke} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid
                strokeDasharray="2 4"
                stroke={theme.palette.divider}
                vertical={false}
              />
              <XAxis
                dataKey="ts"
                tickFormatter={formatClock}
                tick={{ fontSize: 10, fill: theme.palette.text.secondary }}
                tickLine={false}
                axisLine={{ stroke: theme.palette.divider }}
                minTickGap={56}
              />
              <YAxis
                tick={{ fontSize: 10, fill: theme.palette.text.secondary, fontVariant: "tabular-nums" }}
                tickLine={false}
                axisLine={false}
                width={58}
                tickFormatter={(v: number) => v.toFixed(2)}
              />
              <Tooltip content={<CustomTooltip />} cursor={{ stroke: theme.palette.text.secondary, strokeDasharray: "3 3" }} />
              <ReferenceLine y={0} stroke={theme.palette.divider} />
              <Area
                type="monotone"
                dataKey="total_pnl"
                name="Total"
                stroke={totalStroke}
                strokeWidth={2}
                fill="url(#pnlTotalFill)"
                dot={false}
                activeDot={{ r: 3, strokeWidth: 0 }}
                isAnimationActive={false}
              />
              <Area
                type="monotone"
                dataKey="unrealized_pnl"
                name="Unrealized"
                stroke={unrealStroke}
                strokeWidth={1.25}
                strokeDasharray="4 3"
                fill="none"
                dot={false}
                isAnimationActive={false}
              />
              <Area
                type="monotone"
                dataKey="realized_pnl"
                name="Realized"
                stroke={realStroke}
                strokeWidth={1.25}
                fill="none"
                dot={false}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </Box>
    </Paper>
  );
}
