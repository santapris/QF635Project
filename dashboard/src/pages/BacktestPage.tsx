import { useState, useMemo, useEffect, useRef } from "react";
import Grid from "@mui/material/Grid";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import FormControl from "@mui/material/FormControl";
import InputLabel from "@mui/material/InputLabel";
import Select from "@mui/material/Select";
import MenuItem from "@mui/material/MenuItem";
import Alert from "@mui/material/Alert";
import CircularProgress from "@mui/material/CircularProgress";
import { useTheme } from "@mui/material/styles";
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
} from "recharts";
import type {
  BacktestState,
  BacktestStatus,
  BacktestResult,
  BacktestConfigOption,
  PipelineAction,
} from "../store/pipelineStore";
import MStripe from "../components/bmw/MStripe";

const HTTP_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8765";

interface Props {
  backtest: BacktestState;
  dispatch: React.Dispatch<PipelineAction>;
}

function MetricCard({ label, value, color }: { label: string; value: string; color?: string }) {
  const theme = useTheme();
  return (
    <Box
      sx={{
        p: 1.5,
        border: "1px solid",
        borderColor: "divider",
        borderRadius: 1,
        minWidth: 120,
        flex: "1 1 120px",
      }}
    >
      <Typography
        sx={{ fontSize: 10, letterSpacing: "1px", textTransform: "uppercase", color: "text.secondary", mb: 0.25 }}
      >
        {label}
      </Typography>
      <Typography
        sx={{
          fontSize: 18,
          fontWeight: 800,
          fontVariantNumeric: "tabular-nums",
          lineHeight: 1.1,
          color: color ?? theme.palette.text.primary,
        }}
      >
        {value}
      </Typography>
    </Box>
  );
}

function fmt(n: number | string, digits = 2): string {
  const v = Number(n);
  return isNaN(v) ? String(n) : v.toFixed(digits);
}

function fmtPct(n: number, digits = 2): string {
  return `${(n * 100).toFixed(digits)}%`;
}

function fmtSigned(n: number | string, digits = 2): string {
  const v = Number(n);
  if (isNaN(v)) return String(n);
  return `${v > 0 ? "+" : ""}${v.toFixed(digits)}`;
}

function nsToMs(ns: number): number {
  return Math.floor(ns / 1_000_000);
}

function formatHHMMSS(ms: number): string {
  const d = new Date(ms);
  return [d.getHours(), d.getMinutes(), d.getSeconds()]
    .map((n) => String(n).padStart(2, "0"))
    .join(":");
}

function basename(path: string): string {
  return path.split("/").pop() ?? path;
}

export default function BacktestPage({ backtest, dispatch }: Props) {
  const theme = useTheme();
  const [configs, setConfigs] = useState<BacktestConfigOption[]>([]);
  const [selectedConfig, setSelectedConfig] = useState<string>("");
  const [configError, setConfigError] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Load the list of runnable backtest configs (TOML files under configs/
  // that declare a [backtest] section) once on mount.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${HTTP_BASE}/backtest/configs`);
        if (!res.ok) throw new Error(`server error: ${res.status}`);
        const data = await res.json();
        if (cancelled) return;
        const list = (data.configs ?? []) as BacktestConfigOption[];
        setConfigs(list);
        if (list.length > 0) setSelectedConfig(list[0].name);
      } catch (e) {
        if (!cancelled) setConfigError(`Could not load configs: ${String(e)}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // Poll REST while running — fallback in case the WS broadcast is missed.
  useEffect(() => {
    if (backtest.status !== "running") {
      if (pollRef.current !== null) {
        clearTimeout(pollRef.current);
        pollRef.current = null;
      }
      return;
    }
    function schedulePoll() {
      pollRef.current = setTimeout(async () => {
        try {
          const res = await fetch(`${HTTP_BASE}/backtest/result`);
          if (res.ok) {
            const data = await res.json();
            if (data.status !== "running") {
              dispatch({
                type: "BACKTEST_RESULT",
                payload: {
                  status: (data.status ?? "error") as BacktestStatus,
                  result: (data.result as BacktestResult) ?? null,
                  error: data.error != null ? String(data.error) : null,
                },
              });
            } else {
              schedulePoll();
            }
          } else {
            schedulePoll();
          }
        } catch {
          schedulePoll();
        }
      }, 1500);
    }
    schedulePoll();
    return () => {
      if (pollRef.current !== null) {
        clearTimeout(pollRef.current);
        pollRef.current = null;
      }
    };
  }, [backtest.status, dispatch]);

  async function handleRun() {
    setSubmitError(null);
    try {
      const res = await fetch(`${HTTP_BASE}/backtest/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ config: selectedConfig }),
      });
      if (res.status === 409) {
        setSubmitError("A backtest is already running.");
        return;
      }
      if (!res.ok) {
        setSubmitError(`Server error: ${res.status}`);
      }
    } catch (e) {
      setSubmitError(`Could not reach backend: ${String(e)}`);
    }
  }

  const result = backtest.result;
  const metrics = result?.metrics ?? null;

  const equityData = useMemo(() => {
    if (!result?.equity_curve) return [];
    return result.equity_curve.map(([ts_ns, pnl]) => ({ ts: nsToMs(ts_ns), pnl }));
  }, [result]);

  const lastPnl = equityData.length > 0 ? equityData[equityData.length - 1].pnl : 0;
  const pnlColor =
    lastPnl > 0 ? theme.palette.success.main : lastPnl < 0 ? theme.palette.error.main : theme.palette.info.main;

  const isRunning = backtest.status === "running";

  return (
    <Grid container spacing={2}>
      {/* Config panel */}
      <Grid size={{ xs: 12, lg: 4 }}>
        <Paper sx={{ p: 2, height: "100%" }}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1 }}>
            <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
              Strategy Backtest
            </Typography>
          </Box>
          <MStripe width={28} height={3} sx={{ mb: 2 }} />

          <Typography sx={{ fontSize: 12, color: "text.secondary", mb: 1.5 }}>
            Runs a deployed strategy through the real risk/OMS pipeline against
            a config under <code>configs/</code>.
          </Typography>

          <FormControl size="small" fullWidth disabled={configs.length === 0}>
            <InputLabel id="backtest-config-label">Config</InputLabel>
            <Select
              labelId="backtest-config-label"
              label="Config"
              value={selectedConfig}
              onChange={(e) => setSelectedConfig(e.target.value)}
            >
              {configs.map((c) => (
                <MenuItem key={c.name} value={c.name}>
                  {c.name}
                </MenuItem>
              ))}
            </Select>
          </FormControl>

          {configError && (
            <Alert severity="error" sx={{ mt: 1.5 }}>
              {configError}
            </Alert>
          )}
          {configs.length === 0 && !configError && (
            <Typography sx={{ mt: 1.5, fontSize: 12, color: "text.disabled" }}>
              No backtest-ready configs found under configs/ (need a [backtest] section).
            </Typography>
          )}

          <Box sx={{ mt: 2 }}>
            <Button
              variant="contained"
              fullWidth
              disabled={isRunning || !selectedConfig}
              onClick={handleRun}
              startIcon={isRunning ? <CircularProgress size={16} color="inherit" /> : undefined}
            >
              {isRunning ? "Running…" : "Run Backtest"}
            </Button>
          </Box>

          {submitError && (
            <Alert severity="error" sx={{ mt: 1.5 }}>
              {submitError}
            </Alert>
          )}
          {backtest.status === "error" && backtest.error && (
            <Alert severity="error" sx={{ mt: 1.5 }}>
              {backtest.error}
            </Alert>
          )}
          {backtest.status === "idle" && (
            <Typography sx={{ mt: 2, fontSize: 12, color: "text.disabled", textAlign: "center" }}>
              Pick a config and run. Results stream back over WebSocket.
            </Typography>
          )}
        </Paper>
      </Grid>

      {/* Results panel */}
      <Grid size={{ xs: 12, lg: 8 }}>
        {/* Metrics row */}
        {result && (
          <Paper sx={{ p: 2, mb: 2 }}>
            <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1 }}>
              <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
                {basename(result.config_path)} — Results
              </Typography>
              {backtest.completed_at && (
                <Typography sx={{ fontSize: 11, color: "text.disabled", ml: "auto" }}>
                  completed {new Date(backtest.completed_at).toLocaleTimeString()}
                </Typography>
              )}
            </Box>
            <MStripe width={28} height={3} sx={{ mb: 2 }} />
            {metrics ? (
              <Box sx={{ display: "flex", flexWrap: "wrap", gap: 1 }}>
                <MetricCard
                  label="Total Return"
                  value={fmtPct(metrics.total_return)}
                  color={metrics.total_return > 0 ? theme.palette.success.main : theme.palette.error.main}
                />
                <MetricCard label="Annualized Return" value={fmtPct(metrics.annualized_return)} />
                <MetricCard label="Annualized Vol" value={fmtPct(metrics.annualized_volatility)} />
                <MetricCard label="Sharpe" value={fmt(metrics.sharpe_ratio, 3)} />
                <MetricCard label="Sortino" value={fmt(metrics.sortino_ratio, 3)} />
                <MetricCard
                  label="Max Drawdown"
                  value={fmt(metrics.max_drawdown)}
                  color={metrics.max_drawdown > 0 ? theme.palette.error.main : undefined}
                />
                <MetricCard label="Max Drawdown %" value={fmtPct(metrics.max_drawdown_pct)} />
                <MetricCard label="Trades" value={String(metrics.num_trades)} />
                <MetricCard label="Win Rate" value={fmtPct(metrics.win_rate)} />
                <MetricCard label="Profit Factor" value={fmt(metrics.profit_factor)} />
                <MetricCard label="Fills" value={String(result.num_fills)} />
                <MetricCard label="Equity Pts" value={String(result.num_equity_points)} />
              </Box>
            ) : (
              <Typography sx={{ fontSize: 13, color: "text.disabled" }}>
                No metrics — backtest ran without any equity samples.
              </Typography>
            )}
          </Paper>
        )}

        {/* Equity curve chart */}
        <Paper sx={{ p: 2, height: 400, display: "flex", flexDirection: "column" }}>
          <Box sx={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", mb: 0.5 }}>
            <Box>
              <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
                P&amp;L Curve
              </Typography>
              <MStripe width={28} height={3} sx={{ mt: 0.5 }} />
            </Box>
            {result && (
              <Typography
                sx={{
                  fontSize: 22,
                  fontWeight: 800,
                  lineHeight: 1,
                  fontVariantNumeric: "tabular-nums",
                  color: pnlColor,
                }}
              >
                {fmtSigned(lastPnl)}
              </Typography>
            )}
          </Box>

          <Box sx={{ flex: 1, minHeight: 0 }}>
            {isRunning && (
              <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", gap: 1.5 }}>
                <CircularProgress size={20} />
                <Typography sx={{ fontSize: 13, color: "text.secondary" }}>Running backtest…</Typography>
              </Box>
            )}
            {!isRunning && equityData.length === 0 && (
              <Typography variant="body2" color="text.disabled" sx={{ mt: 6, textAlign: "center" }}>
                {backtest.status === "idle" ? "Run a backtest to see the P&L curve." : "No equity data."}
              </Typography>
            )}
            {!isRunning && equityData.length > 0 && (
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={equityData} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                  <defs>
                    <linearGradient id="pnlFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={pnlColor} stopOpacity={0.28} />
                      <stop offset="100%" stopColor={pnlColor} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="2 4" stroke={theme.palette.divider} vertical={false} />
                  <XAxis
                    dataKey="ts"
                    tickFormatter={formatHHMMSS}
                    tick={{ fontSize: 10, fill: theme.palette.text.secondary }}
                    tickLine={false}
                    axisLine={{ stroke: theme.palette.divider }}
                    minTickGap={56}
                  />
                  <YAxis
                    tick={{ fontSize: 10, fill: theme.palette.text.secondary, fontVariant: "tabular-nums" }}
                    tickLine={false}
                    axisLine={false}
                    width={62}
                    tickFormatter={(v: number) => v.toFixed(2)}
                  />
                  <Tooltip
                    formatter={(v: number) => [fmtSigned(v), "Total PnL"]}
                    labelFormatter={(l: number) => formatHHMMSS(l)}
                    contentStyle={{
                      background: theme.palette.mode === "dark" ? "#161616" : "#fff",
                      border: `1px solid ${theme.palette.divider}`,
                      fontSize: 12,
                    }}
                  />
                  <ReferenceLine y={0} stroke={theme.palette.divider} />
                  <Area
                    type="monotone"
                    dataKey="pnl"
                    stroke={pnlColor}
                    strokeWidth={2}
                    fill="url(#pnlFill)"
                    dot={false}
                    activeDot={{ r: 3, strokeWidth: 0 }}
                    isAnimationActive={false}
                  />
                </AreaChart>
              </ResponsiveContainer>
            )}
          </Box>
        </Paper>
      </Grid>
    </Grid>
  );
}
