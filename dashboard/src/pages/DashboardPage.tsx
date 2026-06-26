import { useMemo, useState } from "react";
import Grid from "@mui/material/Grid";
import Box from "@mui/material/Box";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import Typography from "@mui/material/Typography";
import type { PipelineState } from "../store/pipelineStore";
import MarketDataPanel from "../components/MarketDataPanel";
import SignalsPanel from "../components/SignalsPanel";
import OrdersPanel from "../components/OrdersPanel";
import PositionPanel from "../components/PositionPanel";
import OpenOrdersPanel from "../components/OpenOrdersPanel";
import RiskPanel from "../components/RiskPanel";
import FillsPanel from "../components/FillsPanel";
import RoutingPanel from "../components/RoutingPanel";
import PnlChart from "../components/PnlChart";
import AccountPanel from "../components/AccountPanel";
import MStripe from "../components/bmw/MStripe";
import SectionLabel from "../components/bmw/SectionLabel";
import SpecCell from "../components/bmw/SpecCell";
import GaugeStrip from "../components/bmw/GaugeStrip";
import LatencyPanel from "../components/LatencyPanel";

interface Props {
  state: PipelineState;
}

/** Sum a numeric field across position rows, tolerating string values. */
function sumField(rows: { unrealized_pnl?: string; realized_pnl?: string }[], key: "unrealized_pnl" | "realized_pnl"): number {
  return rows.reduce((acc, r) => acc + (parseFloat(r[key] ?? "0") || 0), 0);
}

function fmtSigned(n: number, digits = 2): string {
  const sign = n > 0 ? "+" : "";
  return `${sign}${n.toFixed(digits)}`;
}

function pnlColor(n: number): string | undefined {
  if (n > 0) return "success.main";
  if (n < 0) return "error.main";
  return undefined;
}

export default function DashboardPage({ state }: Props) {
  const [tab, setTab] = useState(0);
  const nSignals = state.signals.length;
  const nRisk = state.riskDecisions.length;
  const nOrders = state.orders.length;
  const nOpenOrders = state.openOrders.length;
  const nRoutings = state.routings.length;
  const nFills = state.fills.length;

  const positionRows = useMemo(() => Object.values(state.positions), [state.positions]);

  // KPI roll-ups derived from live pipeline state.
  const unrealized = sumField(positionRows, "unrealized_pnl");
  const realized = sumField(positionRows, "realized_pnl");
  const totalPnl = unrealized + realized;

  // Net exposure = count of instruments the venue reports a non-flat position on.
  const liveInstruments = state.venueNet.filter((v) => parseFloat(v.net_quantity) !== 0).length;

  // Account equity = total of all non-zero balances (free + locked).
  const equity = useMemo(() => {
    if (!state.account) return null;
    return state.account.balances.reduce(
      (acc, b) => acc + (parseFloat(b.free) || 0) + (parseFloat(b.locked) || 0),
      0,
    );
  }, [state.account]);

  // Tab order mirrors the pipeline: intent → risk gate → order lifecycle → routing → fills.
  const tabs = [
    { label: `Open Orders · ${nOpenOrders}`, node: <OpenOrdersPanel openOrders={state.openOrders} /> },
    { label: `Signals · ${nSignals}`, node: <SignalsPanel signals={state.signals} /> },
    { label: `Risk · ${nRisk}`, node: <RiskPanel riskDecisions={state.riskDecisions} /> },
    { label: `Orders · ${nOrders}`, node: <OrdersPanel orders={state.orders} /> },
    { label: `Routing · ${nRoutings}`, node: <RoutingPanel routings={state.routings} /> },
    { label: `Fills · ${nFills}`, node: <FillsPanel fills={state.fills} /> },
  ];

  return (
    <Box sx={{ display: "flex", flexDirection: "column", gap: 3 }}>
      {/* ── Hero band ───────────────────────────────────────────── */}
      <Box>
        <MStripe width={56} height={5} sx={{ mb: 1.5 }} />
        <Typography
          component="h1"
          sx={{
            fontWeight: 800,
            fontSize: { xs: 34, sm: 44, md: 52 },
            lineHeight: 1,
            letterSpacing: "-0.5px",
            textTransform: "uppercase",
            color: "text.primary",
          }}
        >
          Dashboard
        </Typography>
        <Typography
          sx={{
            mt: 1,
            maxWidth: 720,
            fontWeight: 300,
            fontSize: 15,
            lineHeight: 1.5,
            color: "text.secondary",
          }}
        >
          Live operator telemetry across the trading pipeline — market data, signals,
          risk-cleared orders, fills and position P&amp;L, streamed off the event bus in real time.
        </Typography>
      </Box>

      {/* ── KPI spec-cell strip ─────────────────────────────────── */}
      <Grid container spacing={1.5}>
        <Grid size={{ xs: 6, sm: 4, md: 2 }}>
          <SpecCell
            label="Total P&L"
            value={fmtSigned(totalPnl, 2)}
            unit="USDT"
            valueColor={pnlColor(totalPnl)}
            accent
          />
        </Grid>
        <Grid size={{ xs: 6, sm: 4, md: 2 }}>
          <SpecCell
            label="Unrealized"
            value={fmtSigned(unrealized, 2)}
            unit="USDT"
            valueColor={pnlColor(unrealized)}
          />
        </Grid>
        <Grid size={{ xs: 6, sm: 4, md: 2 }}>
          <SpecCell
            label="Realized"
            value={fmtSigned(realized, 2)}
            unit="USDT"
            valueColor={pnlColor(realized)}
          />
        </Grid>
        <Grid size={{ xs: 6, sm: 4, md: 2 }}>
          <SpecCell
            label="Open Orders"
            value={nOpenOrders}
            unit={`across ${liveInstruments} instrument${liveInstruments === 1 ? "" : "s"}`}
          />
        </Grid>
        <Grid size={{ xs: 6, sm: 4, md: 2 }}>
          <SpecCell label="Fills" value={nFills} unit="recent" />
        </Grid>
        <Grid size={{ xs: 6, sm: 4, md: 2 }}>
          <SpecCell
            label="Equity"
            value={equity == null ? "—" : equity.toFixed(2)}
            unit={equity == null ? "" : "USDT"}
          />
        </Grid>
      </Grid>

      {/* ── Microstructure gauge strip ──────────────────────────── */}
      <Box>
        <SectionLabel>Microstructure</SectionLabel>
        <GaugeStrip analytics={state.analytics} />
      </Box>

      {/* ── Telemetry band: P&L curve + live market data ────────── */}
      <Box>
        <SectionLabel>Performance &amp; Market</SectionLabel>
        <Grid container spacing={2}>
          <Grid size={{ xs: 12, lg: 8 }} sx={{ minHeight: 280 }}>
            <PnlChart pnlHistory={state.pnlHistory} />
          </Grid>
          <Grid size={{ xs: 12, lg: 4 }} sx={{ minHeight: 280 }}>
            <MarketDataPanel
              ticks={state.ticks}
              tickHistory={state.tickHistory}
              recentTrades={state.recentTrades}
            />
          </Grid>
        </Grid>
      </Box>

      {/* ── Position band: positions + account ──────────────────── */}
      <Box>
        <SectionLabel>Position &amp; Account</SectionLabel>
        <Grid container spacing={2}>
          <Grid size={{ xs: 12, lg: 8 }} sx={{ minHeight: 240 }}>
            <PositionPanel positions={state.positions} venueNet={state.venueNet} strategies={state.strategies} />
          </Grid>
          <Grid size={{ xs: 12, lg: 4 }} sx={{ minHeight: 240 }}>
            <AccountPanel account={state.account} />
          </Grid>
        </Grid>
      </Box>

      {/* ── Order-flow band: full pipeline audit trail ──────────── */}
      <Box>
        <SectionLabel
          action={
            <Tabs
              value={tab}
              onChange={(_, v) => setTab(v)}
              variant="scrollable"
              scrollButtons="auto"
              sx={{ minHeight: 32, border: "none", maxWidth: { xs: 220, sm: 460, md: 640 } }}
            >
              {tabs.map((t) => (
                <Tab key={t.label} sx={{ minHeight: 32, py: 0 }} label={t.label} />
              ))}
            </Tabs>
          }
        >
          Order Flow
        </SectionLabel>
        <Box sx={{ minHeight: 360, display: "flex", flexDirection: "column" }}>
          {tabs[tab].node}
        </Box>
      </Box>

      {/* ── Pipeline latency ────────────────────────────────────── */}
      <Box>
        <SectionLabel>Pipeline Latency</SectionLabel>
        <LatencyPanel latency={state.latency} />
      </Box>
    </Box>
  );
}
