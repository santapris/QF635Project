import { useState } from "react";
import Grid from "@mui/material/Grid";
import Box from "@mui/material/Box";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import type { PipelineState } from "../store/pipelineStore";
import MarketDataPanel from "../components/MarketDataPanel";
import SignalsPanel from "../components/SignalsPanel";
import OrdersPanel from "../components/OrdersPanel";
import PositionPanel from "../components/PositionPanel";
import OpenOrdersPanel from "../components/OpenOrdersPanel";
import PnlChart from "../components/PnlChart";
import AccountPanel from "../components/AccountPanel";

interface Props {
  state: PipelineState;
}

export default function DashboardPage({ state }: Props) {
  const [tab, setTab] = useState(0);
  const nSignals = state.signals.length;
  const nOrders = state.orders.length;
  const nOpenOrders = state.openOrders.length;

  return (
    <Grid container spacing={2} sx={{ height: "100%" }}>
      {/* Top row: market data + PnL chart */}
      <Grid size={{ xs: 12, lg: 8 }} sx={{ minHeight: 220 }}>
        <PnlChart pnlHistory={state.pnlHistory} />
      </Grid>
      <Grid size={{ xs: 12, lg: 4 }} sx={{ minHeight: 220 }}>
        <MarketDataPanel
          ticks={state.ticks}
          tickHistory={state.tickHistory}
          recentTrades={state.recentTrades}
        />
      </Grid>

      {/* Middle row: positions + account */}
      <Grid size={{ xs: 12, lg: 8 }} sx={{ minHeight: 200 }}>
        <PositionPanel positions={state.positions} venueNet={state.venueNet} />
      </Grid>
      <Grid size={{ xs: 12, lg: 4 }} sx={{ minHeight: 200 }}>
        <AccountPanel account={state.account} />
      </Grid>

      {/* Bottom row: signals + orders + open orders with tabs */}
      <Grid size={{ xs: 12, lg: 12 }} sx={{ minHeight: 300 }}>
        <Box sx={{ height: "100%", display: "flex", flexDirection: "column", gap: 1 }}>
          <Tabs value={tab} onChange={(_, v) => setTab(v)}>
            <Tab label={`Signals (${nSignals})`} />
            <Tab label={`Orders (${nOrders})`} />
            <Tab label={`Open Orders (${nOpenOrders})`} />
          </Tabs>
          <Box sx={{ flex: 1, minHeight: 0 }}>
            {tab === 0 && <OpenOrdersPanel openOrders={state.openOrders} />}
            {tab === 1 && <SignalsPanel signals={state.signals} />}
            {tab === 2 && <OrdersPanel orders={state.orders} />}
          </Box>
        </Box>
      </Grid>
    </Grid>
  );
}
