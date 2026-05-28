import Grid from "@mui/material/Grid";
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
  return (
    <Grid container spacing={2} sx={{ height: "100%" }}>
      {/* Top row: market data + PnL chart */}
      <Grid size={{ xs: 12, lg: 4 }} sx={{ minHeight: 220 }}>
        <MarketDataPanel
          ticks={state.ticks}
          tickHistory={state.tickHistory}
          recentTrades={state.recentTrades}
        />
      </Grid>
      <Grid size={{ xs: 12, lg: 8 }} sx={{ minHeight: 220 }}>
        <PnlChart pnlHistory={state.pnlHistory} />
      </Grid>

      {/* Middle row: positions + account */}
      <Grid size={{ xs: 12, lg: 8 }} sx={{ minHeight: 200 }}>
        <PositionPanel positions={state.positions} venueNet={state.venueNet} />
      </Grid>
      <Grid size={{ xs: 12, lg: 4 }} sx={{ minHeight: 200 }}>
        <AccountPanel account={state.account} />
      </Grid>

      {/* Individual resting orders — authoritative OMS snapshot. */}
      <Grid size={{ xs: 12, lg: 8 }} sx={{ minHeight: 200 }}>
        <OpenOrdersPanel openOrders={state.openOrders} />
      </Grid>

      {/* Bottom row: signals + orders */}
      <Grid size={{ xs: 12, lg: 6 }} sx={{ minHeight: 300 }}>
        <SignalsPanel signals={state.signals} />
      </Grid>
      <Grid size={{ xs: 12, lg: 6 }} sx={{ minHeight: 300 }}>
        <OrdersPanel orders={state.orders} />
      </Grid>
    </Grid>
  );
}
