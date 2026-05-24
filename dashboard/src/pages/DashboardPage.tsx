import Grid from "@mui/material/Grid";
import type { PipelineState } from "../store/pipelineStore";
import MarketDataPanel from "../components/MarketDataPanel";
import SignalsPanel from "../components/SignalsPanel";
import OrdersPanel from "../components/OrdersPanel";
import PositionPanel from "../components/PositionPanel";
import PnlChart from "../components/PnlChart";

interface Props {
  state: PipelineState;
}

export default function DashboardPage({ state }: Props) {
  return (
    <Grid container spacing={2} sx={{ height: "100%" }}>
      {/* Top row: market data + PnL chart */}
      <Grid size={{ xs: 12, md: 4 }} sx={{ minHeight: 220 }}>
        <MarketDataPanel ticks={state.ticks} recentTrades={state.recentTrades} />
      </Grid>
      <Grid size={{ xs: 12, md: 8 }} sx={{ minHeight: 220 }}>
        <PnlChart pnlHistory={state.pnlHistory} />
      </Grid>

      {/* Middle row: positions */}
      <Grid size={{ xs: 12 }} sx={{ minHeight: 200 }}>
        <PositionPanel positions={state.positions} />
      </Grid>

      {/* Bottom row: signals + orders */}
      <Grid size={{ xs: 12, md: 6 }} sx={{ minHeight: 300 }}>
        <SignalsPanel signals={state.signals} />
      </Grid>
      <Grid size={{ xs: 12, md: 6 }} sx={{ minHeight: 300 }}>
        <OrdersPanel orders={state.orders} />
      </Grid>
    </Grid>
  );
}
