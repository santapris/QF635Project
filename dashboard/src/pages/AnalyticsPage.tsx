import Grid from "@mui/material/Grid";
import type { PipelineState } from "../store/pipelineStore";
import QuoteChainChart from "../components/analytics/QuoteChainChart";
import AnalyticsDriversChart from "../components/analytics/AnalyticsDriversChart";
import LiveGaugesPanel from "../components/analytics/LiveGaugesPanel";
import SpreadDecompositionChart from "../components/analytics/SpreadDecompositionChart";
import AnalyticsTable from "../components/analytics/AnalyticsTable";

interface Props {
  state: PipelineState;
}

export default function AnalyticsPage({ state }: Props) {
  const { analytics, analyticsHistory } = state;

  return (
    // Fixed pixel heights per row. ResponsiveContainer needs a parent with a
    // *definite* height; `minHeight` leaves it indefinite, which makes the chart
    // creep a few px taller on every mount/resize (the classic recharts growth
    // bug). A fixed `height` pins each panel so the layout is stable across
    // re-opens.
    <Grid container spacing={2}>
      {/* Row 1: Quote Decision Chain — full width */}
      <Grid size={{ xs: 12 }} sx={{ height: 300 }}>
        <QuoteChainChart history={analyticsHistory} />
      </Grid>

      {/* Row 2: Analytics Drivers (8/12) + Live Gauges (4/12) */}
      <Grid size={{ xs: 12, lg: 8 }} sx={{ height: 260 }}>
        <AnalyticsDriversChart history={analyticsHistory} />
      </Grid>
      <Grid size={{ xs: 12, lg: 4 }} sx={{ height: 260 }}>
        <LiveGaugesPanel analytics={analytics} />
      </Grid>

      {/* Row 3: Spread Decomposition — full width */}
      <Grid size={{ xs: 12 }} sx={{ height: 240 }}>
        <SpreadDecompositionChart history={analyticsHistory} />
      </Grid>

      {/* Row 4: Analytics Log — full width */}
      <Grid size={{ xs: 12 }} sx={{ height: 320 }}>
        <AnalyticsTable history={analyticsHistory} />
      </Grid>
    </Grid>
  );
}
