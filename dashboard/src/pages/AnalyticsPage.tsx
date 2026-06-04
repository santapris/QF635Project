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
    <Grid container spacing={2}>
      {/* Row 1: Quote Decision Chain — full width */}
      <Grid size={{ xs: 12 }} sx={{ minHeight: 280 }}>
        <QuoteChainChart history={analyticsHistory} />
      </Grid>

      {/* Row 2: Analytics Drivers (8/12) + Live Gauges (4/12) */}
      <Grid size={{ xs: 12, lg: 8 }} sx={{ minHeight: 240 }}>
        <AnalyticsDriversChart history={analyticsHistory} />
      </Grid>
      <Grid size={{ xs: 12, lg: 4 }} sx={{ minHeight: 240 }}>
        <LiveGaugesPanel analytics={analytics} />
      </Grid>

      {/* Row 3: Spread Decomposition — full width */}
      <Grid size={{ xs: 12 }} sx={{ minHeight: 220 }}>
        <SpreadDecompositionChart history={analyticsHistory} />
      </Grid>

      {/* Row 4: Analytics Log — full width */}
      <Grid size={{ xs: 12 }} sx={{ minHeight: 200 }}>
        <AnalyticsTable history={analyticsHistory} />
      </Grid>
    </Grid>
  );
}
