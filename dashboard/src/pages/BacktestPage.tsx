import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Alert from "@mui/material/Alert";

export default function BacktestPage() {
  return (
    <Paper sx={{ p: 4, maxWidth: 600, mx: "auto", mt: 4 }}>
      <Typography variant="h5" gutterBottom>
        Backtest
      </Typography>
      <Alert severity="info">
        Backtest runner UI is not yet implemented (Phase C+). Results will be streamed over the
        same WebSocket connection once the backend supports the <code>backtest</code> topic.
      </Alert>
    </Paper>
  );
}
