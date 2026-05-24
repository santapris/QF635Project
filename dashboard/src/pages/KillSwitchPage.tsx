import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Alert from "@mui/material/Alert";

export default function KillSwitchPage() {
  return (
    <Paper sx={{ p: 4, maxWidth: 600, mx: "auto", mt: 4 }}>
      <Typography variant="h5" gutterBottom>
        Kill Switch
      </Typography>
      <Alert severity="info">
        Kill switch controls are not yet implemented (Phase C). The backend endpoint{" "}
        <code>POST /api/killswitch</code> will be wired up in a future release.
      </Alert>
    </Paper>
  );
}
