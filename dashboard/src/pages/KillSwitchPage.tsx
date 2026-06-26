import { useState } from "react";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Stack from "@mui/material/Stack";
import Dialog from "@mui/material/Dialog";
import DialogTitle from "@mui/material/DialogTitle";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogActions from "@mui/material/DialogActions";
import WarningRoundedIcon from "@mui/icons-material/WarningRounded";
import type { KillSwitchState, PipelineAction } from "../store/pipelineStore";
import { HTTP_BASE } from "../hooks/useStatePoll";
import { formatTs } from "../utils/formatTs";

// Modern, neutral status palette (no brand theme here).
const ARMED = { base: "#10b981", soft: "rgba(16,185,129,0.10)" };
const KILL = { base: "#f43f5e", soft: "rgba(244,63,94,0.10)" };

interface Props {
  killSwitch: KillSwitchState | null;
  dispatch: React.Dispatch<PipelineAction>;
}

const SHELL = {
  // `main` is a flex column, so stretch to fill it rather than relying on height:100%.
  flexGrow: 1,
  alignSelf: "stretch",
  minHeight: 0,
  width: "100%",
  display: "flex",
  flexDirection: "column",
  alignItems: "center",
  justifyContent: "center",
  borderRadius: 4,
  border: "1px solid",
  borderColor: "divider",
} as const;

// Inner content stays readable; only the Paper stretches to fill the page.
const CONTENT_MAX = 480;

export default function KillSwitchPage({ killSwitch, dispatch }: Props) {
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [resetting, setResetting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // No snapshot yet (page opened before the first poll landed).
  if (killSwitch === null) {
    return (
      <Paper sx={{ ...SHELL, p: 4 }}>
        <Box sx={{ width: "100%", maxWidth: CONTENT_MAX }}>
          <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>Kill Switch</Typography>
          <Alert severity="info" sx={{ borderRadius: 2 }}>Loading kill-switch state…</Alert>
        </Box>
      </Paper>
    );
  }

  // Backend has no risk engine wired — the latch is not observable here.
  if (!killSwitch.available) {
    return (
      <Paper sx={{ ...SHELL, p: 4 }}>
        <Box sx={{ width: "100%", maxWidth: CONTENT_MAX }}>
          <Typography variant="h6" sx={{ fontWeight: 700, mb: 1 }}>Kill Switch</Typography>
          <Alert severity="warning" sx={{ borderRadius: 2 }}>
            No risk engine is connected to this dashboard, so the kill-switch state
            is unavailable.
          </Alert>
        </Box>
      </Paper>
    );
  }

  const { engaged } = killSwitch;
  const c = engaged ? KILL : ARMED;

  const applyState = (data: Record<string, unknown>) => {
    dispatch({
      type: "KILL_SWITCH_SNAPSHOT",
      payload: {
        available: Boolean(data.available),
        engaged: Boolean(data.engaged),
        triggered_by: String(data.triggered_by ?? ""),
        reason: String(data.reason ?? ""),
        ts: data.triggered_at_ns ? Number(data.triggered_at_ns) / 1_000_000 : null,
      },
    });
  };

  const doEngage = async () => {
    setError(null);
    try {
      const res = await fetch(`${HTTP_BASE}/command/killswitch/engage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ triggered_by: "operator", reason: "manual engage via dashboard" }),
      });
      if (!res.ok) {
        setError(`Engage failed (HTTP ${res.status}).`);
        return;
      }
      applyState(await res.json());
    } catch {
      setError("Engage failed: could not reach the trading process.");
    }
  };

  const doReset = async () => {
    setResetting(true);
    setError(null);
    try {
      const res = await fetch(`${HTTP_BASE}/command/killswitch/reset`, { method: "POST" });
      if (!res.ok) {
        setError(`Reset failed (HTTP ${res.status}).`);
        return;
      }
      // Reset doesn't publish a bus event, so update the store from the response.
      applyState(await res.json());
      setConfirmOpen(false);
    } catch {
      setError("Reset failed: could not reach the trading process.");
    } finally {
      setResetting(false);
    }
  };

  return (
    <Paper elevation={0} sx={{ ...SHELL, p: 4, position: "relative", overflow: "hidden" }}>
      <Box
        sx={{
          display: "flex", flexDirection: "column", alignItems: "center", gap: 3.5,
          width: "100%", maxWidth: CONTENT_MAX,
        }}
      >
        <Typography
          variant="overline"
          sx={{ color: "text.secondary", letterSpacing: "3px", fontWeight: 600 }}
        >
          Kill Switch
        </Typography>

        {/* Status disc — flat, ringed, no glow */}
        <Box
          sx={{
            width: 84, height: 84, borderRadius: "50%",
            display: "grid", placeItems: "center", my: 1,
            bgcolor: c.soft,
            border: `2px solid ${c.base}`,
            color: c.base, fontSize: 32, lineHeight: 1,
          }}
        >
          {engaged ? <WarningRoundedIcon sx={{ fontSize: 44 }} /> : "✓"}
        </Box>

        {/* State label + description */}
        <Box sx={{ textAlign: "center" }}>
          <Typography
            sx={{
              fontWeight: 800, fontSize: "1.75rem", letterSpacing: "0.5px",
              color: c.base, lineHeight: 1.1,
            }}
          >
            {engaged ? "HALTED" : "LIVE"}
          </Typography>
          <Typography variant="body2" sx={{ color: "text.secondary", mt: 0.75, maxWidth: 340 }}>
            {engaged
              ? "All new orders are rejected and resting orders cancelled until an operator re-arms."
              : "Trading is permitted. The switch latches automatically on a KILL-severity rule."}
          </Typography>
        </Box>

        {engaged && (
          <Stack
            spacing={1.25}
            sx={{
              width: "100%", p: 2, borderRadius: 2.5,
              bgcolor: "action.hover", border: "1px solid", borderColor: "divider",
            }}
          >
            <Detail label="Triggered by" value={killSwitch.triggered_by || "—"} />
            <Detail label="Reason" value={killSwitch.reason || "—"} />
            <Detail
              label="At"
              value={killSwitch.ts != null ? formatTs(killSwitch.ts) : "—"}
            />
          </Stack>
        )}

        {error && (
          <Alert severity="error" sx={{ width: "100%", borderRadius: 2 }}>{error}</Alert>
        )}

        {/* Action */}
        {engaged ? (
          <Button
            fullWidth
            disableElevation
            disabled={resetting}
            onClick={() => setConfirmOpen(true)}
            startIcon={<Box component="span" sx={{ fontSize: 22, lineHeight: 1 }}>↻</Box>}
            sx={{
              py: 2, borderRadius: 3, textTransform: "none",
              fontWeight: 700, fontSize: "1.05rem", letterSpacing: "0.3px",
              color: ARMED.base,
              border: `1.5px solid ${ARMED.base}`,
              bgcolor: ARMED.soft,
              "& .MuiButton-startIcon": {
                m: 0, mr: 1, display: "flex", alignItems: "center",
                transition: "transform 0.2s cubic-bezier(.34,1.56,.64,1)",
              },
              "&:hover": { bgcolor: "rgba(16,185,129,0.18)" },
              "&:hover .MuiButton-startIcon": { transform: "scale(1.3)" },
            }}
          >
            Reset &amp; Re-arm
          </Button>
        ) : (
          <Button
            fullWidth
            disableElevation
            onClick={doEngage}
            startIcon={<WarningRoundedIcon sx={{ fontSize: "26px !important" }} />}
            sx={{
              py: 2.25, borderRadius: 3, textTransform: "none",
              fontWeight: 800, fontSize: "1.1rem", letterSpacing: "0.4px", color: "#fff",
              bgcolor: KILL.base,
              // Center icon + text on the same line and let the icon grow on hover.
              "& .MuiButton-startIcon": {
                m: 0, mr: 1, display: "flex", alignItems: "center",
                transition: "transform 0.2s cubic-bezier(.34,1.56,.64,1)",
                transformOrigin: "center",
              },
              "&:hover": { bgcolor: KILL.base },
              "&:hover .MuiButton-startIcon": { transform: "scale(1.35)" },
            }}
          >
            Engage Kill Switch
          </Button>
        )}
      </Box>

      <Dialog
        open={confirmOpen}
        onClose={() => !resetting && setConfirmOpen(false)}
        slotProps={{ paper: { sx: { borderRadius: 3, maxWidth: 380 } } }}
      >
        <DialogTitle sx={{ fontWeight: 700 }}>Reset kill switch?</DialogTitle>
        <DialogContent>
          <DialogContentText>
            This re-arms the system and allows new orders to flow again. Only do
            this once you have addressed the condition that triggered the halt
            (<strong>{killSwitch.triggered_by || "unknown"}</strong>).
          </DialogContentText>
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={() => setConfirmOpen(false)} disabled={resetting} sx={{ textTransform: "none" }}>
            Cancel
          </Button>
          <Button
            onClick={doReset}
            variant="contained"
            disableElevation
            disabled={resetting}
            sx={{
              textTransform: "none", borderRadius: 2,
              bgcolor: ARMED.base, "&:hover": { bgcolor: "#0ea372" },
            }}
          >
            {resetting ? "Resetting…" : "Confirm reset"}
          </Button>
        </DialogActions>
      </Dialog>
    </Paper>
  );
}

function Detail({ label, value }: { label: string; value: string }) {
  return (
    <Box sx={{ display: "flex", gap: 2 }}>
      <Typography variant="body2" sx={{ minWidth: 96, color: "text.secondary" }}>
        {label}
      </Typography>
      <Typography variant="body2" sx={{ fontWeight: 500, wordBreak: "break-word" }}>
        {value}
      </Typography>
    </Box>
  );
}
