import AppBar from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import Button from "@mui/material/Button";
import Box from "@mui/material/Box";
import FiberManualRecordIcon from "@mui/icons-material/FiberManualRecord";
import { NavLink } from "react-router-dom";
import type { ConnectionStatus } from "../store/pipelineStore";
import ThemeToggle from "./ThemeToggle";
import MStripe from "./bmw/MStripe";

const STATUS_COLOR: Record<ConnectionStatus, string> = {
  connected: "#0fa336",
  connecting: "#f4b400",
  reconnecting: "#f4b400",
  disconnected: "#e22718",
};

const NAV_ITEMS = [
  { to: "/", label: "Dashboard" },
  { to: "/analytics", label: "Analytics" },
  { to: "/logs", label: "Logs" },
  { to: "/killswitch", label: "Kill Switch" },
  { to: "/backtest", label: "Backtest" },
];

interface NavBarProps {
  status: ConnectionStatus;
}

export default function NavBar({ status }: NavBarProps) {
  return (
    <AppBar position="fixed">
      <Toolbar variant="dense" sx={{ gap: 1, minHeight: 48 }}>
        {/* Brand mark: M tricolor tick + wordmark */}
        <Box sx={{ display: "flex", alignItems: "center", gap: 1.25, mr: { xs: 1.5, md: 4 } }}>
          <MStripe width={18} height={20} />
          <Typography
            sx={{
              fontWeight: 800,
              fontSize: 18,
              letterSpacing: "1px",
              color: "text.primary",
              lineHeight: 1,
            }}
          >
            QF635
          </Typography>
        </Box>

        <Box sx={{ display: "flex", gap: { xs: 0, md: 0.5 }, flexGrow: 1, overflow: "auto" }}>
          {NAV_ITEMS.map(({ to, label }) => (
            <Button
              key={to}
              component={NavLink}
              to={to}
              end={to === "/"}
              color="inherit"
              size="small"
              sx={{
                px: 1.5,
                fontSize: 12,
                fontWeight: 400,
                letterSpacing: "0.5px",
                textTransform: "uppercase",
                color: "text.secondary",
                borderBottom: "2px solid transparent",
                borderRadius: 0,
                whiteSpace: "nowrap",
                "&.active": {
                  color: "text.primary",
                  fontWeight: 700,
                  borderBottomColor: "text.primary",
                },
                "&:hover": { color: "text.primary", backgroundColor: "transparent" },
              }}
            >
              {label}
            </Button>
          ))}
        </Box>

        {/* Live link status — dot + uppercase machined label */}
        <Box sx={{ display: "flex", alignItems: "center", gap: 0.75, mr: 1 }}>
          <FiberManualRecordIcon sx={{ fontSize: 9, color: STATUS_COLOR[status] }} />
          <Typography
            sx={{
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: "1.5px",
              textTransform: "uppercase",
              color: "text.secondary",
              display: { xs: "none", sm: "block" },
            }}
          >
            {status}
          </Typography>
        </Box>

        <ThemeToggle />
      </Toolbar>
      {/* Signature M-stripe pinned under the nav bar */}
      <MStripe height={3} />
    </AppBar>
  );
}
