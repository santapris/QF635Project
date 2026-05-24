import AppBar from "@mui/material/AppBar";
import Toolbar from "@mui/material/Toolbar";
import Typography from "@mui/material/Typography";
import Button from "@mui/material/Button";
import Chip from "@mui/material/Chip";
import Box from "@mui/material/Box";
import { NavLink } from "react-router-dom";
import type { ConnectionStatus } from "../store/pipelineStore";
import ThemeToggle from "./ThemeToggle";

const STATUS_COLOR: Record<ConnectionStatus, "success" | "warning" | "error" | "default"> = {
  connected: "success",
  connecting: "warning",
  reconnecting: "warning",
  disconnected: "error",
};

interface NavBarProps {
  status: ConnectionStatus;
}

export default function NavBar({ status }: NavBarProps) {
  return (
    <AppBar position="fixed">
      <Toolbar variant="dense">
        <Typography variant="h6" sx={{ fontWeight: 700, letterSpacing: 1, mr: 4 }}>
          QF635
        </Typography>
        <Box sx={{ display: "flex", gap: 1, flexGrow: 1 }}>
          {[
            { to: "/", label: "Dashboard" },
            { to: "/logs", label: "Logs" },
            { to: "/killswitch", label: "Kill Switch" },
            { to: "/backtest", label: "Backtest" },
          ].map(({ to, label }) => (
            <Button
              key={to}
              component={NavLink}
              to={to}
              color="inherit"
              size="small"
              sx={{
                opacity: 0.7,
                "&.active": { opacity: 1, textDecoration: "underline" },
              }}
            >
              {label}
            </Button>
          ))}
        </Box>
        <Chip
          label={status}
          color={STATUS_COLOR[status]}
          size="small"
          sx={{ fontWeight: 600, textTransform: "capitalize", mr: 1 }}
        />
        <ThemeToggle />
      </Toolbar>
    </AppBar>
  );
}
