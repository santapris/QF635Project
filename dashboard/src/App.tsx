import { useReducer, useCallback } from "react";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import CssBaseline from "@mui/material/CssBaseline";
import Box from "@mui/material/Box";
import { createTheme, ThemeProvider } from "@mui/material/styles";
import { pipelineReducer, initialState } from "./store/pipelineStore";
import { usePipelineSocket } from "./hooks/usePipelineSocket";
import NavBar from "./components/NavBar";
import DashboardPage from "./pages/DashboardPage";
import LogsPage from "./pages/LogsPage";
import KillSwitchPage from "./pages/KillSwitchPage";
import BacktestPage from "./pages/BacktestPage";

const theme = createTheme({
  palette: { mode: "dark" },
  typography: { fontSize: 13 },
});

const TOOLBAR_HEIGHT = 48; // dense AppBar

function AppInner() {
  const [state, dispatch] = useReducer(pipelineReducer, initialState);
  usePipelineSocket(dispatch);

  const clearLogs = useCallback(() => dispatch({ type: "CLEAR_LOGS" }), []);

  return (
    <Box sx={{ display: "flex", flexDirection: "column", minHeight: "100vh" }}>
      <NavBar status={state.status} />
      <Box
        component="main"
        sx={{
          flexGrow: 1,
          mt: `${TOOLBAR_HEIGHT}px`,
          p: 2,
          overflow: "auto",
        }}
      >
        <Routes>
          <Route path="/" element={<DashboardPage state={state} />} />
          <Route path="/logs" element={<LogsPage logs={state.logs} onClear={clearLogs} />} />
          <Route path="/killswitch" element={<KillSwitchPage />} />
          <Route path="/backtest" element={<BacktestPage />} />
        </Routes>
      </Box>
    </Box>
  );
}

export default function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <BrowserRouter>
        <AppInner />
      </BrowserRouter>
    </ThemeProvider>
  );
}
