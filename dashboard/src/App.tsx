import { useReducer, useCallback, useEffect, useMemo, useState } from "react";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import CssBaseline from "@mui/material/CssBaseline";
import Box from "@mui/material/Box";
import { ThemeProvider } from "@mui/material/styles";
import { pipelineReducer, initialState } from "./store/pipelineStore";
import { usePipelineSocket } from "./hooks/usePipelineSocket";
import { useStatePoll } from "./hooks/useStatePoll";
import NavBar from "./components/NavBar";
import DashboardPage from "./pages/DashboardPage";
import LogsPage from "./pages/LogsPage";
import KillSwitchPage from "./pages/KillSwitchPage";
import BacktestPage from "./pages/BacktestPage";
import AnalyticsPage from "./pages/AnalyticsPage";
import { buildTheme, getInitialMode, THEME_STORAGE_KEY, type ThemeMode } from "./theme/theme";
import { ThemeModeContext } from "./theme/ThemeModeContext";

const TOOLBAR_HEIGHT = 48; // dense AppBar

function AppInner() {
  const [state, dispatch] = useReducer(pipelineReducer, initialState);
  usePipelineSocket(dispatch);
  useStatePoll(dispatch);

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
          <Route path="/analytics" element={<AnalyticsPage state={state} />} />
          <Route path="/logs" element={<LogsPage logs={state.logs} onClear={clearLogs} />} />
          <Route path="/killswitch" element={<KillSwitchPage />} />
          <Route path="/backtest" element={<BacktestPage />} />
        </Routes>
      </Box>
    </Box>
  );
}

export default function App() {
  const [mode, setMode] = useState<ThemeMode>(getInitialMode);

  useEffect(() => {
    window.localStorage.setItem(THEME_STORAGE_KEY, mode);
  }, [mode]);

  const themeContext = useMemo(
    () => ({
      mode,
      toggleMode: () => setMode((m) => (m === "dark" ? "light" : "dark")),
    }),
    [mode],
  );

  const theme = useMemo(() => buildTheme(mode), [mode]);

  return (
    <ThemeModeContext.Provider value={themeContext}>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <BrowserRouter>
          <AppInner />
        </BrowserRouter>
      </ThemeProvider>
    </ThemeModeContext.Provider>
  );
}
