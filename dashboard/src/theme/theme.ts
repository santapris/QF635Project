import { createTheme, type Theme } from "@mui/material/styles";

export type ThemeMode = "light" | "dark";

export const THEME_STORAGE_KEY = "qf635:theme-mode";

export function getInitialMode(): ThemeMode {
  if (typeof window === "undefined") return "dark";
  const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
  if (stored === "light" || stored === "dark") return stored;
  const prefersLight = window.matchMedia?.("(prefers-color-scheme: light)").matches;
  return prefersLight ? "light" : "dark";
}

export function buildTheme(mode: ThemeMode): Theme {
  return createTheme({
    palette: {
      mode,
      ...(mode === "dark"
        ? {
            background: { default: "#0e1116", paper: "#161b22" },
          }
        : {
            background: { default: "#f5f6f8", paper: "#ffffff" },
          }),
    },
    typography: { fontSize: 13 },
  });
}
