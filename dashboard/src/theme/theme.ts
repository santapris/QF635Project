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
            success:  { main: "#3fb950", light: "#56d364", dark: "#2ea043" },
            error:    { main: "#f85149", light: "#ff7b72", dark: "#da3633" },
            warning:  { main: "#d29922", light: "#e3b341", dark: "#bb8009" },
            info:     { main: "#58a6ff", light: "#79c0ff", dark: "#388bfd" },
          }
        : {
            background: { default: "#f5f6f8", paper: "#ffffff" },
            success:  { main: "#1a7f37", light: "#2da44e", dark: "#116329" },
            error:    { main: "#cf222e", light: "#ff7b72", dark: "#a40e26" },
            warning:  { main: "#9a6700", light: "#bf8700", dark: "#6e4a00" },
            info:     { main: "#0969da", light: "#54aeff", dark: "#0550ae" },
          }),
    },
    typography: { fontSize: 13 },
  });
}

export type SemanticStatus = "pass" | "fail" | "warning" | "info" | "debug";

export const SEMANTIC_COLOR: Record<SemanticStatus, "success" | "error" | "warning" | "info" | "default"> = {
  pass:    "success",
  fail:    "error",
  warning: "warning",
  info:    "info",
  debug:   "default",
};
