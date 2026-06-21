import { createTheme, type Theme } from "@mui/material/styles";
// Registers the `MuiDataGrid` key on the MUI theme `components` type.
import type {} from "@mui/x-data-grid/themeAugmentation";

export type ThemeMode = "light" | "dark";

export const THEME_STORAGE_KEY = "qf635:theme-mode";

/**
 * BMW M design tokens (from DESIGN-BMW.md).
 * The dark mode is the canonical BMW M "motorsport" surface — a true-black canvas
 * with white BMW-Type-style headlines. Light mode is a usable inversion for daytime
 * desks but keeps the same M tricolor signature and sharp-cornered geometry.
 */
export const BMW = {
  // M tricolor — brand-identity accent only, never an action fill.
  mBlueLight: "#0066b1",
  mBlueDark: "#1c69d4",
  mRed: "#e22718",
  bmwBlue: "#1c69d4",
  electricBlue: "#0653b6",
  // The signature 3-stop stripe.
  stripe: "linear-gradient(90deg, #0066b1 0%, #0066b1 33.33%, #1c69d4 33.33%, #1c69d4 66.66%, #e22718 66.66%, #e22718 100%)",
} as const;

const FONT_STACK =
  '"Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';

export function getInitialMode(): ThemeMode {
  if (typeof window === "undefined") return "dark";
  const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
  if (stored === "light" || stored === "dark") return stored;
  // Default to the canonical BMW M dark surface.
  return "dark";
}

export function buildTheme(mode: ThemeMode): Theme {
  const isDark = mode === "dark";

  // Surface ladder — true black canvas in dark, near-white in light.
  const canvas = isDark ? "#000000" : "#f4f5f6";
  const surfaceCard = isDark ? "#0f0f0f" : "#ffffff";
  const surfaceElevated = isDark ? "#1a1a1a" : "#ffffff";
  const hairline = isDark ? "#262626" : "#dcdee2";
  const hairlineStrong = isDark ? "#3c3c3c" : "#c6c9cf";

  const ink = isDark ? "#ffffff" : "#0a0a0a";
  const body = isDark ? "#bbbbbb" : "#3f4348";
  const muted = isDark ? "#7e7e7e" : "#8a8f96";

  const theme = createTheme({
    palette: {
      mode,
      primary: { main: ink, contrastText: canvas },
      background: { default: canvas, paper: surfaceCard },
      text: { primary: ink, secondary: body, disabled: muted },
      divider: hairline,
      success: { main: isDark ? "#0fa336" : "#1a7f37", light: "#56d364", dark: "#0b7a28" },
      error: { main: BMW.mRed, light: "#ff6a5d", dark: "#b51a0f" },
      warning: { main: "#f4b400", light: "#ffce4d", dark: "#bb8009" },
      info: { main: BMW.bmwBlue, light: "#5a93e6", dark: "#0653b6" },
    },
    shape: { borderRadius: 0 },
    typography: {
      fontFamily: FONT_STACK,
      fontSize: 13,
      // Heavy display vs. light body is the BMW editorial signature.
      h1: { fontWeight: 800, letterSpacing: "-0.5px", lineHeight: 1 },
      h2: { fontWeight: 800, letterSpacing: "-0.5px", lineHeight: 1.05 },
      h3: { fontWeight: 800, letterSpacing: "-0.5px", lineHeight: 1.1 },
      h4: { fontWeight: 700, letterSpacing: "-0.25px", lineHeight: 1.15 },
      h5: { fontWeight: 700, lineHeight: 1.2 },
      h6: { fontWeight: 700, lineHeight: 1.3 },
      subtitle1: { fontWeight: 700 },
      subtitle2: { fontWeight: 700, letterSpacing: "0.5px" },
      body1: { fontWeight: 300, lineHeight: 1.5 },
      body2: { fontWeight: 300, lineHeight: 1.5 },
      button: { fontWeight: 700, letterSpacing: "1.5px", textTransform: "uppercase" },
      caption: { fontWeight: 400, letterSpacing: "0.5px" },
      overline: { fontWeight: 700, letterSpacing: "1.5px", textTransform: "uppercase" },
    },
    components: {
      MuiCssBaseline: {
        styleOverrides: {
          body: { backgroundColor: canvas },
          // Slim, dark, motorsport-precise scrollbars.
          "*::-webkit-scrollbar": { width: 8, height: 8 },
          "*::-webkit-scrollbar-thumb": {
            backgroundColor: hairlineStrong,
            borderRadius: 0,
          },
          "*::-webkit-scrollbar-track": { backgroundColor: "transparent" },
        },
      },
      MuiPaper: {
        defaultProps: { elevation: 0 },
        styleOverrides: {
          root: {
            backgroundColor: surfaceCard,
            backgroundImage: "none",
            border: `1px solid ${hairline}`,
            borderRadius: 0,
          },
        },
      },
      MuiAppBar: {
        defaultProps: { elevation: 0, color: "default" },
        styleOverrides: {
          root: {
            backgroundColor: canvas,
            backgroundImage: "none",
            borderRadius: 0,
            borderBottom: `1px solid ${hairline}`,
          },
        },
      },
      MuiButton: {
        defaultProps: { disableElevation: true },
        styleOverrides: {
          root: { borderRadius: 0 },
          outlined: { borderColor: hairlineStrong },
        },
      },
      MuiToggleButton: {
        styleOverrides: {
          root: {
            borderRadius: 0,
            border: `1px solid ${hairline}`,
            textTransform: "uppercase",
            letterSpacing: "1px",
            fontWeight: 700,
            fontSize: 12,
            color: body,
            "&.Mui-selected": {
              backgroundColor: surfaceElevated,
              color: ink,
              "&:hover": { backgroundColor: surfaceElevated },
            },
          },
        },
      },
      MuiChip: {
        styleOverrides: {
          root: { borderRadius: 0, fontWeight: 700, letterSpacing: "0.5px" },
        },
      },
      MuiTabs: {
        styleOverrides: {
          root: { minHeight: 40, borderBottom: `1px solid ${hairline}` },
          indicator: { height: 2, backgroundColor: ink },
        },
      },
      MuiTab: {
        styleOverrides: {
          root: {
            minHeight: 40,
            textTransform: "uppercase",
            letterSpacing: "1.5px",
            fontWeight: 700,
            fontSize: 12,
            color: body,
            "&.Mui-selected": { color: ink },
          },
        },
      },
      MuiOutlinedInput: {
        styleOverrides: {
          root: { borderRadius: 0, backgroundColor: surfaceElevated },
        },
      },
      MuiSelect: { styleOverrides: { select: { borderRadius: 0 } } },
      MuiTooltip: {
        styleOverrides: {
          tooltip: {
            borderRadius: 0,
            backgroundColor: surfaceElevated,
            border: `1px solid ${hairlineStrong}`,
            fontWeight: 400,
            letterSpacing: "0.3px",
          },
        },
      },
      MuiDataGrid: {
        styleOverrides: {
          root: {
            border: "none",
            fontWeight: 300,
            "--DataGrid-rowBorderColor": hairline,
          },
          columnHeaders: { borderBottom: `1px solid ${hairlineStrong}` },
          columnHeaderTitle: {
            textTransform: "uppercase",
            letterSpacing: "1px",
            fontWeight: 700,
            fontSize: 11,
            color: muted,
          },
          cell: { borderColor: hairline },
          footerContainer: { borderTop: `1px solid ${hairline}` },
        },
      },
    },
  });

  return theme;
}

export type SemanticStatus = "pass" | "fail" | "warning" | "info" | "debug";

export const SEMANTIC_COLOR: Record<SemanticStatus, "success" | "error" | "warning" | "info" | "default"> = {
  pass:    "success",
  fail:    "error",
  warning: "warning",
  info:    "info",
  debug:   "default",
};
