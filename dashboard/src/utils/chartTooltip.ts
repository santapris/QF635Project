import type { Theme } from "@mui/material/styles";

/**
 * Themed style props for the default Recharts <Tooltip>. Without these,
 * Recharts renders a hard-coded white tooltip that is unreadable in dark mode.
 * Mirrors the look of the custom tooltip in PnlChart.
 *
 * Usage: <Tooltip {...chartTooltipStyle(theme)} ... />
 */
export function chartTooltipStyle(theme: Theme) {
  return {
    contentStyle: {
      backgroundColor: theme.palette.mode === "dark" ? "#161616" : "#ffffff",
      border: `1px solid ${theme.palette.divider}`,
      borderRadius: 0,
      boxShadow: theme.shadows[4],
      fontSize: 12,
    },
    labelStyle: {
      color: theme.palette.text.secondary,
      fontSize: 11,
      marginBottom: 4,
    },
    itemStyle: {
      fontSize: 12,
    },
    cursor: { stroke: theme.palette.text.secondary, strokeDasharray: "3 3" },
  };
}
