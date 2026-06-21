import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import MStripe from "./MStripe";

interface SpecCellProps {
  label: string;
  value: React.ReactNode;
  /** Small unit or context shown after the value (e.g. "USDT"). */
  unit?: string;
  /** Optional status hint shown top-right (e.g. live status pill). */
  badge?: React.ReactNode;
  /** Value color override — defaults to ink. Pass success/error main for PnL. */
  valueColor?: string;
  /** Show the M-stripe accent at the bottom (used to mark the hero metric). */
  accent?: boolean;
}

/**
 * BMW M `spec-cell`: a value in heavy display type over an UPPERCASE label, on a
 * barely-off-black surface. Used for the KPI hero strip at the top of the dashboard.
 */
export default function SpecCell({
  label,
  value,
  unit,
  badge,
  valueColor,
  accent,
}: SpecCellProps) {
  return (
    <Box
      sx={{
        position: "relative",
        height: "100%",
        bgcolor: (t) => (t.palette.mode === "dark" ? "#0d0d0d" : "#ffffff"),
        border: "1px solid",
        borderColor: "divider",
        px: 2.5,
        py: 2,
        display: "flex",
        flexDirection: "column",
        justifyContent: "space-between",
        minHeight: 96,
        overflow: "hidden",
      }}
    >
      <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <Typography
          sx={{
            fontSize: 11,
            fontWeight: 700,
            letterSpacing: "1.5px",
            textTransform: "uppercase",
            color: "text.secondary",
          }}
        >
          {label}
        </Typography>
        {badge}
      </Box>

      <Box sx={{ display: "flex", alignItems: "baseline", gap: 0.75, mt: 1 }}>
        <Typography
          sx={{
            fontWeight: 800,
            fontSize: { xs: 26, md: 30 },
            lineHeight: 1,
            letterSpacing: "-0.5px",
            color: valueColor ?? "text.primary",
            fontVariantNumeric: "tabular-nums",
            whiteSpace: "nowrap",
            overflow: "hidden",
            textOverflow: "ellipsis",
          }}
        >
          {value}
        </Typography>
        {unit && (
          <Typography
            sx={{
              fontSize: 12,
              fontWeight: 400,
              letterSpacing: "0.5px",
              color: "text.secondary",
            }}
          >
            {unit}
          </Typography>
        )}
      </Box>

      {accent && (
        <MStripe height={3} width={48} sx={{ position: "absolute", left: 0, bottom: 0 }} />
      )}
    </Box>
  );
}
