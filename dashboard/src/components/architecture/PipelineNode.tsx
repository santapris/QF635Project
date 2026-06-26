/**
 * A single component box in the architecture graph. Renders in the BMW idiom
 * (carbon surface, hairline border, UPPERCASE machined label) and reacts to
 * live pipeline activity:
 *
 *   - a node that received an event within FRESH_MS glows on its accent colour
 *     and runs a brief pulse animation keyed on the activity counter;
 *   - older activity fades the node to "idle", then "stale";
 *   - when the kill switch is engaged the whole node washes M-red.
 */
import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import { useTheme } from "@mui/material/styles";
import { BMW } from "../../theme/theme";
import { FRESH_MS, STALE_MS } from "../../hooks/useFlowPulse";

export interface PipelineNodeData {
  label: string;
  sublabel: string;
  /** Which side handles to render. */
  inputs: Position[];
  outputs: Position[];
  accent: string;
  activity: number;
  lastSeen: number | null;
  /** When true the whole graph is in a halted state. */
  halted: boolean;
  /** ms-epoch tick that drives freshness re-eval (parent re-renders on it). */
  now: number;
  [key: string]: unknown;
}

function freshness(lastSeen: number | null, now: number): "live" | "idle" | "stale" {
  if (lastSeen == null) return "stale";
  const age = now - lastSeen;
  if (age <= FRESH_MS) return "live";
  if (age <= STALE_MS) return "idle";
  return "stale";
}

function PipelineNodeImpl({ data }: NodeProps) {
  const d = data as PipelineNodeData;
  const theme = useTheme();
  const isDark = theme.palette.mode === "dark";
  const state = freshness(d.lastSeen, d.now);

  const accent = d.halted ? BMW.mRed : d.accent;
  const live = state === "live" && !d.halted;
  const borderColor =
    d.halted ? BMW.mRed
    : state === "live" ? accent
    : state === "idle" ? theme.palette.divider
    : theme.palette.divider;

  return (
    <Box
      key={d.activity /* remount the pulse animation on each new event */}
      sx={{
        position: "relative",
        minWidth: 132,
        px: 1.75,
        py: 1.25,
        bgcolor: isDark ? "#0f0f0f" : "#ffffff",
        border: `1.5px solid ${borderColor}`,
        transition: "border-color 0.4s ease, box-shadow 0.4s ease",
        boxShadow: live ? `0 0 0 1px ${accent}, 0 0 16px -2px ${accent}` : "none",
        "@keyframes nodePulse": {
          "0%": { boxShadow: `0 0 0 0 ${accent}` },
          "60%": { boxShadow: `0 0 18px 3px ${accent}` },
          "100%": { boxShadow: "0 0 0 0 transparent" },
        },
        animation: live ? "nodePulse 0.9s ease-out" : "none",
      }}
    >
      {/* Top accent tick — the BMW signature, recoloured by state. */}
      <Box sx={{ position: "absolute", top: 0, left: 0, right: 0, height: 3, bgcolor: accent, opacity: state === "stale" ? 0.25 : 1 }} />

      {d.inputs.map((pos, i) => (
        <Handle key={`in-${pos}-${i}`} type="target" position={pos} id={`in-${pos}`} style={{ opacity: 0 }} />
      ))}
      {d.outputs.map((pos, i) => (
        <Handle key={`out-${pos}-${i}`} type="source" position={pos} id={`out-${pos}`} style={{ opacity: 0 }} />
      ))}

      <Typography
        sx={{
          fontWeight: 700,
          fontSize: 13,
          letterSpacing: "1px",
          textTransform: "uppercase",
          color: "text.primary",
          lineHeight: 1.15,
        }}
      >
        {d.label}
      </Typography>
      <Typography
        sx={{
          fontWeight: 400,
          fontSize: 10,
          letterSpacing: "0.5px",
          color: "text.secondary",
          mt: 0.25,
        }}
      >
        {d.sublabel}
      </Typography>

      {/* Live dot — colour tracks freshness. */}
      <Box
        sx={{
          position: "absolute",
          top: 7,
          right: 7,
          width: 7,
          height: 7,
          borderRadius: "50%",
          bgcolor: state === "live" ? accent : state === "idle" ? theme.palette.warning.main : theme.palette.divider,
          transition: "background-color 0.4s ease",
        }}
      />
    </Box>
  );
}

export default memo(PipelineNodeImpl);
