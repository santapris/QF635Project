import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import MStripe from "./MStripe";

interface SectionLabelProps {
  children: React.ReactNode;
  /** Optional right-aligned content (counts, controls). */
  action?: React.ReactNode;
}

/**
 * Band header used between editorial sections: a short M-stripe tick followed by
 * an UPPERCASE letterspaced label. The stripe marks significance, sparingly.
 */
export default function SectionLabel({ children, action }: SectionLabelProps) {
  return (
    <Box
      sx={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        mb: 1.25,
      }}
    >
      <Box sx={{ display: "flex", alignItems: "center", gap: 1.25, minWidth: 0 }}>
        <MStripe width={24} height={14} />
        <Typography
          component="h2"
          sx={{
            fontWeight: 700,
            fontSize: 13,
            letterSpacing: "1.5px",
            textTransform: "uppercase",
            color: "text.primary",
            whiteSpace: "nowrap",
            overflow: "hidden",
            textOverflow: "ellipsis",
          }}
        >
          {children}
        </Typography>
      </Box>
      {action && <Box sx={{ flexShrink: 0 }}>{action}</Box>}
    </Box>
  );
}
