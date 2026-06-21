import Box from "@mui/material/Box";
import { BMW } from "../../theme/theme";

interface MStripeProps {
  /** Stripe thickness in px. Spec calls for 4px; stays 4px across breakpoints. */
  height?: number;
  /** Optional fixed width; defaults to filling the container. */
  width?: number | string;
  sx?: object;
}

/**
 * The BMW M tricolor stripe (blue-light → blue-dark → red).
 * Brand-identity signature only — never used as a button or action surface.
 */
export default function MStripe({ height = 4, width = "100%", sx }: MStripeProps) {
  return (
    <Box
      aria-hidden
      sx={{
        height,
        width,
        flexShrink: 0,
        background: BMW.stripe,
        ...sx,
      }}
    />
  );
}
