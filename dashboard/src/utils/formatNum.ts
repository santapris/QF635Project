/**
 * Format a numeric value for display, capping the decimal count.
 *
 * Backend numbers arrive as strings carrying full venue precision (e.g.
 * "0.00012345678"). Rendering that raw makes columns jitter and overflow, so
 * every numeric cell in the dashboard is funnelled through here.
 *
 * - Caps at `decimals` (default 4) decimal places.
 * - Trims trailing zeros so whole numbers stay clean ("12" not "12.0000").
 * - Passes through anything that isn't a finite number unchanged (e.g. "—",
 *   null), so it is safe to drop onto any column's value formatter.
 */
export function formatNum(
  val: string | number | null | undefined,
  decimals = 4
): string {
  if (val == null || val === "") return "—";
  const n = typeof val === "number" ? val : parseFloat(val);
  if (!isFinite(n)) return String(val);
  // toFixed then strip trailing zeros / dangling decimal point.
  return n.toFixed(decimals).replace(/\.?0+$/, "");
}
