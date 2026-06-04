import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Chip from "@mui/material/Chip";
import { DataGrid, type GridColDef, type GridRenderCellParams } from "@mui/x-data-grid";
import type { AnalyticsPoint } from "../../store/pipelineStore";
import { formatTs } from "../../utils/formatTs";

interface Props {
  history: AnalyticsPoint[];
}

function fmt(v: number | null | undefined, decimals = 4): string {
  if (v == null) return "—";
  return v.toFixed(decimals);
}

function vpinColor(v: number | null): "success" | "warning" | "error" | "default" {
  if (v == null) return "default";
  if (v < 0.5) return "success";
  if (v < 0.7) return "warning";
  return "error";
}

const columns: GridColDef[] = [
  {
    field: "ts",
    headerName: "Time",
    width: 90,
    renderCell: (p: GridRenderCellParams) => formatTs(p.value as number),
  },
  { field: "microprice", headerName: "Microprice", width: 100, renderCell: (p) => fmt(p.value as number, 2) },
  { field: "sigma", headerName: "σ", width: 85, renderCell: (p) => fmt(p.value as number, 6) },
  { field: "obi", headerName: "OBI L1", width: 75, renderCell: (p) => fmt(p.value as number, 3) },
  { field: "obi_l2", headerName: "OBI L2", width: 75, renderCell: (p) => fmt(p.value as number, 3) },
  { field: "ofi", headerName: "OFI", width: 75, renderCell: (p) => fmt(p.value as number, 3) },
  {
    field: "vpin",
    headerName: "VPIN",
    width: 100,
    renderCell: (p: GridRenderCellParams) => {
      const v = p.value as number | null;
      return <Chip label={fmt(v, 3)} color={vpinColor(v)} size="small" sx={{ fontFamily: "monospace" }} />;
    },
  },
  { field: "inventory", headerName: "Inventory", width: 95, renderCell: (p) => fmt(p.value as number, 6) },
  { field: "reservation", headerName: "Reservation", width: 105, renderCell: (p) => fmt(p.value as number, 4) },
  { field: "half_spread", headerName: "½ Spread", width: 85, renderCell: (p) => fmt(p.value as number, 5) },
  { field: "bid_quote", headerName: "Bid Quote", width: 95, renderCell: (p) => fmt(p.value as number, 2) },
  { field: "ask_quote", headerName: "Ask Quote", width: 95, renderCell: (p) => fmt(p.value as number, 2) },
  {
    field: "vpin_widened",
    headerName: "VPIN Wide",
    width: 90,
    renderCell: (p: GridRenderCellParams) =>
      p.value ? <Chip label="YES" color="error" size="small" /> : null,
  },
];

export default function AnalyticsTable({ history }: Props) {
  // DataGrid wants oldest-first; history is already chronological (oldest first).
  // Reverse for display so newest is at top.
  const rows = [...history].reverse().map((p, i) => ({ id: i, ...p }));

  return (
    <Paper sx={{ p: 2, height: "100%" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Analytics Log
      </Typography>
      <DataGrid
        rows={rows}
        columns={columns}
        density="compact"
        disableRowSelectionOnClick
        hideFooter={rows.length <= 100}
        sx={{ border: "none", fontSize: 11 }}
      />
    </Paper>
  );
}
