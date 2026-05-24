import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { PositionRow } from "../store/pipelineStore";

function pnlColor(val: string): string {
  const n = parseFloat(val);
  if (n > 0) return "success.main";
  if (n < 0) return "error.main";
  return "text.primary";
}

const columns: GridColDef<PositionRow>[] = [
  { field: "instrument", headerName: "Instrument", flex: 1, minWidth: 120 },
  { field: "quantity", headerName: "Qty", flex: 1, minWidth: 90, align: "right", headerAlign: "right" },
  {
    field: "average_entry_price",
    headerName: "Avg Entry",
    flex: 1,
    minWidth: 110,
    align: "right",
    headerAlign: "right",
  },
  { field: "mark_price", headerName: "Mark", flex: 1, minWidth: 110, align: "right", headerAlign: "right" },
  {
    field: "unrealized_pnl",
    headerName: "Unrealized PnL",
    flex: 1,
    minWidth: 130,
    align: "right",
    headerAlign: "right",
    renderCell: ({ value }) => (
      <Box sx={{ color: pnlColor(value as string), fontWeight: 600 }}>{value}</Box>
    ),
  },
  {
    field: "realized_pnl",
    headerName: "Realized PnL",
    flex: 1,
    minWidth: 130,
    align: "right",
    headerAlign: "right",
    renderCell: ({ value }) => (
      <Box sx={{ color: pnlColor(value as string), fontWeight: 600 }}>{value}</Box>
    ),
  },
];

interface Props {
  positions: Record<string, PositionRow>;
}

export default function PositionPanel({ positions }: Props) {
  const rows = Object.values(positions);

  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Positions
      </Typography>
      <Box sx={{ flex: 1, minHeight: 0 }}>
        <DataGrid
          rows={rows}
          columns={columns}
          density="compact"
          disableRowSelectionOnClick
          hideFooter
          getRowId={(row) => `${row.strategy_id}-${row.instrument}`}
          localeText={{ noRowsLabel: "No open positions" }}
          sx={{ border: "none", height: "100%" }}
        />
      </Box>
    </Paper>
  );
}
