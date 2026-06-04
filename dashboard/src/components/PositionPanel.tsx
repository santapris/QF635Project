import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { PositionRow, VenueNetRow } from "../store/pipelineStore";

function pnlColor(val: string): string {
  const n = parseFloat(val);
  if (n > 0) return "success.main";
  if (n < 0) return "error.main";
  return "text.primary";
}

function formatPnl(val: string): string {
  const n = parseFloat(val);
  if (isNaN(n)) return val;
  return n.toFixed(4);
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
      <Box sx={{ color: pnlColor(value as string), fontWeight: 600 }}>{formatPnl(value as string)}</Box>
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
      <Box sx={{ color: pnlColor(value as string), fontWeight: 600 }}>{formatPnl(value as string)}</Box>
    ),
  },
];

const venueColumns: GridColDef<VenueNetRow>[] = [
  { field: "instrument", headerName: "Instrument", flex: 1, minWidth: 120 },
  {
    field: "net_quantity",
    headerName: "Net Qty",
    flex: 1,
    minWidth: 90,
    align: "right",
    headerAlign: "right",
    renderCell: ({ value }) => {
      const n = parseFloat(value as string);
      return (
        <Box sx={{ color: n > 0 ? "success.main" : n < 0 ? "error.main" : "text.primary", fontWeight: 600 }}>
          {value}
        </Box>
      );
    },
  },
  { field: "entry_price", headerName: "Entry", flex: 1, minWidth: 110, align: "right", headerAlign: "right" },
  { field: "mark_price", headerName: "Mark", flex: 1, minWidth: 110, align: "right", headerAlign: "right" },
  {
    field: "unrealized_pnl",
    headerName: "Unrealized PnL",
    flex: 1,
    minWidth: 130,
    align: "right",
    headerAlign: "right",
    renderCell: ({ value }) => (
      <Box sx={{ color: pnlColor(value as string), fontWeight: 600 }}>{formatPnl(value as string)}</Box>
    ),
  },
];

interface Props {
  positions: Record<string, PositionRow>;
  venueNet: VenueNetRow[];
}

export default function PositionPanel({ positions, venueNet }: Props) {
  const rows = Object.values(positions);

  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      {/* Exchange net — ground truth, comparable to the Binance UI. */}
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Net Position (exchange)
      </Typography>
      <Box sx={{ mb: 1 }}>
        <DataGrid
          rows={venueNet}
          columns={venueColumns}
          density="compact"
          disableRowSelectionOnClick
          hideFooter
          autoHeight
          getRowId={(row) => row.id}
          localeText={{ noRowsLabel: "No exchange position" }}
          sx={{ border: "none" }}
        />
      </Box>
      {/* Per-strategy breakdown — our fill-derived attribution. */}
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        By Strategy
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
