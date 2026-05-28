import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Chip from "@mui/material/Chip";
import Box from "@mui/material/Box";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { OpenOrderRow } from "../store/pipelineStore";
import { formatTs } from "../utils/formatTs";

const columns: GridColDef<OpenOrderRow>[] = [
  { field: "ts", headerName: "Created", width: 110, renderCell: ({ value }) => formatTs(value as number) },
  { field: "order_id", headerName: "Order ID", width: 130 },
  { field: "strategy_id", headerName: "Strategy", width: 110 },
  { field: "instrument", headerName: "Instrument", width: 120 },
  {
    field: "side",
    headerName: "Side",
    width: 80,
    renderCell: ({ value }) => (
      <Chip
        label={value}
        size="small"
        color={value === "BUY" ? "success" : "error"}
        sx={{ fontWeight: 700 }}
      />
    ),
  },
  { field: "order_type", headerName: "Type", width: 90 },
  { field: "quantity", headerName: "Qty", width: 80, align: "right", headerAlign: "right" },
  { field: "leaves_quantity", headerName: "Leaves", width: 80, align: "right", headerAlign: "right" },
  { field: "price", headerName: "Price", width: 100, align: "right", headerAlign: "right" },
  {
    field: "status",
    headerName: "Status",
    width: 110,
    renderCell: ({ value }) => (
      <Chip
        label={value}
        size="small"
        color={value === "PARTIALLY_FILLED" ? "warning" : "info"}
      />
    ),
  },
];

interface Props {
  openOrders: OpenOrderRow[];
}

// One row per currently-resting order, sourced from the OMS's authoritative
// open-orders snapshot (polled). Because each snapshot is the complete current
// truth, a missed WS event can never leave a stale row here.
export default function OpenOrdersPanel({ openOrders }: Props) {
  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Open Orders
      </Typography>
      <Box sx={{ flex: 1, minHeight: 0 }}>
        <DataGrid
          rows={openOrders}
          columns={columns}
          density="compact"
          disableRowSelectionOnClick
          hideFooter
          getRowId={(row) => row.id}
          localeText={{ noRowsLabel: "No resting orders" }}
          sx={{ border: "none", height: "100%" }}
        />
      </Box>
    </Paper>
  );
}
