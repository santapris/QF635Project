import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Chip from "@mui/material/Chip";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { OrderRow } from "../store/pipelineStore";
import { formatTs } from "../utils/formatTs";

const STATUS_COLOR: Record<string, "default" | "info" | "success" | "error" | "warning"> = {
  new: "info",
  partially_filled: "warning",
  filled: "success",
  canceled: "default",
  rejected: "error",
};

const columns: GridColDef<OrderRow>[] = [
  { field: "ts", headerName: "Time", width: 110, renderCell: ({ value }) => formatTs(value as number) },
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
  { field: "price", headerName: "Price", width: 100, align: "right", headerAlign: "right" },
  {
    field: "status",
    headerName: "Status",
    width: 130,
    renderCell: ({ value }) => (
      <Chip
        label={value}
        size="small"
        color={STATUS_COLOR[value as string] ?? "default"}
      />
    ),
  },
];

interface Props {
  orders: OrderRow[];
}

export default function OrdersPanel({ orders }: Props) {
  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Orders
      </Typography>
      <DataGrid
        rows={orders}
        columns={columns}
        density="compact"
        disableRowSelectionOnClick
        hideFooterSelectedRowCount
        pageSizeOptions={[25, 50]}
        initialState={{ pagination: { paginationModel: { pageSize: 25 } } }}
        sx={{ flex: 1, border: "none" }}
      />
    </Paper>
  );
}
