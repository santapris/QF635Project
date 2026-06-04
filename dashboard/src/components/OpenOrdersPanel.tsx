import { useState, useMemo } from "react";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Chip from "@mui/material/Chip";
import Box from "@mui/material/Box";
import TextField from "@mui/material/TextField";
import MenuItem from "@mui/material/MenuItem";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { OpenOrderRow } from "../store/pipelineStore";
import { formatTs } from "../utils/formatTs";

const STATUS_VALUES = ["ALL", "ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"] as const;

const STATUS_COLOR: Record<string, "default" | "info" | "success" | "error" | "warning"> = {
  ACKNOWLEDGED: "info",
  PARTIALLY_FILLED: "warning",
  FILLED: "success",
  CANCELLED: "default",
  REJECTED: "error",
};

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
      <Typography
        variant="body2"
        color={value === "BUY" ? "success" : "error"}
        sx={{ width: "100%", height: "100%", display: "flex", alignItems: "center", fontWeight: 700 }}
      >
        {value}
      </Typography>
    ),
  },
  { field: "order_type", headerName: "Type", width: 90 },
  { field: "quantity", headerName: "Qty", width: 80, align: "right", headerAlign: "right" },
  { field: "leaves_quantity", headerName: "Leaves", width: 80, align: "right", headerAlign: "right" },
  { field: "price", headerName: "Price", width: 100, align: "right", headerAlign: "right" },
  {
    field: "status",
    headerName: "Status",
    width: 130,
    sortable: false,
    renderCell: ({ value }) => (
      <Chip
        label={value}
        size="small"
        color={STATUS_COLOR[value as string] ?? "info"}
      />
    ),
  },
];

interface Props {
  openOrders: OpenOrderRow[];
}

export default function OpenOrdersPanel({ openOrders }: Props) {
  const [statusFilter, setStatusFilter] = useState("ALL");

  const filtered = useMemo(() => {
    if (statusFilter === "ALL") return openOrders;
    return openOrders.filter((r) => r.status === statusFilter);
  }, [openOrders, statusFilter]);

  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1 }}>
        <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
          Open Orders
        </Typography>
        <TextField
          select
          label="Status"
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
          size="small"
          sx={{ minWidth: 150 }}
        >
          {STATUS_VALUES.map((s) => (
            <MenuItem key={s} value={s}>
              {s}
            </MenuItem>
          ))}
        </TextField>
      </Box>
      <Box sx={{ flex: 1, minHeight: 0 }}>
        <DataGrid
          rows={filtered}
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
