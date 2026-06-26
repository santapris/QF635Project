import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Chip from "@mui/material/Chip";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { RoutingRow } from "../store/pipelineStore";
import { formatTs } from "../utils/formatTs";
import { formatNum } from "../utils/formatNum";

const columns: GridColDef<RoutingRow>[] = [
  { field: "ts", headerName: "Time", width: 110, renderCell: ({ value }) => formatTs(value as number) },
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
  { field: "quantity", headerName: "Qty", width: 90, align: "right", headerAlign: "right", valueFormatter: (value) => formatNum(value as string) },
  {
    field: "intent",
    headerName: "Intent",
    width: 120,
    renderCell: ({ value }) =>
      value ? <Chip label={value} size="small" variant="outlined" /> : "—",
  },
  { field: "algo", headerName: "Algo", width: 110 },
  { field: "reason", headerName: "Reason", flex: 1, minWidth: 180 },
];

interface Props {
  routings: RoutingRow[];
}

export default function RoutingPanel({ routings }: Props) {
  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Routing
      </Typography>
      <DataGrid
        rows={routings}
        columns={columns}
        density="compact"
        disableRowSelectionOnClick
        hideFooterSelectedRowCount
        pageSizeOptions={[25, 50]}
        initialState={{ pagination: { paginationModel: { pageSize: 25 } } }}
        localeText={{ noRowsLabel: "No routing decisions yet" }}
        sx={{ flex: 1, border: "none" }}
      />
    </Paper>
  );
}
