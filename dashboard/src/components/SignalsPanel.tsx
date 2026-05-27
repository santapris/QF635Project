import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Chip from "@mui/material/Chip";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { SignalRow } from "../store/pipelineStore";
import { formatTs } from "../utils/formatTs";

const columns: GridColDef<SignalRow>[] = [
  { field: "ts", headerName: "Time", width: 110, renderCell: ({ value }) => formatTs(value as number) },
  { field: "strategy_id", headerName: "Strategy", width: 120 },
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
  { field: "target_quantity", headerName: "Qty", width: 90, align: "right", headerAlign: "right" },
  { field: "order_type", headerName: "Type", width: 90 },
  { field: "rationale", headerName: "Rationale", flex: 1, minWidth: 150 },
];

interface Props {
  signals: SignalRow[];
}

export default function SignalsPanel({ signals }: Props) {
  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Signals
      </Typography>
      <DataGrid
        rows={signals}
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
