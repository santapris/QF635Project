import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Chip from "@mui/material/Chip";
import Box from "@mui/material/Box";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { SignalRow, OrderLeg } from "../store/pipelineStore";
import { formatTs } from "../utils/formatTs";

const columns: GridColDef<SignalRow>[] = [
  { field: "ts", headerName: "Time", width: 110, renderCell: ({ value }) => formatTs(value as number) },
  { field: "strategy_id", headerName: "Strategy", width: 120 },
  { field: "instrument", headerName: "Instrument", width: 120 },
  {
    field: "legs",
    headerName: "Legs",
    flex: 1,
    minWidth: 200,
    sortable: false,
    renderCell: ({ value }) => (
      <Box sx={{ display: "flex", gap: 0.5, flexWrap: "wrap", alignItems: "center", width: "100%", height: "100%" }}>
        {(value as OrderLeg[]).map((leg) => (
          <Chip
            key={leg.leg_id}
            size="small"
            variant="outlined"
            label={`${leg.side} ${leg.quantity} ${leg.order_type}`}
            color={leg.side === "BUY" ? "success" : "error"}
          />
        ))}
      </Box>
    ),
  },
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
