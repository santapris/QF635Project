import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Chip from "@mui/material/Chip";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { RiskRow } from "../store/pipelineStore";
import { formatTs } from "../utils/formatTs";

const columns: GridColDef<RiskRow>[] = [
  { field: "ts", headerName: "Time", width: 110, renderCell: ({ value }) => formatTs(value as number) },
  { field: "strategy_id", headerName: "Strategy", width: 130 },
  {
    field: "approved",
    headerName: "Decision",
    width: 120,
    sortable: false,
    renderCell: ({ value }) => (
      <Chip
        label={value ? "APPROVED" : "REJECTED"}
        size="small"
        color={value ? "success" : "error"}
      />
    ),
  },
  { field: "rule_name", headerName: "Rule", width: 160, renderCell: ({ value }) => value ?? "—" },
  {
    field: "approved_quantity",
    headerName: "Appr. Qty",
    width: 110,
    align: "right",
    headerAlign: "right",
    renderCell: ({ value }) => value ?? "—",
  },
  { field: "reason", headerName: "Reason", flex: 1, minWidth: 200 },
];

interface Props {
  riskDecisions: RiskRow[];
}

export default function RiskPanel({ riskDecisions }: Props) {
  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Risk Decisions
      </Typography>
      <DataGrid
        rows={riskDecisions}
        columns={columns}
        density="compact"
        disableRowSelectionOnClick
        hideFooterSelectedRowCount
        pageSizeOptions={[25, 50]}
        initialState={{ pagination: { paginationModel: { pageSize: 25 } } }}
        localeText={{ noRowsLabel: "No risk decisions yet" }}
        sx={{ flex: 1, border: "none" }}
      />
    </Paper>
  );
}
