import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Chip from "@mui/material/Chip";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { FillRow } from "../store/pipelineStore";
import { formatTs } from "../utils/formatTs";
import { formatNum } from "../utils/formatNum";

const columns: GridColDef<FillRow>[] = [
  { field: "ts", headerName: "Time", width: 110, renderCell: ({ value }) => formatTs(value as number) },
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
  { field: "fill_price", headerName: "Price", width: 110, align: "right", headerAlign: "right", valueFormatter: (value) => formatNum(value as string) },
  { field: "fill_quantity", headerName: "Qty", width: 90, align: "right", headerAlign: "right", valueFormatter: (value) => formatNum(value as string) },
  { field: "fee", headerName: "Fee", width: 90, align: "right", headerAlign: "right", valueFormatter: (value) => formatNum(value as string) },
  {
    field: "is_maker",
    headerName: "Liquidity",
    width: 110,
    sortable: false,
    renderCell: ({ value }) =>
      value == null ? (
        "—"
      ) : (
        <Chip
          label={value ? "MAKER" : "TAKER"}
          size="small"
          variant="outlined"
          color={value ? "success" : "warning"}
        />
      ),
  },
];

interface Props {
  fills: FillRow[];
}

export default function FillsPanel({ fills }: Props) {
  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Fills
      </Typography>
      <DataGrid
        rows={fills}
        columns={columns}
        density="compact"
        disableRowSelectionOnClick
        hideFooterSelectedRowCount
        pageSizeOptions={[25, 50]}
        initialState={{ pagination: { paginationModel: { pageSize: 25 } } }}
        localeText={{ noRowsLabel: "No fills yet" }}
        sx={{ flex: 1, border: "none" }}
      />
    </Paper>
  );
}
