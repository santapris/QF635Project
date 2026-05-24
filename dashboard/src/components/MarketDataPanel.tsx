import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { TickData, TradeData } from "../store/pipelineStore";

interface TickRow extends TickData {
  id: string;
}

interface TradeRowWithId extends TradeData {
  id: string;
}

const tickColumns: GridColDef<TickRow>[] = [
  { field: "instrument", headerName: "Instrument", flex: 1, minWidth: 110 },
  { field: "bid_price", headerName: "Bid", flex: 1, minWidth: 100, align: "right", headerAlign: "right" },
  { field: "ask_price", headerName: "Ask", flex: 1, minWidth: 100, align: "right", headerAlign: "right" },
  { field: "bid_size", headerName: "Bid Sz", flex: 1, minWidth: 90, align: "right", headerAlign: "right" },
  { field: "ask_size", headerName: "Ask Sz", flex: 1, minWidth: 90, align: "right", headerAlign: "right" },
];

const tradeColumns: GridColDef<TradeRowWithId>[] = [
  { field: "instrument", headerName: "Instrument", flex: 1, minWidth: 110 },
  { field: "price", headerName: "Price", flex: 1, minWidth: 100, align: "right", headerAlign: "right" },
  { field: "quantity", headerName: "Qty", flex: 1, minWidth: 90, align: "right", headerAlign: "right" },
  {
    field: "aggressor_side",
    headerName: "Side",
    width: 90,
    renderCell: ({ value }) =>
      value ? (
        <Chip
          label={value}
          size="small"
          color={value === "BUY" ? "success" : "error"}
          sx={{ fontWeight: 700 }}
        />
      ) : null,
  },
];

interface Props {
  ticks: Record<string, TickData>;
  recentTrades: TradeData[];
}

export default function MarketDataPanel({ ticks, recentTrades }: Props) {
  const tickRows: TickRow[] = Object.values(ticks).map((t) => ({ ...t, id: t.instrument }));
  const tradeRows: TradeRowWithId[] = recentTrades
    .slice(0, 20)
    .map((t, i) => ({ ...t, id: `${t.ts}-${i}` }));
  const hasTicks = tickRows.length > 0;

  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Market Data
      </Typography>

      {hasTicks && (
        <>
          <Typography variant="caption" color="text.secondary">
            Best Bid/Ask
          </Typography>
          <Box sx={{ mb: 2 }}>
            <DataGrid
              rows={tickRows}
              columns={tickColumns}
              density="compact"
              disableRowSelectionOnClick
              hideFooter
              autoHeight
              sx={{ border: "none" }}
            />
          </Box>
        </>
      )}

      <Typography variant="caption" color="text.secondary">
        Recent Trades
      </Typography>
      <Box sx={{ flex: 1, minHeight: 0 }}>
        <DataGrid
          rows={tradeRows}
          columns={tradeColumns}
          density="compact"
          disableRowSelectionOnClick
          hideFooterSelectedRowCount
          pageSizeOptions={[10, 20]}
          initialState={{ pagination: { paginationModel: { pageSize: 10 } } }}
          localeText={{ noRowsLabel: "Waiting for trades…" }}
          sx={{ border: "none", height: "100%" }}
        />
      </Box>
    </Paper>
  );
}
