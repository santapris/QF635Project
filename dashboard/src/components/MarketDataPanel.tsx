import { useMemo, useState } from "react";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import MenuItem from "@mui/material/MenuItem";
import Select from "@mui/material/Select";
import PauseIcon from "@mui/icons-material/Pause";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { TickData, TradeData } from "../store/pipelineStore";

interface TickRow extends TickData {
  id: string;
}

interface TradeRowWithId extends TradeData {
  id: string;
}

type TapeMode = "ticks" | "trades";

const ALL_INSTRUMENTS = "__ALL__";

const tickColumns: GridColDef<TickRow>[] = [
  { field: "instrument", headerName: "Instrument", flex: 1, minWidth: 110 },
  { field: "bid_price", headerName: "Bid", flex: 1, minWidth: 100, align: "right", headerAlign: "right" },
  { field: "ask_price", headerName: "Ask", flex: 1, minWidth: 100, align: "right", headerAlign: "right" },
  { field: "bid_size", headerName: "Bid Sz", flex: 1, minWidth: 90, align: "right", headerAlign: "right" },
  { field: "ask_size", headerName: "Ask Sz", flex: 1, minWidth: 90, align: "right", headerAlign: "right" },
];

const tickTapeColumns: GridColDef<TickRow>[] = [
  { field: "ts", headerName: "Time", width: 180 },
  { field: "instrument", headerName: "Instrument", width: 110 },
  { field: "bid_price", headerName: "Bid", flex: 1, minWidth: 90, align: "right", headerAlign: "right" },
  { field: "ask_price", headerName: "Ask", flex: 1, minWidth: 90, align: "right", headerAlign: "right" },
  { field: "bid_size", headerName: "Bid Sz", width: 80, align: "right", headerAlign: "right" },
  { field: "ask_size", headerName: "Ask Sz", width: 80, align: "right", headerAlign: "right" },
];

const tradeColumns: GridColDef<TradeRowWithId>[] = [
  { field: "ts", headerName: "Time", width: 180 },
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
  tickHistory: TickData[];
  recentTrades: TradeData[];
}

export default function MarketDataPanel({ ticks, tickHistory, recentTrades }: Props) {
  const [paused, setPaused] = useState(false);
  const [mode, setMode] = useState<TapeMode>("trades");
  const [instrument, setInstrument] = useState<string>(ALL_INSTRUMENTS);

  // Freeze the tape source when paused so the user can inspect without scrolling.
  const [frozenTicks, setFrozenTicks] = useState<TickData[] | null>(null);
  const [frozenTrades, setFrozenTrades] = useState<TradeData[] | null>(null);

  const togglePause = () => {
    if (paused) {
      setFrozenTicks(null);
      setFrozenTrades(null);
      setPaused(false);
    } else {
      setFrozenTicks(tickHistory);
      setFrozenTrades(recentTrades);
      setPaused(true);
    }
  };

  const tickRows: TickRow[] = Object.values(ticks).map((t) => ({ ...t, id: t.instrument }));
  const hasTicks = tickRows.length > 0;

  const instrumentOptions = useMemo(() => {
    const fromLatest = Object.keys(ticks);
    const fromHistory = tickHistory.map((t) => t.instrument);
    const fromTrades = recentTrades.map((t) => t.instrument);
    return Array.from(new Set([...fromLatest, ...fromHistory, ...fromTrades])).sort();
  }, [ticks, tickHistory, recentTrades]);

  const tapeTickRows: TickRow[] = useMemo(() => {
    const src = frozenTicks ?? tickHistory;
    const filtered = instrument === ALL_INSTRUMENTS
      ? src
      : src.filter((t) => t.instrument === instrument);
    return filtered.map((t, i) => ({ ...t, id: `${t.ts}-${t.instrument}-${i}` }));
  }, [frozenTicks, tickHistory, instrument]);

  const tapeTradeRows: TradeRowWithId[] = useMemo(() => {
    const src = frozenTrades ?? recentTrades;
    const filtered = instrument === ALL_INSTRUMENTS
      ? src
      : src.filter((t) => t.instrument === instrument);
    return filtered.map((t, i) => ({ ...t, id: `${t.ts}-${t.instrument}-${i}` }));
  }, [frozenTrades, recentTrades, instrument]);

  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
          Market Data
        </Typography>
        <Tooltip title={paused ? "Resume" : "Pause tape"}>
          <IconButton size="small" onClick={togglePause} color={paused ? "warning" : "default"}>
            {paused ? <PlayArrowIcon fontSize="small" /> : <PauseIcon fontSize="small" />}
          </IconButton>
        </Tooltip>
      </Box>

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

      <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1 }}>
        <ToggleButtonGroup
          size="small"
          exclusive
          value={mode}
          onChange={(_, v: TapeMode | null) => v && setMode(v)}
        >
          <ToggleButton value="trades" sx={{ py: 0.25, px: 1 }}>Trades</ToggleButton>
          <ToggleButton value="ticks" sx={{ py: 0.25, px: 1 }}>Ticks</ToggleButton>
        </ToggleButtonGroup>
        <Select
          size="small"
          value={instrument}
          onChange={(e) => setInstrument(e.target.value)}
          sx={{ minWidth: 140, height: 28 }}
        >
          <MenuItem value={ALL_INSTRUMENTS}>All instruments</MenuItem>
          {instrumentOptions.map((s) => (
            <MenuItem key={s} value={s}>{s}</MenuItem>
          ))}
        </Select>
        {paused && (
          <Chip label="PAUSED" size="small" color="warning" sx={{ fontWeight: 700 }} />
        )}
      </Box>

      <Box sx={{ flex: 1, minHeight: 0 }}>
        {mode === "ticks" ? (
          <DataGrid
            rows={tapeTickRows}
            columns={tickTapeColumns}
            density="compact"
            disableRowSelectionOnClick
            hideFooterSelectedRowCount
            pageSizeOptions={[25, 50, 100]}
            initialState={{ pagination: { paginationModel: { pageSize: 25 } } }}
            localeText={{ noRowsLabel: "Waiting for ticks…" }}
            sx={{ border: "none", height: "100%" }}
          />
        ) : (
          <DataGrid
            rows={tapeTradeRows}
            columns={tradeColumns}
            density="compact"
            disableRowSelectionOnClick
            hideFooterSelectedRowCount
            pageSizeOptions={[10, 20, 50]}
            initialState={{ pagination: { paginationModel: { pageSize: 10 } } }}
            localeText={{ noRowsLabel: "Waiting for trades…" }}
            sx={{ border: "none", height: "100%" }}
          />
        )}
      </Box>
    </Paper>
  );
}
