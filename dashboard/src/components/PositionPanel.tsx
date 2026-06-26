import { useMemo, useState } from "react";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import Tooltip from "@mui/material/Tooltip";
import IconButton from "@mui/material/IconButton";
import Dialog from "@mui/material/Dialog";
import DialogTitle from "@mui/material/DialogTitle";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogActions from "@mui/material/DialogActions";
import Button from "@mui/material/Button";
import PauseCircleOutlineIcon from "@mui/icons-material/PauseCircleOutlined";
import PlayCircleOutlineIcon from "@mui/icons-material/PlayCircleOutlined";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { PositionRow, VenueNetRow, StrategyInfo } from "../store/pipelineStore";
import { formatNum } from "../utils/formatNum";
import { pauseStrategy, resumeStrategy } from "../hooks/strategyCommands";

function pnlColor(val: string): string {
  const n = parseFloat(val);
  if (n > 0) return "success.main";
  if (n < 0) return "error.main";
  return "text.primary";
}

// A position row may be real (fill-derived position) or synthetic — a strategy
// that is registered but currently flat, so it has no position but must still
// be listed and controllable. Synthetic rows carry empty numeric fields.
interface StrategyRow extends PositionRow {
  synthetic: boolean;
}

const numCol = (field: keyof PositionRow, headerName: string, minWidth = 110): GridColDef<StrategyRow> => ({
  field: field as string,
  headerName,
  flex: 1,
  minWidth,
  align: "right",
  headerAlign: "right",
  valueFormatter: (value) => formatNum(value as string),
});

const pnlCol = (field: keyof PositionRow, headerName: string): GridColDef<StrategyRow> => ({
  field: field as string,
  headerName,
  flex: 1,
  minWidth: 130,
  align: "right",
  headerAlign: "right",
  renderCell: ({ value }) => (
    <Box sx={{ color: pnlColor(value as string), fontWeight: 600 }}>{formatNum(value as string)}</Box>
  ),
});

type PendingAction = { action: "pause" | "resume"; strategyId: string };

const venueColumns: GridColDef<VenueNetRow>[] = [
  { field: "instrument", headerName: "Instrument", flex: 1, minWidth: 120 },
  {
    field: "net_quantity",
    headerName: "Net Qty",
    flex: 1,
    minWidth: 90,
    align: "right",
    headerAlign: "right",
    renderCell: ({ value }) => {
      const n = parseFloat(value as string);
      return (
        <Box sx={{ color: n > 0 ? "success.main" : n < 0 ? "error.main" : "text.primary", fontWeight: 600 }}>
          {formatNum(value as string)}
        </Box>
      );
    },
  },
  {
    field: "entry_price",
    headerName: "Entry",
    flex: 1,
    minWidth: 110,
    align: "right",
    headerAlign: "right",
    valueFormatter: (value) => formatNum(value as string),
  },
  {
    field: "mark_price",
    headerName: "Mark",
    flex: 1,
    minWidth: 110,
    align: "right",
    headerAlign: "right",
    valueFormatter: (value) => formatNum(value as string),
  },
  {
    field: "unrealized_pnl",
    headerName: "Unrealized PnL",
    flex: 1,
    minWidth: 130,
    align: "right",
    headerAlign: "right",
    renderCell: ({ value }) => (
      <Box sx={{ color: pnlColor(value as string), fontWeight: 600 }}>{formatNum(value as string)}</Box>
    ),
  },
];

interface Props {
  positions: Record<string, PositionRow>;
  venueNet: VenueNetRow[];
  strategies: StrategyInfo[];
}

export default function PositionPanel({ positions, venueNet, strategies }: Props) {
  const [pending, setPending] = useState<PendingAction | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Authoritative pause state comes from /state/strategies (covers flat
  // strategies too); positions only carry a best-effort copy.
  const pausedById = useMemo(() => {
    const m = new Map<string, boolean>();
    for (const s of strategies) m.set(s.strategy_id, s.paused);
    return m;
  }, [strategies]);

  // Union of position rows and registered strategies: a strategy with an open
  // position contributes its real row(s); a flat registered strategy gets one
  // synthetic placeholder row so it stays listed and controllable.
  const rows = useMemo<StrategyRow[]>(() => {
    const out: StrategyRow[] = [];
    const seen = new Set<string>();
    for (const p of Object.values(positions)) {
      seen.add(p.strategy_id);
      out.push({ ...p, paused: pausedById.get(p.strategy_id) ?? p.paused, synthetic: false });
    }
    for (const s of strategies) {
      if (seen.has(s.strategy_id)) continue;
      out.push({
        id: `${s.strategy_id}:flat`,
        strategy_id: s.strategy_id,
        instrument: "—",
        quantity: "0",
        average_entry_price: "0",
        unrealized_pnl: "0",
        realized_pnl: "0",
        mark_price: "0",
        ts: Date.now(),
        paused: s.paused,
        synthetic: true,
      });
    }
    return out;
  }, [positions, strategies, pausedById]);

  const runPending = async () => {
    if (!pending) return;
    setBusy(true);
    setError(null);
    try {
      if (pending.action === "pause") await pauseStrategy(pending.strategyId);
      else await resumeStrategy(pending.strategyId);
      setPending(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "command failed");
    } finally {
      setBusy(false);
    }
  };

  const columns = useMemo<GridColDef<StrategyRow>[]>(() => [
    { field: "strategy_id", headerName: "Strategy", flex: 1, minWidth: 110 },
    {
      field: "instrument",
      headerName: "Instrument",
      flex: 1,
      minWidth: 120,
      renderCell: ({ row, value }) =>
        row.paused ? (
          <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
            <span>{value as string}</span>
            <Chip label="PAUSED" color="warning" size="small" sx={{ height: 18, fontSize: 10 }} />
          </Box>
        ) : (
          (value as string)
        ),
    },
    numCol("quantity", "Qty", 90),
    numCol("average_entry_price", "Avg Entry"),
    numCol("mark_price", "Mark"),
    pnlCol("unrealized_pnl", "Unrealized PnL"),
    pnlCol("realized_pnl", "Realized PnL"),
    {
      field: "actions",
      headerName: "",
      width: 56,
      sortable: false,
      filterable: false,
      align: "center",
      headerAlign: "center",
      renderCell: ({ row }) => {
        const paused = row.paused;
        return (
          <Tooltip title={paused ? "Resume strategy" : "Pause strategy (cancels resting orders)"}>
            <IconButton
              size="small"
              color={paused ? "success" : "warning"}
              onClick={() =>
                setPending({ action: paused ? "resume" : "pause", strategyId: row.strategy_id })
              }
            >
              {paused ? (
                <PlayCircleOutlineIcon fontSize="small" />
              ) : (
                <PauseCircleOutlineIcon fontSize="small" />
              )}
            </IconButton>
          </Tooltip>
        );
      },
    },
  ], []);

  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      {/* Exchange net — ground truth, comparable to the Binance UI. */}
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        Net Position (exchange)
      </Typography>
      <Box sx={{ mb: 1 }}>
        <DataGrid
          rows={venueNet}
          columns={venueColumns}
          density="compact"
          disableRowSelectionOnClick
          hideFooter
          autoHeight
          getRowId={(row) => row.id}
          localeText={{ noRowsLabel: "No exchange position" }}
          sx={{ border: "none" }}
        />
      </Box>
      {/* Per-strategy breakdown — our fill-derived attribution. */}
      <Typography variant="subtitle2" gutterBottom sx={{ fontWeight: 700 }}>
        By Strategy
      </Typography>
      <Box sx={{ flex: 1, minHeight: 0 }}>
        <DataGrid
          rows={rows}
          columns={columns}
          density="compact"
          disableRowSelectionOnClick
          hideFooter
          getRowId={(row) => row.id}
          localeText={{ noRowsLabel: "No strategies" }}
          sx={{ border: "none", height: "100%" }}
        />
      </Box>

      <Dialog open={pending !== null} onClose={() => (busy ? null : setPending(null))}>
        <DialogTitle>
          {pending?.action === "pause" ? "Pause strategy?" : "Resume strategy?"}
        </DialogTitle>
        <DialogContent>
          <DialogContentText>
            {pending?.action === "pause" ? (
              <>
                Pause <b>{pending?.strategyId}</b>? This stops new signals and
                cancels its resting orders. The open position is left untouched.
              </>
            ) : (
              <>
                Resume <b>{pending?.strategyId}</b>? It will start quoting again
                on the next market event.
              </>
            )}
          </DialogContentText>
          {error && (
            <Typography color="error" variant="body2" sx={{ mt: 1 }}>
              {error}
            </Typography>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setPending(null)} disabled={busy}>
            Cancel
          </Button>
          <Button
            onClick={runPending}
            disabled={busy}
            variant="contained"
            color={pending?.action === "pause" ? "warning" : "success"}
          >
            {pending?.action === "pause" ? "Pause" : "Resume"}
          </Button>
        </DialogActions>
      </Dialog>
    </Paper>
  );
}
