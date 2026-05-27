import { useState, useMemo } from "react";
import Paper from "@mui/material/Paper";
import Box from "@mui/material/Box";
import TextField from "@mui/material/TextField";
import MenuItem from "@mui/material/MenuItem";
import Button from "@mui/material/Button";
import Chip from "@mui/material/Chip";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { LogRow } from "../store/pipelineStore";
import { formatTs } from "../utils/formatTs";

const LEVELS = ["all", "debug", "info", "warning", "error", "critical"];

const LEVEL_COLOR: Record<string, "default" | "info" | "success" | "warning" | "error"> = {
  debug: "default",
  info: "info",
  warning: "warning",
  error: "error",
  critical: "error",
};

const columns: GridColDef<LogRow>[] = [
  { field: "ts", headerName: "Time", width: 110, renderCell: ({ value }) => formatTs(value as number) },
  {
    field: "level",
    headerName: "Level",
    width: 90,
    renderCell: ({ value }) => (
      <Chip
        label={value}
        size="small"
        color={LEVEL_COLOR[value as string] ?? "default"}
        sx={{ fontWeight: 600, textTransform: "uppercase", fontSize: 10 }}
      />
    ),
  },
  { field: "logger", headerName: "Logger", width: 160 },
  { field: "message", headerName: "Message", flex: 1, minWidth: 200 },
];

interface Props {
  logs: LogRow[];
  onClear: () => void;
}

export default function LogViewer({ logs, onClear }: Props) {
  const [level, setLevel] = useState("all");
  const [loggerFilter, setLoggerFilter] = useState("");
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    return logs.filter((r) => {
      if (level !== "all" && r.level !== level) return false;
      if (loggerFilter && !r.logger.includes(loggerFilter)) return false;
      if (search && !r.message.toLowerCase().includes(search.toLowerCase())) return false;
      return true;
    });
  }, [logs, level, loggerFilter, search]);

  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Box sx={{ display: "flex", gap: 2, mb: 2, flexWrap: "wrap", alignItems: "center" }}>
        <TextField
          select
          label="Level"
          value={level}
          onChange={(e) => setLevel(e.target.value)}
          size="small"
          sx={{ minWidth: 110 }}
        >
          {LEVELS.map((l) => (
            <MenuItem key={l} value={l}>
              {l}
            </MenuItem>
          ))}
        </TextField>
        <TextField
          label="Logger"
          value={loggerFilter}
          onChange={(e) => setLoggerFilter(e.target.value)}
          size="small"
          placeholder="filter by logger…"
          sx={{ minWidth: 180 }}
        />
        <TextField
          label="Search"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          size="small"
          placeholder="search messages…"
          sx={{ flex: 1, minWidth: 200 }}
        />
        <Button variant="outlined" size="small" color="error" onClick={onClear}>
          Clear
        </Button>
      </Box>
      <DataGrid
        rows={filtered}
        columns={columns}
        density="compact"
        disableRowSelectionOnClick
        hideFooterSelectedRowCount
        pageSizeOptions={[50, 100, 200]}
        initialState={{ pagination: { paginationModel: { pageSize: 50 } } }}
        sx={{
          flex: 1,
          border: "none",
          fontFamily: "monospace",
          fontSize: 12,
        }}
        getRowClassName={({ row }) =>
          row.level === "error" || row.level === "critical" ? "log-row-error" : ""
        }
      />
    </Paper>
  );
}
