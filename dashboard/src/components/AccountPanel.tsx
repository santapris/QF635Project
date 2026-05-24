import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import Box from "@mui/material/Box";
import { DataGrid } from "@mui/x-data-grid";
import type { GridColDef } from "@mui/x-data-grid";
import type { AccountSnapshot, AccountBalance } from "../store/pipelineStore";

const columns: GridColDef<AccountBalance & { id: string; total: string }>[] = [
  { field: "asset", headerName: "Asset", flex: 1, minWidth: 80 },
  { field: "free", headerName: "Free", flex: 1, minWidth: 110, align: "right", headerAlign: "right" },
  { field: "locked", headerName: "Locked", flex: 1, minWidth: 110, align: "right", headerAlign: "right" },
  { field: "total", headerName: "Total", flex: 1, minWidth: 110, align: "right", headerAlign: "right" },
];

interface Props {
  account: AccountSnapshot | null;
}

export default function AccountPanel({ account }: Props) {
  const rows = (account?.balances ?? [])
    .map((b) => {
      const total = (parseFloat(b.free) + parseFloat(b.locked)).toString();
      return { ...b, id: b.asset, total };
    })
    .filter((r) => parseFloat(r.total) !== 0);

  return (
    <Paper sx={{ p: 2, height: "100%", display: "flex", flexDirection: "column" }}>
      <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
        <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
          Account
        </Typography>
        {account && (
          <Typography variant="caption" color="text.secondary">
            {new Date(account.ts).toLocaleTimeString()}
          </Typography>
        )}
      </Box>
      <Box sx={{ flex: 1, minHeight: 0, mt: 1 }}>
        <DataGrid
          rows={rows}
          columns={columns}
          density="compact"
          disableRowSelectionOnClick
          hideFooter
          localeText={{ noRowsLabel: account ? "No non-zero balances" : "Waiting for account snapshot…" }}
          sx={{ border: "none", height: "100%" }}
        />
      </Box>
    </Paper>
  );
}
