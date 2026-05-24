import type { LogRow } from "../store/pipelineStore";
import LogViewer from "../components/LogViewer";

interface Props {
  logs: LogRow[];
  onClear: () => void;
}

export default function LogsPage({ logs, onClear }: Props) {
  return <LogViewer logs={logs} onClear={onClear} />;
}
