/**
 * Live architecture map of the trading system.
 *
 * The system is a pub/sub design: the Event Bus is the hub and every component
 * publishes to and subscribes from it. We model that honestly — the Bus sits at
 * the centre and every component links to it — and tame the resulting hairball
 * two ways:
 *
 *   1. Edges are coloured + labelled by Kafka topic (market-data, signals,
 *      risk-decisions, orders, fills, positions, analytics, alerts).
 *   2. Edges are dimmed by default and only light up when their topic is live;
 *      a topic filter lets the operator isolate a single flow.
 *
 * Node pulses and edge packets are driven entirely by the pipeline store
 * (see useFlowPulse). Pan/zoom/drag come free with React Flow.
 */
import { useEffect, useState } from "react";
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  Controls,
  Position,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Chip from "@mui/material/Chip";
import Slider from "@mui/material/Slider";
import Typography from "@mui/material/Typography";
import { useTheme } from "@mui/material/styles";
import type { PipelineState } from "../store/pipelineStore";
import { useFlowPulse, type NodeId, type DriverId, FRESH_MS, STALE_MS } from "../hooks/useFlowPulse";
import PipelineNode, { type PipelineNodeData } from "../components/architecture/PipelineNode";
import FlowEdge, { type FlowEdgeData } from "../components/architecture/FlowEdge";
import { BMW } from "../theme/theme";
import SectionLabel from "../components/bmw/SectionLabel";

const nodeTypes = { pipeline: PipelineNode };
const edgeTypes = { flow: FlowEdge };

// ── Kafka topics ─────────────────────────────────────────────────────────
// Each bus edge belongs to exactly one topic, coloured so a given flow is
// traceable across the hub. These are the topics from docs/architecture.
type Topic =
  | "market-data"
  | "signals"
  | "risk-decisions"
  | "orders"
  | "open-orders"
  | "fills"
  | "positions"
  | "analytics"
  | "alerts";

const TOPIC_COLOR: Record<Topic, string> = {
  "market-data":   BMW.mBlueLight,   // light blue
  signals:         BMW.mBlueDark,    // dark blue
  "risk-decisions":"#f4b400",        // amber
  orders:          BMW.electricBlue, // deep blue
  "open-orders":   "#00b3a4",        // teal — working-order snapshots
  fills:           "#0fa336",        // green
  positions:       "#0fa336",        // green (position lifecycle)
  analytics:       "#9b6dff",        // violet — set apart from the trade path
  alerts:          BMW.mRed,         // red
};

const TOPICS: Topic[] = [
  "market-data", "signals", "risk-decisions", "orders", "open-orders", "fills", "positions", "analytics", "alerts",
];

// Node accent — used for the box itself (its own activity colour).
const ACCENT: Record<NodeId, string> = {
  exchange: "#7e7e7e",
  feed: BMW.mBlueLight,
  bus: BMW.bmwBlue,
  strategy: BMW.mBlueDark,
  risk: "#f4b400",
  oms: BMW.electricBlue,
  gateway: "#7e7e7e",
  position: "#0fa336",
  analytics: "#9b6dff",
  killswitch: BMW.mRed,
};

interface NodeDef {
  id: NodeId;
  label: string;
  sublabel: string;
  x: number;
  y: number;
  inputs: Position[];
  outputs: Position[];
}

// ── Clockwise hub-and-spoke layout ────────────────────────────────────────
// The Event Bus sits dead centre. The eight components that touch the bus are
// placed on a ring AROUND it, walking CLOCKWISE in trade-lifecycle order so the
// eye follows the data cycle:
//
//        Analytics ── Strategy
//       ╱                ╲
//   Feed      ┌─────┐    Position
//    │        │ BUS │      │
//  Exchange   └─────┘    Risk
//       ╲                ╱
//        Gateway ──── OMS
//
//   Exchange → Gateway → OMS → Risk → Position → Strategy → Analytics → Feed
//   (Kill Switch hangs off Risk, outside)
//
// Angles are in screen space (0° = east/right, growing CLOCKWISE because the
// y-axis points down). We start Exchange at due-west (180°) and step clockwise.
const CX = 640; // ring centre x
const CY = 360; // ring centre y
const RX = 470; // horizontal radius (wider than tall — boxes are wide)
const RY = 300; // vertical radius

// Components in clockwise flow order, starting at due-west.
const RING_ORDER: NodeId[] = [
  "exchange", "feed", "analytics", "strategy", "position", "risk", "oms", "gateway",
];

/** Position on the ring for the i-th clockwise node (0 = west, going CW). */
function ringPos(i: number): { x: number; y: number } {
  const step = (2 * Math.PI) / RING_ORDER.length;
  const angle = Math.PI + i * step; // start at 180° (west), increase CW
  return {
    x: Math.round(CX + RX * Math.cos(angle) - 66), // -66 ≈ half node width, to centre the box
    y: Math.round(CY + RY * Math.sin(angle) - 24), // -24 ≈ half node height
  };
}

const RING_META: Record<
  Exclude<NodeId, "bus" | "killswitch">,
  { label: string; sublabel: string }
> = {
  exchange:  { label: "Exchange",      sublabel: "Binance WS / REST" },
  feed:      { label: "Feed Handler",  sublabel: "normalisation" },
  strategy:  { label: "Strategy",      sublabel: "signal engine" },
  risk:      { label: "Risk",          sublabel: "pre-trade rules" },
  oms:       { label: "OMS",           sublabel: "routing / orders" },
  gateway:   { label: "Order Gateway", sublabel: "exchange adapter" },
  position:  { label: "Position / PnL",sublabel: "fills → exposure" },
  analytics: { label: "Analytics",     sublabel: "microstructure" },
};

// All ring nodes carry handles on every side so radial spokes attach cleanly.
const ALL_SIDES = [Position.Top, Position.Bottom, Position.Left, Position.Right];

const NODE_DEFS: NodeDef[] = [
  { id: "bus", label: "Event Bus", sublabel: "asyncio / Kafka", x: CX - 66, y: CY - 24, inputs: ALL_SIDES, outputs: ALL_SIDES },
  ...RING_ORDER.map((id) => {
    const { x, y } = ringPos(RING_ORDER.indexOf(id));
    return { id, ...RING_META[id as keyof typeof RING_META], x, y, inputs: ALL_SIDES, outputs: ALL_SIDES };
  }),
  // Kill Switch hangs just outside the ring at bottom-right, not on the bus.
  { id: "killswitch", label: "Kill Switch", sublabel: "halt latch", x: CX + RX + 180, y: CY + 110, inputs: ALL_SIDES, outputs: [] },
];

interface EdgeDef {
  id: string;
  source: NodeId;
  target: NodeId;
  sourceHandle?: string;
  targetHandle?: string;
  topic: Topic;
  /** Flow whose activity drives this edge's packet (node or gateway sub-flow). */
  driver: DriverId;
  /** Optional latency-stage key from state.latency. */
  stage?: "tick_to_signal" | "signal_to_decision" | "decision_to_order" | "order_to_fill";
}

// Spoke edges attach on each node's BUS-FACING side (computed from the ring
// geometry) so every radial line runs straight from rim to centre. The bus
// uses the matching opposite side. Ring-tangential edges (exchange↔feed,
// oms→gateway, gateway→exchange) hop between neighbours along the rim instead.
//
//   node          bus-facing side   bus side
//   feed          right             left
//   analytics     bottom            top
//   strategy      bottom            top
//   risk          left              right
//   oms           left              right
//   gateway       top               bottom
//   position      top               bottom
const EDGE_DEFS: EdgeDef[] = [
  // ingress along the rim: exchange → feed, then feed publishes to bus
  { id: "e-ex-feed",   source: "exchange", target: "feed", sourceHandle: "out-top", targetHandle: "in-bottom", topic: "market-data", driver: "feed" },
  { id: "e-feed-bus",  source: "feed",     target: "bus",  sourceHandle: "out-right", targetHandle: "in-left", topic: "market-data", driver: "feed" },
  // bus → analytics: analytics CONSUMES market-data (it subscribes to
  // Topic.MARKET_DATA), so the ingress edge is market-data, driven by the feed
  // flow — NOT the analytics topic. The analytics topic is what analytics
  // PRODUCES, carried by e-an-bus below.
  { id: "e-bus-an",    source: "bus", target: "analytics", sourceHandle: "out-top", targetHandle: "in-bottom", topic: "market-data", driver: "feed" },
  // bus → strategy (market-data sub) ; strategy → bus (signals pub)
  { id: "e-bus-strat", source: "bus",      target: "strategy", sourceHandle: "out-top", targetHandle: "in-bottom", topic: "market-data", driver: "strategy", stage: "tick_to_signal" },
  { id: "e-strat-bus", source: "strategy", target: "bus",      sourceHandle: "out-right", targetHandle: "in-top", topic: "signals", driver: "strategy" },
  // bus → risk (signals sub) ; risk → bus (risk-decisions pub)
  { id: "e-bus-risk",  source: "bus",  target: "risk", sourceHandle: "out-right", targetHandle: "in-left", topic: "signals", driver: "risk", stage: "signal_to_decision" },
  { id: "e-risk-bus",  source: "risk", target: "bus",  sourceHandle: "out-left", targetHandle: "in-right", topic: "risk-decisions", driver: "risk" },
  // bus → oms (risk-decisions sub) ; oms → bus (orders pub)
  { id: "e-bus-oms",   source: "bus", target: "oms",  sourceHandle: "out-bottom", targetHandle: "in-right", topic: "risk-decisions", driver: "oms", stage: "decision_to_order" },
  { id: "e-oms-bus",   source: "oms", target: "bus",  sourceHandle: "out-top", targetHandle: "in-bottom", topic: "orders", driver: "gateway-orders" },
  // egress: oms → gateway → exchange (the order leaving the system). Both legs
  // are driven by ORDER activity, so an order packet flows the whole way out —
  // OMS→Gateway and Gateway→Exchange pulse together, 1:1.
  { id: "e-oms-gw",    source: "oms",     target: "gateway",  sourceHandle: "out-bottom", targetHandle: "in-right", topic: "orders", driver: "gateway-orders" },
  { id: "e-gw-ex",     source: "gateway", target: "exchange", sourceHandle: "out-bottom", targetHandle: "in-bottom", topic: "orders", driver: "gateway-orders" },
  // return: exchange → gateway → bus (a FILL coming back). Separate flow —
  // fills only occur when the exchange matches, so this does NOT mirror orders.
  { id: "e-ex-gw",     source: "exchange", target: "gateway", sourceHandle: "out-right", targetHandle: "in-left", topic: "fills", driver: "gateway-fills", stage: "order_to_fill" },
  { id: "e-gw-bus",    source: "gateway", target: "bus",      sourceHandle: "out-top", targetHandle: "in-bottom", topic: "fills", driver: "gateway-fills" },
  // bus → position (fills sub) ; position → bus (positions pub)
  { id: "e-bus-pos",   source: "bus",      target: "position", sourceHandle: "out-top", targetHandle: "in-top", topic: "fills", driver: "position" },
  { id: "e-pos-bus",   source: "position", target: "bus",      sourceHandle: "out-bottom", targetHandle: "in-right", topic: "positions", driver: "position" },
  // risk → kill switch (alerts) — short hop out to the latch beside Risk
  { id: "e-risk-ks",   source: "risk", target: "killswitch", sourceHandle: "out-right", targetHandle: "in-left", topic: "alerts", driver: "killswitch" },

  // ── Feedback edges ─────────────────────────────────────────────────────
  // The trade path above is one direction; the system is actually a feedback
  // mesh. These close the loops that the spine omits. Each is backed by a real
  // bus subscription in the backend (see risk/oms/strategy/position engines).

  // fills fan out: besides position, BOTH strategy and risk subscribe to fills.
  // (strategy/registry.py subscribes FILLS; risk/engine.py subscribes FILLS)
  { id: "e-bus-strat-fill", source: "bus", target: "strategy", sourceHandle: "out-top", targetHandle: "in-top", topic: "fills", driver: "gateway-fills" },
  { id: "e-bus-risk-fill",  source: "bus", target: "risk",     sourceHandle: "out-right", targetHandle: "in-top", topic: "fills", driver: "gateway-fills" },
  // positions fan out: strategy and risk both subscribe to positions.
  { id: "e-bus-strat-pos",  source: "bus", target: "strategy", sourceHandle: "out-left", targetHandle: "in-left", topic: "positions", driver: "position" },
  { id: "e-bus-risk-pos",   source: "bus", target: "risk",     sourceHandle: "out-right", targetHandle: "in-bottom", topic: "positions", driver: "position" },
  // analytics is not a dead-end leaf: risk subscribes to ANALYTICS (e.g. VPIN
  // circuit breaker). analytics → bus → risk.
  { id: "e-an-bus",   source: "analytics", target: "bus",  sourceHandle: "out-right", targetHandle: "in-right", topic: "analytics", driver: "analytics" },
  { id: "e-bus-risk-an", source: "bus",    target: "risk", sourceHandle: "out-right", targetHandle: "in-bottom", topic: "analytics", driver: "analytics" },
  // open-orders working-exposure loop: OMS publishes OPEN_ORDERS snapshots,
  // risk subscribes to track effective exposure. oms → bus → risk.
  { id: "e-oms-bus-oo", source: "oms", target: "bus",  sourceHandle: "out-left", targetHandle: "in-left", topic: "open-orders", driver: "oms" },
  { id: "e-bus-risk-oo", source: "bus", target: "risk", sourceHandle: "out-right", targetHandle: "in-left", topic: "open-orders", driver: "oms" },
  // alerts don't only come from risk: the feed (stale-feed / circuit-breaker)
  // and the gateway reconciler also publish to ALERTS.
  { id: "e-feed-alert", source: "feed",    target: "bus", sourceHandle: "out-top", targetHandle: "in-left", topic: "alerts", driver: "feed" },
  { id: "e-gw-alert",   source: "gateway", target: "bus", sourceHandle: "out-top", targetHandle: "in-bottom", topic: "alerts", driver: "gateway-fills" },
];

function fmtLatency(stage: PipelineState["latency"], key: EdgeDef["stage"]): string | null {
  if (!stage || !key) return null;
  const s = stage[key];
  if (!s || s.p50_ms == null) return null;
  return `p50 ${s.p50_ms.toFixed(1)}ms`;
}

function ArchitectureGraph({ state }: { state: PipelineState }) {
  const theme = useTheme();
  const halted = Boolean(state.killSwitch?.engaged);

  const pulse = useFlowPulse(state);

  // Flow-stream gain: a visual multiplier on each edge's measured events/sec.
  // 1× shows true throughput; lower calms a busy graph, higher exaggerates a
  // quiet one. Drives only the dot stream's speed/density, not the underlying
  // rate or node pulses.
  const [flowGain, setFlowGain] = useState(0);

  // Topic filter: null = show all. Selecting a topic isolates its edges.
  const [focusTopic, setFocusTopic] = useState<Topic | null>(null);

  // A timer so freshness rings + edge activity decay even when idle.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 500);
    return () => clearInterval(t);
  }, []);

  // Nodes/edges are created ONCE with stable identity + positions, then mutated
  // in place each tick. Replacing the whole array every 500ms makes React Flow
  // drop its measured dimensions and the graph blanks out — so we use the
  // controlled hooks and only patch `data`, leaving id/position untouched.
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<PipelineNodeData>>(
    NODE_DEFS.map((def) => ({
      id: def.id,
      type: "pipeline",
      position: { x: def.x, y: def.y },
      data: {
        label: def.label,
        sublabel: def.sublabel,
        inputs: def.inputs,
        outputs: def.outputs,
        accent: ACCENT[def.id],
        activity: 0,
        lastSeen: null,
        halted: false,
        now: Date.now(),
      },
    }))
  );

  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge<FlowEdgeData>>(
    EDGE_DEFS.map((def) => ({
      id: def.id,
      source: def.source,
      target: def.target,
      sourceHandle: def.sourceHandle,
      targetHandle: def.targetHandle,
      type: "flow",
      data: {
        accent: TOPIC_COLOR[def.topic],
        rate: 0,
        active: false,
        dimmed: false,
        latency: null,
      },
    }))
  );

  // Patch live data into existing nodes (identity + position preserved).
  // At flow rate 0 the whole animation is off: freeze activity (no pulse) and
  // null lastSeen (freshness reads "stale" → no live glow), matching the edges
  // which also go quiet. The kill-switch wash still shows when halted.
  useEffect(() => {
    const off = flowGain === 0;
    setNodes((nds) =>
      nds.map((n) => {
        const id = n.id as NodeId;
        return {
          ...n,
          data: {
            ...n.data,
            activity: off ? 0 : pulse[id].activity,
            lastSeen: off ? null : pulse[id].lastSeen,
            halted: halted && id !== "killswitch",
            now,
          },
        };
      })
    );
  }, [pulse, halted, now, flowGain, setNodes]);

  // Patch live data into existing edges.
  useEffect(() => {
    const byId = new Map(EDGE_DEFS.map((d) => [d.id, d]));
    setEdges((eds) =>
      eds.map((e) => {
        const def = byId.get(e.id)!;
        const driver = pulse[def.driver];
        const age = driver.lastSeen != null ? now - driver.lastSeen : Infinity;
        const live = age <= FRESH_MS;
        const focused = focusTopic == null || focusTopic === def.topic;
        // Fade the flow rate to 0 as the driver goes stale: the hook can't lower
        // a frozen rate once events stop (no render fires), so the 500ms `now`
        // timer decays it here. Linear fade over [FRESH_MS, STALE_MS].
        const fade = age <= FRESH_MS ? 1 : age >= STALE_MS ? 0 : 1 - (age - FRESH_MS) / (STALE_MS - FRESH_MS);
        return {
          ...e,
          data: {
            accent: halted ? BMW.mRed : TOPIC_COLOR[def.topic],
            rate: driver.rate * fade * flowGain,
            active: focused && (live || halted),
            dimmed: !focused,
            latency: focused ? fmtLatency(state.latency, def.stage) : null,
          },
        };
      })
    );
  }, [pulse, halted, now, state.latency, focusTopic, flowGain, setEdges]);

  return (
    <Box>
      {/* Topic filter — click to isolate one Kafka flow through the hub. */}
      <Stack direction="row" spacing={0.75} sx={{ mb: 1, flexWrap: "wrap", rowGap: 0.75 }}>
        <Chip
          label="All topics"
          size="small"
          onClick={() => setFocusTopic(null)}
          variant={focusTopic == null ? "filled" : "outlined"}
          sx={{
            bgcolor: focusTopic == null ? "action.selected" : "transparent",
            fontSize: 11, letterSpacing: "0.5px",
          }}
        />
        {TOPICS.map((t) => {
          const on = focusTopic === t;
          return (
            <Chip
              key={t}
              label={t}
              size="small"
              onClick={() => setFocusTopic(on ? null : t)}
              variant="outlined"
              sx={{
                fontSize: 11,
                letterSpacing: "0.5px",
                borderColor: TOPIC_COLOR[t],
                color: on ? "#000" : TOPIC_COLOR[t],
                bgcolor: on ? TOPIC_COLOR[t] : "transparent",
                "& .MuiChip-label": { fontWeight: on ? 700 : 400 },
                "&:hover": { bgcolor: on ? TOPIC_COLOR[t] : `${TOPIC_COLOR[t]}22` },
              }}
            />
          );
        })}
      </Stack>

      {/* Flow gain — visual multiplier on each edge's measured throughput. The
          dot stream's speed + density scale with real events/sec; this just
          dials the whole graph up or down for legibility. 1× = true rate. */}
      <Stack direction="row" spacing={1.5} sx={{ mb: 1, maxWidth: 380, alignItems: "center" }}>
        <Typography
          sx={{ fontSize: 11, letterSpacing: "0.5px", color: "text.secondary", whiteSpace: "nowrap" }}
        >
          FLOW RATE
        </Typography>
        <Slider
          size="small"
          value={flowGain}
          min={0}
          max={4}
          step={0.25}
          onChange={(_, v) => setFlowGain(v as number)}
          sx={{ color: BMW.bmwBlue }}
        />
      </Stack>

      <Box
        sx={{
          height: "calc(100vh - 240px)",
          border: `1px solid ${theme.palette.divider}`,
          "--rf-edge-idle": theme.palette.mode === "dark" ? "#6b6b6b" : "#9aa0a6",
          ".react-flow__attribution": { display: "none" },
        }}
      >
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          fitView
          proOptions={{ hideAttribution: true }}
          nodesConnectable={false}
          nodesDraggable
          minZoom={0.3}
          maxZoom={1.75}
        >
          <Background color={theme.palette.divider} gap={28} size={1} />
          <Controls showInteractive={false} />
        </ReactFlow>
      </Box>
    </Box>
  );
}

export default function ArchitecturePage({ state }: { state: PipelineState }) {
  return (
    <Box>
      <SectionLabel>System Architecture — Live</SectionLabel>
      <ReactFlowProvider>
        <ArchitectureGraph state={state} />
      </ReactFlowProvider>
    </Box>
  );
}
