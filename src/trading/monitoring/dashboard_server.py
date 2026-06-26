"""Real-time dashboard server.

Two channels with distinct semantics:

- ``GET  /state/positions``, ``GET /state/account`` and
  ``GET /state/killswitch`` are *snapshot* endpoints. They return the
  current state-of-the-world (open positions, exchange-reported balances,
  kill-switch latch) by reading directly from the
  :class:`~trading.position.engine.PositionEngine` /
  :class:`~trading.risk.engine.RiskEngine` and from the latest
  :class:`AccountSnapshotEvent` cached locally. The frontend fetches
  these on mount and polls them periodically — works regardless of
  whether the user opens the dashboard before or after pipeline start.
- ``POST /command/killswitch/reset`` is the one *control* endpoint: it
  re-arms the latched kill switch. The reset is marshalled onto the
  trading loop so the engine stays the sole mutator of switch state.
- ``WS /ws`` is the *event stream*. It carries activity events (ticks,
  trades, signals, risk decisions, orders, fills, alerts) and log
  records. Clients see only events that fire while connected. No
  state-of-the-world topics are pushed on the WS — those belong to the
  REST endpoints.

This split matches how production dashboards typically work: state is
queryable and cacheable; events are streamed.

Usage (wire into any stage or LiveApp)::

    from trading.monitoring import DashboardServer

    dashboard = DashboardServer(bus=bus, port=8765, position_engine=position)
    await dashboard.start()
    # ...run app...
    await dashboard.stop()

Wire-in for LiveApp is handled automatically when ``dashboard_port`` is set
in the app config; see :class:`~trading.config.builder.LiveApp`.

WebSocket envelope::

    {
        "topic":      "fills",          # bus topic name, or "logs"
        "event_type": "FillEvent",
        "timestamp":  "2026-05-23T...", # ISO-8601, ms precision
        "data":       { ...fields... }
    }
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import MutableSet
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

from ..core.events import (
    AccountSnapshotEvent,
    BaseEvent,
    MicrostructureSnapshotEvent,
    OpenOrdersSnapshotEvent,
    StrategyDiagnosticsEvent,
    VenuePositionSnapshotEvent,
)
from ..event_bus.base import AbstractEventBus, Topic

if TYPE_CHECKING:
    from ..position.engine import PositionEngine
    from ..risk.engine import RiskEngine
    from .latency import LatencyCollector

_log = structlog.get_logger(__name__)

# Bus topics streamed on the WebSocket as activity events. Position/account
# state is intentionally NOT included — clients query REST for those.
_STREAM_TOPICS = (
    Topic.MARKET_DATA,
    Topic.SIGNALS,
    Topic.RISK_DECISIONS,
    Topic.ORDERS,
    Topic.FILLS,
    Topic.ALERTS,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


_BACKTEST_IDLE: dict[str, Any] = {
    "status": "idle", "result": None, "error": None,
    "started_at": None, "completed_at": None,
}

# src/trading/monitoring/dashboard_server.py -> repo_root/configs
_CONFIGS_DIR = Path(__file__).resolve().parents[3] / "configs"


def _resolve_backtest_config_path(name: str) -> str | None:
    """Map a bare config filename (as offered by /backtest/configs) to a real
    path under _CONFIGS_DIR. Rejects anything else to avoid arbitrary file
    reads via a client-supplied path."""
    candidate = _CONFIGS_DIR / Path(name).name
    if candidate.is_file() and candidate.suffix == ".toml":
        return str(candidate)
    return None


def _event_to_message(topic: str, event: BaseEvent) -> str:
    data = event.model_dump(mode="json")
    event_type = data.pop("event_type", type(event).__name__)
    envelope = {
        "topic": topic,
        "event_type": event_type,
        "timestamp": _now_iso(),
        "data": data,
    }
    return json.dumps(envelope, default=str)


def _log_to_message(record: dict[str, Any]) -> str:
    level = record.get("level", "info")
    logger = record.get("logger", "")
    event = record.get("event", "")
    timestamp = record.get("timestamp", _now_iso())
    extra = {
        k: v
        for k, v in record.items()
        if k not in {"level", "logger", "event", "timestamp", "_record"}
    }
    envelope = {
        "topic": "logs",
        "event_type": "LogRecord",
        "timestamp": timestamp,
        "data": {
            "level": str(level).lower(),
            "logger": str(logger),
            "message": str(event),
            "extra": {k: str(v) for k, v in extra.items()},
        },
    }
    return json.dumps(envelope, default=str)


class _StructlogForwarder:
    """Structlog processor that queues log records for the dashboard."""

    def __init__(self, queue: asyncio.Queue[str]) -> None:
        self._queue = queue

    def __call__(
        self, logger: Any, method: str, event_dict: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            msg = _log_to_message(event_dict)
            self._queue.put_nowait(msg)
        except asyncio.QueueFull:
            pass  # drop silently — dashboard is best-effort
        except RuntimeError:
            pass  # no running loop yet (startup logging)
        return event_dict


class DashboardServer:
    """REST + WebSocket server backing the operator dashboard.

    Parameters
    ----------
    bus:
        The application event bus. Subscribed to streaming topics on
        :meth:`start`.
    port:
        TCP port (default ``8765``).
    host:
        Bind address (default ``0.0.0.0`` — all interfaces, needed for WSL2).
    position_engine:
        Optional reference to the running :class:`PositionEngine`. When
        present, ``GET /state/positions`` reads live state directly from
        the engine; otherwise the endpoint returns an empty list.
    risk_engine:
        Optional reference to the running :class:`RiskEngine`. When present,
        ``GET /state/killswitch`` reads the live latch state and
        ``POST /command/killswitch/reset`` re-arms the switch. Absent → those
        endpoints report/​return 503.
    """

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        port: int = 8765,
        host: str = "0.0.0.0",
        position_engine: "PositionEngine | None" = None,
        latency_collector: "LatencyCollector | None" = None,
        risk_engine: "RiskEngine | None" = None,
    ) -> None:
        self._bus = bus
        self._port = port
        self._host = host
        self._position_engine = position_engine
        self._latency_collector = latency_collector
        self._risk_engine = risk_engine
        # The trading event loop, captured on start(). The uvicorn server runs
        # on its own thread/loop; any write into engine state (kill-switch
        # reset) must be marshalled back onto *this* loop, where the engine
        # mutates its own state, rather than touched from the HTTP thread.
        self._trading_loop: asyncio.AbstractEventLoop | None = None
        self._clients: MutableSet[Any] = set()
        self._broadcast_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=10_000)
        self._broadcaster_task: asyncio.Task[None] | None = None
        self._server: Any = None  # uvicorn Server instance
        self._server_thread: threading.Thread | None = None
        # Latest exchange-reported account snapshot. Written by the bus
        # handler on the trading loop thread, read by REST handlers on the
        # uvicorn thread. Python dict and tuple assignment is atomic under
        # the GIL, so a plain attribute is sufficient — no lock needed.
        self._latest_account: AccountSnapshotEvent | None = None
        # Latest OMS working-order snapshot, cached for the REST endpoint
        # (same atomic-attribute pattern as _latest_account).
        self._latest_open_orders: OpenOrdersSnapshotEvent | None = None
        # Latest exchange-reported net positions (ground truth, for the
        # 'net' row shown alongside per-strategy positions).
        self._latest_venue_positions: VenuePositionSnapshotEvent | None = None
        # Latest analytics snapshots for /state/analytics REST endpoint.
        self._latest_microstructure: MicrostructureSnapshotEvent | None = None
        self._latest_diagnostics: StrategyDiagnosticsEvent | None = None
        # Event-driven pair backtest state, polled via REST and pushed on the
        # WS "backtest" topic. _backtest_lock is a threading.Lock (not asyncio)
        # because the backtest runs in a plain daemon thread, not the event
        # loop — uvicorn's loop and the main trading loop are different loops,
        # so cross-thread access here must not assume asyncio primitives.
        self._backtest_state: dict[str, Any] = dict(_BACKTEST_IDLE)
        self._backtest_lock = threading.Lock()
        # Captured in start() so the backtest thread can call_soon_threadsafe
        # back onto the main loop to push WS broadcasts safely.
        self._main_loop: asyncio.AbstractEventLoop | None = None

    # ------------------------------------------------------------------
    # Public lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        try:
            import uvicorn
        except ImportError:
            _log.warning(
                "dashboard_disabled",
                reason="fastapi/uvicorn not installed",
                hint="pip install 'trading[dashboard]'",
            )
            return

        self._main_loop = asyncio.get_event_loop()
        # Capture the trading loop so HTTP handlers (on the uvicorn thread)
        # can marshal engine writes back here. start() runs on the trading loop.
        self._trading_loop = asyncio.get_running_loop()

        # Subscribe to streaming topics for the WS broadcast.
        for topic in _STREAM_TOPICS:
            await self._bus.subscribe(topic, self._on_stream_event_factory(topic))
        # Also subscribe to ACCOUNT and OPEN_ORDERS just to keep the latest
        # snapshots in memory for the REST endpoints — not broadcast on the WS.
        await self._bus.subscribe(Topic.ACCOUNT, self._on_account_event)
        await self._bus.subscribe(Topic.OPEN_ORDERS, self._on_open_orders_event)
        await self._bus.subscribe(Topic.VENUE_POSITIONS, self._on_venue_positions_event)
        await self._bus.subscribe(Topic.ANALYTICS, self._on_analytics_event)

        # Inject structlog forwarder.
        self._inject_structlog_processor()

        # Build app and run uvicorn in a background thread.
        app = self._build_app()
        config = uvicorn.Config(
            app,
            host=self._host,
            port=self._port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(config)

        self._server_thread = threading.Thread(
            target=self._server.run,
            name="dashboard-uvicorn",
            daemon=True,
        )
        self._server_thread.start()

        self._broadcaster_task = asyncio.create_task(
            self._broadcast_loop(), name="dashboard-broadcaster"
        )
        _log.info("dashboard_server_started", host=self._host, port=self._port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._server_thread is not None:
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: self._server_thread.join(timeout=5.0)  # type: ignore[union-attr]
            )
        if self._broadcaster_task is not None:
            self._broadcaster_task.cancel()
            try:
                await self._broadcaster_task
            except asyncio.CancelledError:
                pass
        _log.info("dashboard_server_stopped")

    # ------------------------------------------------------------------
    # State endpoints (REST)
    # ------------------------------------------------------------------

    def _positions_payload(self) -> dict[str, Any]:
        engine = self._position_engine
        positions: list[dict[str, Any]] = []
        if engine is not None:
            for position in engine.get_all_positions():
                positions.append({
                    "strategy_id": str(position.strategy_id),
                    "instrument": position.instrument.symbol,
                    "quantity": str(position.quantity),
                    "average_entry_price": str(position.average_entry_price),
                    "realized_pnl": str(position.realized_pnl),
                    "unrealized_pnl": str(position.unrealized_pnl),
                    "mark_price": str(position.mark_price),
                })
        # Venue net positions: ground truth from the exchange, shown as the
        # 'net' row. Comparable to the exchange UI; the per-strategy rows
        # above are our fill-derived attribution and need not match individually.
        venue: list[dict[str, Any]] = []
        snap = self._latest_venue_positions
        if snap is not None:
            for vp in snap.positions:
                venue.append({
                    "instrument": vp.instrument.symbol,
                    "net_quantity": str(vp.net_quantity),
                    "entry_price": str(vp.entry_price),
                    "mark_price": str(vp.mark_price),
                    "unrealized_pnl": str(vp.unrealized_pnl),
                })
        return {"timestamp": _now_iso(), "positions": positions, "venue_net": venue}

    def _account_payload(self) -> dict[str, Any]:
        snap = self._latest_account
        if snap is None:
            return {"timestamp": _now_iso(), "balances": []}
        return {
            "timestamp": _now_iso(),
            "balances": [
                {"asset": b.asset, "free": str(b.free), "locked": str(b.locked)}
                for b in snap.balances
            ],
        }

    def _open_orders_payload(self) -> dict[str, Any]:
        snap = self._latest_open_orders
        if snap is None:
            return {"timestamp": _now_iso(), "exposures": [], "orders": []}
        return {
            "timestamp": _now_iso(),
            "exposures": [
                {
                    "strategy_id": str(e.strategy_id),
                    "instrument": e.instrument.symbol,
                    "working_buy": str(e.working_buy),
                    "working_sell": str(e.working_sell),
                    "open_order_count": e.open_order_count,
                }
                for e in snap.exposures
            ],
            "orders": [
                {
                    "order_id": o.order_id,
                    "client_order_id": o.client_order_id,
                    "strategy_id": str(o.strategy_id),
                    "instrument": o.instrument.symbol,
                    "side": o.side.value,
                    "order_type": o.order_type.value,
                    "quantity": str(o.quantity),
                    "leaves_quantity": str(o.leaves_quantity),
                    "price": None if o.price is None else str(o.price),
                    "status": o.status.value,
                    "created_at_ns": o.created_at_ns,
                }
                for o in snap.orders
            ],
        }
    
    def _killswitch_payload(self) -> dict[str, Any]:
        """Current latch state. ``available`` is False when no engine is wired
        (the tab then shows an inert/unknown state rather than a false ARMED)."""
        engine = self._risk_engine
        if engine is None:
            return {"timestamp": _now_iso(), "available": False, "engaged": False}
        state = engine.kill_switch.state
        return {
            "timestamp": _now_iso(),
            "available": True,
            "engaged": state.engaged,
            "triggered_by": state.triggered_by,
            "reason": state.reason,
            "triggered_at_ns": state.triggered_at_ns,
        }

    async def _engage_killswitch(self, *, triggered_by: str, reason: str) -> dict[str, Any]:
        """Manually trip the switch on the trading loop and return fresh state.

        Routes through the engine (not kill_switch.engage directly) so the
        KillSwitchEvent is published — that drives the OMS cancel-all and the
        live dashboard update.
        """
        engine = self._risk_engine
        if engine is None:
            raise RuntimeError("no risk engine wired")
        await engine.engage_kill_switch(triggered_by=triggered_by, reason=reason)
        return self._killswitch_payload()

    async def _reset_killswitch(self) -> dict[str, Any]:
        """Re-arm the switch on the trading loop and return the fresh state.

        Marshalled from the HTTP thread onto the trading loop via
        run_coroutine_threadsafe so the engine remains the sole mutator of its
        own kill-switch state (matching the 'writes via engine only' invariant).
        """
        engine = self._risk_engine
        if engine is None:
            raise RuntimeError("no risk engine wired")
        engine.kill_switch.reset()
        _log.warning("kill_switch_reset_via_dashboard")
        return self._killswitch_payload()

    def _analytics_payload(self) -> dict[str, Any]:
        m = self._latest_microstructure
        d = self._latest_diagnostics
        return {
            "timestamp": _now_iso(),
            "microstructure": m.model_dump(mode="json") if m is not None else None,
            "strategy_diagnostics": d.model_dump(mode="json") if d is not None else None,
        }

    def _latency_payload(self) -> dict[str, Any]:
        if self._latency_collector is None:
            return {"timestamp": _now_iso(), "stages": {}}
        return {"timestamp": _now_iso(), "stages": self._latency_collector.snapshot()}

    # ------------------------------------------------------------------
    # Strategy backtest (runs the real deployed strategy/risk/OMS pipeline
    # against a TOML config + CSV data, via the existing BacktestEngine)
    # ------------------------------------------------------------------

    def _backtest_configs_payload(self) -> dict[str, Any]:
        """List config files under configs/ that have a [backtest] section."""
        configs: list[dict[str, str]] = []
        if _CONFIGS_DIR.is_dir():
            for path in sorted(_CONFIGS_DIR.glob("*.toml")):
                try:
                    text = path.read_text()
                except OSError:
                    continue
                if "[backtest]" in text:
                    configs.append({"name": path.name, "path": str(path)})
        return {"configs": configs}

    async def _run_strategy_backtest_async(self, config_path: str) -> dict[str, Any]:
        from ..config import build_backtest_app, load_config

        config = load_config(config_path)
        app = build_backtest_app(config)
        report = await app.run()
        d = report.to_dict()
        d["config_path"] = config_path
        d["equity_curve"] = [
            [p.ts_ns, p.total_pnl] for p in report.equity_points
        ]
        return d

    def _run_backtest_in_thread(self, params: dict[str, Any]) -> None:
        """Run the backtest synchronously in a fresh event loop. Called in a
        daemon thread by the endpoint — keeps the strategy/risk/OMS pipeline's
        own AsyncioBus isolated from the main trading loop entirely."""
        started_at = _now_iso()
        with self._backtest_lock:
            self._backtest_state = {
                "status": "running", "result": None, "error": None,
                "started_at": started_at, "completed_at": None,
            }

        config_name = params.get("config", "")
        config_path = _resolve_backtest_config_path(config_name)

        try:
            if config_path is None:
                raise ValueError(f"unknown backtest config: {config_name!r}")
            d = asyncio.run(self._run_strategy_backtest_async(config_path))

            with self._backtest_lock:
                self._backtest_state = {
                    "status": "complete", "result": d, "error": None,
                    "started_at": started_at, "completed_at": _now_iso(),
                }
            ws_data: dict[str, Any] = {"status": "complete", "result": d}

        except Exception as exc:
            _log.warning("backtest_failed", error=str(exc))
            with self._backtest_lock:
                self._backtest_state = {
                    "status": "error", "result": None, "error": str(exc),
                    "started_at": started_at, "completed_at": _now_iso(),
                }
            ws_data = {"status": "error", "result": None, "error": str(exc)}

        # Push result onto the WS broadcast queue via the main loop — this is
        # safe because call_soon_threadsafe is designed for cross-thread use.
        if self._main_loop is not None:
            msg = json.dumps({
                "topic": "backtest",
                "event_type": "backtest_result",
                "timestamp": _now_iso(),
                "data": ws_data,
            }, default=str)
            try:
                self._main_loop.call_soon_threadsafe(
                    self._broadcast_queue.put_nowait, msg
                )
            except Exception:
                pass  # best-effort; frontend also polls REST

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_app(self) -> Any:
        from starlette.applications import Starlette
        from starlette.middleware import Middleware
        from starlette.middleware.cors import CORSMiddleware
        from starlette.requests import Request
        from starlette.responses import JSONResponse
        from starlette.routing import Route, WebSocketRoute
        from starlette.websockets import WebSocket

        clients = self._clients

        async def positions_endpoint(request: Request) -> JSONResponse:
            return JSONResponse(self._positions_payload())

        async def account_endpoint(request: Request) -> JSONResponse:
            return JSONResponse(self._account_payload())

        async def open_orders_endpoint(request: Request) -> JSONResponse:
            return JSONResponse(self._open_orders_payload())

        async def analytics_endpoint(request: Request) -> JSONResponse:
            return JSONResponse(self._analytics_payload())

        async def backtest_configs_endpoint(request: Request) -> JSONResponse:
            return JSONResponse(self._backtest_configs_payload())

        async def backtest_run_endpoint(request: Request) -> JSONResponse:
            with self._backtest_lock:
                if self._backtest_state["status"] == "running":
                    return JSONResponse({"status": "running"}, status_code=409)
            try:
                params = await request.json()
            except Exception:
                params = {}
            threading.Thread(
                target=self._run_backtest_in_thread,
                args=(params,),
                name="backtest-run",
                daemon=True,
            ).start()
            return JSONResponse({"status": "running"})

        async def backtest_result_endpoint(request: Request) -> JSONResponse:
            with self._backtest_lock:
                return JSONResponse(self._backtest_state)

        async def latency_endpoint(request: Request) -> JSONResponse:
            return JSONResponse(self._latency_payload())

        async def killswitch_endpoint(request: Request) -> JSONResponse:
            return JSONResponse(self._killswitch_payload())

        async def killswitch_engage_endpoint(request: Request) -> JSONResponse:
            if self._risk_engine is None or self._trading_loop is None:
                return JSONResponse(
                    {"error": "kill switch control unavailable"}, status_code=503
                )
            try:
                body = await request.json()
            except Exception:
                body = {}
            triggered_by = str(body.get("triggered_by") or "operator")
            reason = str(body.get("reason") or "manual engage via dashboard")
            fut = asyncio.run_coroutine_threadsafe(
                self._engage_killswitch(triggered_by=triggered_by, reason=reason),
                self._trading_loop,
            )
            payload = await asyncio.wrap_future(fut)
            return JSONResponse(payload)

        async def killswitch_reset_endpoint(request: Request) -> JSONResponse:
            if self._risk_engine is None or self._trading_loop is None:
                return JSONResponse(
                    {"error": "kill switch control unavailable"}, status_code=503
                )
            # We are on the uvicorn thread; the reset must run on the trading
            # loop. Schedule it there and await the result from this thread.
            fut = asyncio.run_coroutine_threadsafe(
                self._reset_killswitch(), self._trading_loop
            )
            payload = await asyncio.wrap_future(fut)
            return JSONResponse(payload)

        async def websocket_endpoint(ws: WebSocket) -> None:
            await ws.accept()
            clients.add(ws)
            _log.debug("dashboard_client_connected", total=len(clients))
            try:
                while True:
                    msg = await ws.receive()
                    if msg["type"] == "websocket.disconnect":
                        break
            except Exception:
                pass
            finally:
                clients.discard(ws)
                _log.debug("dashboard_client_disconnected", total=len(clients))

        # CORSMiddleware handles both preflight OPTIONS and actual response
        # headers for all origins. The dashboard is mostly read-only, except
        # for POST /backtest/run which triggers an operator-initiated backtest.
        middleware = [
            Middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_methods=["GET", "POST", "OPTIONS"],
                allow_headers=["*"],
            )
        ]

        return Starlette(
            routes=[
                Route("/state/positions", positions_endpoint, methods=["GET"]),
                Route("/state/account", account_endpoint, methods=["GET"]),
                Route("/state/open_orders", open_orders_endpoint, methods=["GET"]),
                Route("/state/analytics", analytics_endpoint, methods=["GET"]),
                Route("/backtest/configs", backtest_configs_endpoint, methods=["GET"]),
                Route("/backtest/run", backtest_run_endpoint, methods=["POST"]),
                Route("/backtest/result", backtest_result_endpoint, methods=["GET"]),
                Route("/state/latency", latency_endpoint, methods=["GET"]),
                Route("/state/killswitch", killswitch_endpoint, methods=["GET"]),
                Route(
                    "/command/killswitch/engage",
                    killswitch_engage_endpoint,
                    methods=["POST"],
                ),
                Route(
                    "/command/killswitch/reset",
                    killswitch_reset_endpoint,
                    methods=["POST"],
                ),
                WebSocketRoute("/ws", websocket_endpoint),
            ],
            middleware=middleware,
        )

    def _on_stream_event_factory(self, topic: str):  # type: ignore[return]
        async def _handler(event: BaseEvent) -> None:
            try:
                msg = _event_to_message(topic, event)
                self._broadcast_queue.put_nowait(msg)
            except asyncio.QueueFull:
                pass  # drop bursts silently
        return _handler

    async def _on_account_event(self, event: BaseEvent) -> None:
        if isinstance(event, AccountSnapshotEvent):
            self._latest_account = event

    async def _on_open_orders_event(self, event: BaseEvent) -> None:
        if isinstance(event, OpenOrdersSnapshotEvent):
            self._latest_open_orders = event

    async def _on_venue_positions_event(self, event: BaseEvent) -> None:
        if isinstance(event, VenuePositionSnapshotEvent):
            self._latest_venue_positions = event
    
    async def _on_analytics_event(self, event: BaseEvent) -> None:
        if isinstance(event, MicrostructureSnapshotEvent):
            self._latest_microstructure = event
        elif isinstance(event, StrategyDiagnosticsEvent):
            self._latest_diagnostics = event

    async def _broadcast_loop(self) -> None:
        """Drain the broadcast queue and send to all connected clients."""
        while True:
            msg = await self._broadcast_queue.get()
            if not self._clients:
                continue
            dead: list[Any] = []
            for ws in list(self._clients):
                try:
                    await ws.send_text(msg)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                self._clients.discard(ws)

    def _inject_structlog_processor(self) -> None:
        """Append a forwarder processor to the live structlog chain."""
        forwarder = _StructlogForwarder(self._broadcast_queue)
        current = structlog.get_config()
        processors = list(current.get("processors", []))
        # Insert before the final renderer so the event_dict is still intact.
        processors.insert(-1, forwarder)
        structlog.configure(processors=processors)


__all__ = ["DashboardServer"]
