"""Real-time dashboard server.

Two channels with distinct semantics:

- ``GET  /state/positions`` and ``GET /state/account`` are *snapshot*
  endpoints. They return the current state-of-the-world (open positions,
  exchange-reported balances) by reading directly from the
  :class:`~trading.position.engine.PositionEngine` and from the latest
  :class:`AccountSnapshotEvent` cached locally. The frontend fetches
  these on mount and polls them periodically — works regardless of
  whether the user opens the dashboard before or after pipeline start.
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
from typing import TYPE_CHECKING, Any

import structlog

from ..core.events import AccountSnapshotEvent, BaseEvent
from ..event_bus.base import AbstractEventBus, Topic

if TYPE_CHECKING:
    from ..position.engine import PositionEngine

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
    """

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        port: int = 8765,
        host: str = "0.0.0.0",
        position_engine: "PositionEngine | None" = None,
    ) -> None:
        self._bus = bus
        self._port = port
        self._host = host
        self._position_engine = position_engine
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

        # Subscribe to streaming topics for the WS broadcast.
        for topic in _STREAM_TOPICS:
            await self._bus.subscribe(topic, self._on_stream_event_factory(topic))
        # Also subscribe to ACCOUNT just to keep the latest snapshot in
        # memory for the REST endpoint — not broadcast on the WS.
        await self._bus.subscribe(Topic.ACCOUNT, self._on_account_event)

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
        return {"timestamp": _now_iso(), "positions": positions}

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
        # headers for all origins. The dashboard is a read-only operator
        # tool; permissive CORS is fine here.
        middleware = [
            Middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_methods=["GET", "OPTIONS"],
                allow_headers=["*"],
            )
        ]

        return Starlette(
            routes=[
                Route("/state/positions", positions_endpoint, methods=["GET"]),
                Route("/state/account", account_endpoint, methods=["GET"]),
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
