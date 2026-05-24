"""Real-time dashboard WebSocket server.

Subscribes to all pipeline bus topics and broadcasts every event as a JSON
message to connected browser clients. Also injects a structlog processor so
that log records are forwarded on the ``logs`` pseudo-topic.

Usage (wire into any stage or LiveApp)::

    from trading.monitoring import DashboardServer

    dashboard = DashboardServer(bus=bus, port=8765)  # binds 0.0.0.0 by default
    await dashboard.start()
    # ...run app...
    await dashboard.stop()

Wire-in for LiveApp is handled automatically when ``dashboard_port`` is set
in the app config; see :class:`~trading.config.builder.LiveApp`.

Message envelope
----------------
Every WebSocket message is a JSON object::

    {
        "topic":      "fills",          # bus topic name, or "logs"
        "event_type": "FillEvent",
        "timestamp":  "2026-05-23T...", # ISO-8601, ms precision
        "data":       { ...fields... }
    }

For log records the envelope is::

    {
        "topic":      "logs",
        "event_type": "LogRecord",
        "timestamp":  "2026-05-23T...",
        "data": {
            "level":   "warning",
            "logger":  "trading.risk.engine",
            "message": "signal_rejected",
            "extra":   { "reason": "max_position_exceeded" }
        }
    }
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import MutableSet
from datetime import datetime, timezone
from typing import Any

import structlog

from ..core.events import BaseEvent
from ..event_bus.base import AbstractEventBus, Topic

_log = structlog.get_logger(__name__)

# Topics the server subscribes to.
_TOPICS = (
    Topic.MARKET_DATA,
    Topic.SIGNALS,
    Topic.RISK_DECISIONS,
    Topic.ORDERS,
    Topic.FILLS,
    Topic.POSITIONS,
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
    """FastAPI + uvicorn WebSocket server that broadcasts pipeline events.

    Parameters
    ----------
    bus:
        The application event bus. Subscribed to all pipeline topics on
        :meth:`start`.
    port:
        TCP port for the WebSocket server (default ``8765``).
    host:
        Bind address (default ``0.0.0.0`` — all interfaces, needed for WSL2).
    """

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        port: int = 8765,
        host: str = "0.0.0.0",
    ) -> None:
        self._bus = bus
        self._port = port
        self._host = host
        self._clients: MutableSet[Any] = set()
        self._broadcast_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=10_000)
        self._broadcaster_task: asyncio.Task[None] | None = None
        self._server: Any = None  # uvicorn Server instance
        self._server_thread: threading.Thread | None = None

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

        # Subscribe to all bus topics.
        for topic in _TOPICS:
            await self._bus.subscribe(topic, self._on_event_factory(topic))

        # Inject structlog forwarder.
        self._inject_structlog_processor()

        # Build FastAPI app.
        app = self._build_app()

        # Run uvicorn in a background thread so it has its own event loop
        # and does not fight the trading app for signal handlers.
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
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_app(self) -> Any:
        from starlette.applications import Starlette
        from starlette.routing import WebSocketRoute
        from starlette.websockets import WebSocket

        clients = self._clients

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

        return Starlette(routes=[WebSocketRoute("/ws", websocket_endpoint)])


    def _on_event_factory(self, topic: str):  # type: ignore[return]
        async def _handler(event: BaseEvent) -> None:
            try:
                msg = _event_to_message(topic, event)
                self._broadcast_queue.put_nowait(msg)
            except asyncio.QueueFull:
                pass  # drop market data bursts silently

        return _handler

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
        # structlog stores the processor chain on the bound logger wrapper.
        # The cleanest way to append without reconfiguring everything is to
        # wrap the current final processor.
        current = structlog.get_config()
        processors = list(current.get("processors", []))
        # Insert before the final renderer so the event_dict is still intact.
        processors.insert(-1, forwarder)
        structlog.configure(processors=processors)


__all__ = ["DashboardServer"]
