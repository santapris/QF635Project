"""Optional HTTP health and metrics server.

Serves two endpoints:

- ``GET /healthz`` — liveness probe: returns ``{"status":"ok"}`` while
  the process is up. Suitable for Kubernetes readinessProbe / livenessProbe.
- ``GET /metrics`` — operational snapshot: returns a JSON object built from
  ``metrics_fn()``, which is typically :meth:`LiveApp.metrics_snapshot`.

Requires ``aiohttp>=3.9`` (already in the ``binance`` and ``dev`` extras).
If aiohttp is not installed, :meth:`start` logs a warning and returns
without binding — the app continues running but without the HTTP interface.

Usage:

    server = HealthServer(metrics_fn=app.metrics_snapshot, port=9090)
    await server.start()
    # ... run app ...
    await server.stop()
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any

_log = logging.getLogger(__name__)


class HealthServer:
    """Minimal async HTTP server exposing /healthz and /metrics."""

    def __init__(
        self,
        *,
        metrics_fn: Callable[[], dict],
        port: int = 9090,
        host: str = "0.0.0.0",
    ) -> None:
        self._metrics_fn = metrics_fn
        self._port = port
        self._host = host
        self._runner: Any = None

    async def start(self) -> None:
        try:
            from aiohttp import web
        except ImportError:
            _log.warning(
                "aiohttp not installed; health server on port %d is disabled. "
                "Install with: pip install 'aiohttp>=3.9'",
                self._port,
            )
            return

        app = web.Application()
        app.router.add_get("/healthz", self._handle_healthz)
        app.router.add_get("/metrics", self._handle_metrics)

        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        _log.info("health server listening on %s:%d", self._host, self._port)

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def _handle_healthz(self, request: Any) -> Any:
        from aiohttp import web
        return web.Response(
            text='{"status":"ok"}',
            content_type="application/json",
        )

    async def _handle_metrics(self, request: Any) -> Any:
        from aiohttp import web
        data = self._metrics_fn()
        return web.Response(
            text=json.dumps(data, default=str),
            content_type="application/json",
        )


__all__ = ["HealthServer"]
