"""Batch 5: HealthServer /healthz and /metrics endpoints.

Tests spin up a real aiohttp server on a free port and hit it with a
client. Skipped if aiohttp is not installed.
"""

from __future__ import annotations

import json
import socket

import pytest

from trading.health import HealthServer


def _free_port() -> int:
    """Bind to port 0 and immediately release; returns the OS-assigned port."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _skip_if_no_aiohttp():
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        pytest.skip("aiohttp not installed")


# ---------------------------------------------------------------------------
# 5.1  /healthz returns 200 {"status":"ok"}
# ---------------------------------------------------------------------------

async def test_healthz_returns_ok() -> None:
    _skip_if_no_aiohttp()
    import aiohttp

    port = _free_port()
    server = HealthServer(metrics_fn=lambda: {}, port=port, host="127.0.0.1")
    await server.start()
    try:
        async with aiohttp.ClientSession() as client:
            async with client.get(f"http://127.0.0.1:{port}/healthz") as resp:
                assert resp.status == 200
                body = await resp.json()
                assert body["status"] == "ok"
    finally:
        await server.stop()


# ---------------------------------------------------------------------------
# 5.2  /metrics returns the metrics_fn output as JSON
# ---------------------------------------------------------------------------

async def test_metrics_returns_fn_output() -> None:
    _skip_if_no_aiohttp()
    import aiohttp

    expected = {"oms": {"open_orders": 3}, "risk": {"kill_switch_engaged": False}}
    port = _free_port()
    server = HealthServer(metrics_fn=lambda: expected, port=port, host="127.0.0.1")
    await server.start()
    try:
        async with aiohttp.ClientSession() as client:
            async with client.get(f"http://127.0.0.1:{port}/metrics") as resp:
                assert resp.status == 200
                body = await resp.json()
                assert body["oms"]["open_orders"] == 3
                assert body["risk"]["kill_switch_engaged"] is False
    finally:
        await server.stop()


# ---------------------------------------------------------------------------
# 5.3  /metrics calls metrics_fn fresh on every request
# ---------------------------------------------------------------------------

async def test_metrics_fn_called_per_request() -> None:
    _skip_if_no_aiohttp()
    import aiohttp

    counter = {"calls": 0}

    def _fn() -> dict:
        counter["calls"] += 1
        return {"call_number": counter["calls"]}

    port = _free_port()
    server = HealthServer(metrics_fn=_fn, port=port, host="127.0.0.1")
    await server.start()
    try:
        async with aiohttp.ClientSession() as client:
            async with client.get(f"http://127.0.0.1:{port}/metrics") as r1:
                body1 = await r1.json()
            async with client.get(f"http://127.0.0.1:{port}/metrics") as r2:
                body2 = await r2.json()
        assert body1["call_number"] == 1
        assert body2["call_number"] == 2
    finally:
        await server.stop()


# ---------------------------------------------------------------------------
# 5.4  stop() is idempotent
# ---------------------------------------------------------------------------

async def test_stop_idempotent() -> None:
    _skip_if_no_aiohttp()
    port = _free_port()
    server = HealthServer(metrics_fn=lambda: {}, port=port, host="127.0.0.1")
    await server.start()
    await server.stop()
    await server.stop()  # should not raise


# ---------------------------------------------------------------------------
# 5.5  start() without aiohttp logs a warning and does not raise
# ---------------------------------------------------------------------------

async def test_start_without_aiohttp_does_not_raise(monkeypatch) -> None:
    """If aiohttp import fails inside start(), a warning is logged and we continue."""
    import builtins
    real_import = builtins.__import__

    def _mock_import(name, *args, **kwargs):
        if name == "aiohttp":
            raise ImportError("mocked missing aiohttp")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _mock_import)

    server = HealthServer(metrics_fn=lambda: {}, port=19999)
    # Should not raise despite missing aiohttp.
    await server.start()
    await server.stop()
