"""Minimal WebSocket + HTTP stub that pushes risk decision demo events.

Serves ws://localhost:8765/ws  (WebSocket — what the dashboard connects to)
Serves GET /state/*             (REST stubs — return empty JSON so polls don't error)

Bypasses the full event-bus/DashboardServer machinery to avoid cross-loop issues.
"""

import asyncio
import json
import time
from datetime import datetime, timezone

from aiohttp import web

# ---------------------------------------------------------------------------
# Demo events — exact JSON shape the dashboard expects
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

def approved(bid: float, ask: float, qty: float = 0.01,
             clamp_rule: str | None = None, clamp_reason: str = "") -> dict:
    return {
        "topic": "risk-decisions",
        "event_type": "RiskDecision",
        "timestamp": _now_iso(),
        "data": {
            "ts_event": int(time.time() * 1e9),
            "ts_ingest": int(time.time() * 1e9),
            "source": "risk_engine",
            "signal_event_id": "demo",
            "strategy_id": "as_btcusdt",
            "approved": True,
            "severity": "INFO",
            "rule_name": None,
            "reason": "",
            "approved_legs": [
                {
                    "leg_id": "b1", "side": "BUY",
                    "approved_quantity": str(qty),
                    "rule_name": clamp_rule or "",
                    "clamp_reason": clamp_reason,
                },
                {
                    "leg_id": "a1", "side": "SELL",
                    "approved_quantity": str(qty),
                    "rule_name": clamp_rule or "",
                    "clamp_reason": clamp_reason,
                },
            ],
            "rejected_legs": [],
        },
    }

def rejected(rule: str, reason: str, severity: str = "BLOCK") -> dict:
    return {
        "topic": "risk-decisions",
        "event_type": "RiskDecision",
        "timestamp": _now_iso(),
        "data": {
            "ts_event": int(time.time() * 1e9),
            "ts_ingest": int(time.time() * 1e9),
            "source": "risk_engine",
            "signal_event_id": "demo",
            "strategy_id": "as_btcusdt",
            "approved": False,
            "severity": severity,
            "rule_name": rule,
            "reason": reason,
            "approved_legs": [],
            "rejected_legs": [
                {"leg_id": "b1", "side": "BUY", "rule_name": rule, "reason": reason, "severity": severity},
            ],
        },
    }

DEMO_EVENTS = [
    # Baseline — normal approved quote
    approved(64980, 65020),

    # R1 InstrumentAllowlist — BLOCK
    rejected("instrument_allowlist",
             "ETHUSDT not in allowlist ['BTCUSDT']; rejecting all legs"),

    # R2 MaxPosition — BLOCK
    rejected("max_position",
             "buy leg: confirmed pos 0.48 BTC + 0.04 BTC signal = 0.52 BTC > 0.50 BTC limit"),

    # R3 MaxOrderSize — APPROVED (clamped)
    approved(64955, 65045, qty=0.05,
             clamp_rule="max_order_size",
             clamp_reason="clamped 0.08 → 0.05 (max_order_size limit)"),

    # R4 MaxNotional — APPROVED (clamped)
    approved(64950, 65000, qty=0.076,
             clamp_rule="max_notional",
             clamp_reason="clamped 0.10 → 0.076 (notional 6504.50 > cap 5000.00)"),

    # R5 DailyLossLimit — KILL
    rejected("daily_loss_limit",
             "realized PnL -$2,043.20 < daily loss limit -$2,000.00; engaging kill switch",
             "KILL"),

    # R6 Throttle — BLOCK
    rejected("throttle",
             "signal rate 312/10s exceeds limit 300/10s; blocking this tick"),

    # R7 VPINCircuitBreaker — BLOCK escalating to KILL
    rejected("vpin_circuit_breaker",
             "VPIN 0.86 > threshold 0.80 (breach tick 4/5); blocking signal"),
    rejected("vpin_circuit_breaker",
             "VPIN 0.89 > threshold 0.80 for 5 sustained ticks; engaging kill switch",
             "KILL"),

    # R8 DrawdownCircuitBreaker — WARN then KILL
    rejected("drawdown_circuit_breaker",
             "session drawdown 13.7% >= warn threshold 10.0%; blocking until equity recovers",
             "WARN"),
    rejected("drawdown_circuit_breaker",
             "session drawdown 21.4% >= kill threshold 20.0%; engaging kill switch",
             "KILL"),
]

# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------

_connected: set = set()

async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    _connected.add(ws)
    print(f"Client connected (total={len(_connected)})")
    try:
        async for _ in ws:
            pass  # ignore any incoming messages
    finally:
        _connected.discard(ws)
        print(f"Client disconnected (total={len(_connected)})")
    return ws

# ---------------------------------------------------------------------------
# REST stubs — return empty-but-valid JSON so polling doesn't error
# ---------------------------------------------------------------------------

async def state_positions(_: web.Request) -> web.Response:
    return web.json_response({"strategy_positions": [], "venue_positions": []})

async def state_account(_: web.Request) -> web.Response:
    return web.json_response({"balances": [], "ts": _now_iso()})

async def state_open_orders(_: web.Request) -> web.Response:
    return web.json_response({"orders": [], "exposures": []})

async def state_analytics(_: web.Request) -> web.Response:
    return web.json_response({})

async def state_latency(_: web.Request) -> web.Response:
    return web.json_response({})

# ---------------------------------------------------------------------------
# Broadcaster — pushes events to all connected clients
# ---------------------------------------------------------------------------

async def broadcaster() -> None:
    """Wait for at least one client, then stream all demo events."""
    print("Waiting for browser to connect...")
    while not _connected:
        await asyncio.sleep(0.1)
    await asyncio.sleep(0.3)  # settle
    print(f"Broadcasting {len(DEMO_EVENTS)} demo events to {len(_connected)} client(s)...")
    for i, event in enumerate(DEMO_EVENTS):
        msg = json.dumps(event)
        dead = set()
        for ws in list(_connected):
            try:
                await ws.send_str(msg)
            except Exception:
                dead.add(ws)
        _connected.difference_update(dead)
        rule = event["data"].get("rule_name") or "APPROVED"
        sev = event["data"]["severity"] if not event["data"]["approved"] else ""
        print(f"  [{i+1:02d}] {rule:<28} {sev}", flush=True)
        await asyncio.sleep(0.2)
    print("Done. Server staying alive — Ctrl+C to stop.", flush=True)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    app = web.Application()
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/state/positions",   state_positions)
    app.router.add_get("/state/account",     state_account)
    app.router.add_get("/state/open_orders", state_open_orders)
    app.router.add_get("/state/analytics",   state_analytics)
    app.router.add_get("/state/latency",     state_latency)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8765)
    await site.start()
    print("Server ready on ws://localhost:8765/ws", flush=True)

    asyncio.create_task(broadcaster())
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
