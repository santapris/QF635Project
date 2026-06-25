"""Live / paper-trading runner.

Generic entry point: paper vs. live is decided by the TOML's
``[[order_gateways]]`` block (``type = "simulation"`` for paper,
``type = "binance"`` for Binance, etc.).

Usage:
    python -m trading.runners.run_live --config configs/paper_example.toml
    python -m trading.runners.run_live --config configs/binance_testnet.toml

Starts the wired-up app, then waits for SIGINT/SIGTERM. On shutdown signal,
stops cleanly and exits.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import signal
import structlog
import sys
from pathlib import Path

from trading.config import build_live_app, load_config, load_settings
from trading.health import HealthServer
from trading.logging import configure_logging
from trading.monitoring import DashboardServer


def _parse_args(argv: list[str], settings) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the live trading app.")
    p.add_argument("--config", "-c", type=Path, required=True)
    p.add_argument(
        "--log-level", default=settings.log_level,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    p.add_argument(
        "--health-port", type=int, default=9090,
        help="Port for /healthz and /metrics HTTP server (0 = disabled).",
    )
    p.add_argument(
        "--dashboard-port", type=int, default=settings.dashboard_port,
        help="Port for the real-time dashboard WebSocket server (0 = disabled).",
    )
    return p.parse_args(argv)


async def _amain(args: argparse.Namespace) -> int:
    configure_logging(level=args.log_level)
    _log = structlog.get_logger("trading.runner")

    config = load_config(args.config)
    app = build_live_app(config)

    if args.health_port > 0:
        app.health_server = HealthServer(
            metrics_fn=app.metrics_snapshot,
            port=args.health_port,
        )
    if args.dashboard_port > 0:
        app.dashboard_server = DashboardServer(
            bus=app.bus,
            port=args.dashboard_port,
        )

    await app.start()
    _log.info("trading_app_started")

    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        _log.info("shutdown_signal_received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        _log.info("keyboard_interrupt_shutting_down")
    finally:
        await app.stop()
        _log.info("trading_app_stopped")
    return 0


def main(argv: list[str] | None = None) -> int:
    settings = load_settings()
    args = _parse_args(argv if argv is not None else sys.argv[1:], settings)
    # collect all startup grabage and freeze current live objects so subsquent GC cycles will not visit them 
    # Short lived objects (event snapshots while trading) handled by CPython's reference counting eliminating GC pauses that show up in the tail latency
    gc.collect()
    gc.freeze()
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
