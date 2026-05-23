"""Live / paper-trading runner.

Usage:
    python -m trading.runners.run_live --config configs/paper_example.toml

Starts the wired-up app, then waits for SIGINT/SIGTERM. On shutdown signal,
stops cleanly and exits.

Note: with the current gateway set this runs as paper trading against the
:class:`SimulationGateway`. Wiring in a real exchange gateway is a swap
of one config section once an adapter is implemented.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import structlog
import sys
from pathlib import Path

from trading.config import build_live_app, load_config
from trading.health import HealthServer
from trading.logging import configure_logging


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the live trading app.")
    p.add_argument("--config", "-c", type=Path, required=True)
    p.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    p.add_argument(
        "--health-port", type=int, default=9090,
        help="Port for /healthz and /metrics HTTP server (0 = disabled).",
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
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
