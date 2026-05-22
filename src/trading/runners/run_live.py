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
import logging
import signal
import sys
from pathlib import Path

from trading.config import build_live_app, load_config


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the live trading app.")
    p.add_argument("--config", "-c", type=Path, required=True)
    p.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args(argv)


async def _amain(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    _log = logging.getLogger("trading.runner")

    config = load_config(args.config)
    app = build_live_app(config)
    await app.start()
    _log.info("trading app started")

    # Wait for a shutdown signal.
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        _log.info("shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler; runners on
            # Windows can ctrl-C and we'll cope with KeyboardInterrupt.
            pass

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        _log.info("KeyboardInterrupt — shutting down")
    finally:
        await app.stop()
        _log.info("trading app stopped")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
