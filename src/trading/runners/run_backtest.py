"""Backtest runner.

Usage:
    python -m trading.runners.run_backtest --config configs/backtest_example.toml

Loads config, builds the app, runs the engine, prints the report summary.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from trading.config import build_backtest_app, load_config


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run a backtest from a TOML config.")
    p.add_argument(
        "--config", "-c", type=Path, required=True,
        help="Path to TOML config file.",
    )
    p.add_argument(
        "--log-level", default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    p.add_argument(
        "--json", action="store_true",
        help="Emit a JSON dump of the report instead of the text summary.",
    )
    return p.parse_args(argv)


async def _amain(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = load_config(args.config)
    app = build_backtest_app(config)
    report = await app.run()

    if args.json:
        import json
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        print(report.summary())
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
