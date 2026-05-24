"""Integration tests for config loading and the application builders."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from trading.config import (
    AppConfig,
    build_backtest_app,
    build_live_app,
    load_config,
    load_config_from_dict,
)
from trading.core.exceptions import ConfigError


# A minimal-but-complete config dict that exercises every section.
def _config_dict(tmp_path: Path) -> dict:
    data_csv = tmp_path / "bars.csv"
    data_csv.write_text(textwrap.dedent("""\
        timestamp,open,high,low,close,volume
        1700000000000,100,101,99,100.5,1.0
        1700000060000,100.5,102,100,101.0,2.0
        """))

    return {
        "instruments": [{
            "symbol": "BTC-USDT", "exchange": "SIM", "asset_type": "SPOT",
            "base_currency": "BTC", "quote_currency": "USDT",
            "tick_size": "0.01", "lot_size": "0.0001",
        }],
        "bus": {"backend": "asyncio", "queue_size": 1000},
        "strategies": [{
            "strategy_id": "m1", "type": "momentum",
            "instruments": ["SIM:BTC-USDT"],
            "parameters": {"fast_period": "3", "slow_period": "7"},
        }],
        "risk": {
            "global_rules": [{
                "type": "instrument_allowlist",
                "params": {"allowed_instrument_ids": "SIM:BTC-USDT"},
            }],
            "per_strategy": {
                "m1": [
                    {"type": "max_position", "params": {"max_long": "1", "max_short": "1"}},
                ],
            },
        },
        "order_gateways": [{
            "venue": "SIM", "type": "backtest",
            "submit_ack_ms": 0.0, "cancel_ack_ms": 0.0, "fill_ms": 0.0,
            "seed": 1,
        }],
        "backtest": {
            "data_path": str(data_csv),
            "instrument_id": "SIM:BTC-USDT",
            "snapshot_interval_seconds": 10.0,
            "initial_equity": 100_000.0,
            "periods_per_year": 525600,
            "timestamp_unit": "ms",
        },
    }


def test_load_config_validates_schema(tmp_path) -> None:
    cfg = load_config_from_dict(_config_dict(tmp_path))
    assert isinstance(cfg, AppConfig)
    assert cfg.bus.backend.value == "asyncio"
    assert len(cfg.strategies) == 1


def test_load_config_rejects_unknown_field(tmp_path) -> None:
    bad = _config_dict(tmp_path)
    bad["bus"]["nonsense_key"] = True
    with pytest.raises(ConfigError):
        load_config_from_dict(bad)


def test_build_rejects_unknown_strategy_type(tmp_path) -> None:
    bad = _config_dict(tmp_path)
    bad["strategies"][0]["type"] = "definitely_not_a_strategy"
    bad["order_gateways"][0]["type"] = "simulation"  # live builder needs sim, not backtest
    cfg = load_config_from_dict(bad)
    with pytest.raises(ConfigError, match="unknown strategy type"):
        build_live_app(cfg)


def test_env_overrides_apply(tmp_path, monkeypatch) -> None:
    cfg_path = tmp_path / "cfg.toml"
    # Write a minimal TOML
    cfg_path.write_text(textwrap.dedent("""\
        [bus]
        backend = "memory"
        queue_size = 100
    """))
    monkeypatch.setenv("TRADING__BUS__QUEUE_SIZE", "9999")
    cfg = load_config(cfg_path)
    assert cfg.bus.queue_size == 9999


def test_build_live_app_constructs_components(tmp_path) -> None:
    raw = _config_dict(tmp_path)
    # The live builder needs at least one simulation order_gateway, not backtest.
    raw["order_gateways"][0]["type"] = "simulation"
    cfg = load_config_from_dict(raw)
    app = build_live_app(cfg)
    assert app.position_engine is not None
    assert app.risk_engine is not None
    assert app.oms_engine is not None
    assert len(app.order_gateways) == 1
    assert app.strategy_registry is not None


@pytest.mark.integration
async def test_build_backtest_app_runs_e2e(tmp_path) -> None:
    cfg = load_config_from_dict(_config_dict(tmp_path))
    app = build_backtest_app(cfg)
    report = await app.run()
    # Two bars -> two ticks; the momentum strategy is unlikely to fire on
    # such a tiny series, but the engine must complete and produce a report.
    assert report.metrics is not None
    assert len(report.equity_points) > 0
