"""Batch 4: Binance gateway schema + builder wiring tests.

Tests:
  4.1  BinanceGatewaySpec parses from dict via AppConfig.
  4.2  SimGatewaySpec still parses; existing 'simulation'/'backtest' types unaffected.
  4.3  GatewaySpec union rejects unknown type.
  4.4  GatewaySpec union rejects extra fields on BinanceGatewaySpec.
  4.5  build_live_app wires BinanceGateway when credentials are present.
  4.6  build_live_app raises ConfigError when no instruments match the venue.
  4.7  build_live_app with Binance spec raises RuntimeError when env creds missing.
  4.8  LiveApp.extra_services is empty for simulation-only config.
  4.9  LiveApp.gateways is typed AbstractGateway (no longer SimulationGateway only).
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pytest

from trading.config import (
    AppConfig,
    BinanceGatewaySpec,
    GatewaySpec,
    SimGatewaySpec,
    build_live_app,
    load_config_from_dict,
)
from trading.core.exceptions import ConfigError
from trading.gateways.base import AbstractGateway


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_dict() -> dict:
    return {
        "instruments": [{
            "symbol": "BTC-USDT", "exchange": "BINANCE", "asset_type": "SPOT",
            "base_currency": "BTC", "quote_currency": "USDT",
            "tick_size": "0.01", "lot_size": "0.00001",
        }],
        "bus": {"backend": "memory"},
    }


# ---------------------------------------------------------------------------
# 4.1  BinanceGatewaySpec parses
# ---------------------------------------------------------------------------

def test_binance_gateway_spec_parses() -> None:
    raw = {**_base_dict(), "gateways": [{"type": "binance", "testnet": True}]}
    cfg = load_config_from_dict(raw)
    assert len(cfg.gateways) == 1
    spec = cfg.gateways[0]
    assert isinstance(spec, BinanceGatewaySpec)
    assert spec.testnet is True
    assert spec.venue == "BINANCE"


def test_binance_gateway_spec_defaults() -> None:
    spec = BinanceGatewaySpec(type="binance")
    assert spec.testnet is True
    assert spec.reconcile_interval_seconds == 60.0
    assert spec.mismatch_threshold == "0.0001"


# ---------------------------------------------------------------------------
# 4.2  SimGatewaySpec still parses for simulation and backtest types
# ---------------------------------------------------------------------------

def test_sim_gateway_spec_simulation_parses() -> None:
    raw = {**_base_dict(), "gateways": [{"venue": "SIM", "type": "simulation"}]}
    cfg = load_config_from_dict(raw)
    assert isinstance(cfg.gateways[0], SimGatewaySpec)


def test_sim_gateway_spec_backtest_parses() -> None:
    raw = {**_base_dict(), "gateways": [{"venue": "SIM", "type": "backtest"}]}
    cfg = load_config_from_dict(raw)
    assert isinstance(cfg.gateways[0], SimGatewaySpec)


# ---------------------------------------------------------------------------
# 4.3  Unknown gateway type rejected
# ---------------------------------------------------------------------------

def test_unknown_gateway_type_rejected() -> None:
    raw = {**_base_dict(), "gateways": [{"venue": "X", "type": "coinbase"}]}
    with pytest.raises((ConfigError, Exception)):
        load_config_from_dict(raw)


# ---------------------------------------------------------------------------
# 4.4  Extra fields rejected on BinanceGatewaySpec
# ---------------------------------------------------------------------------

def test_binance_spec_rejects_extra_fields() -> None:
    raw = {**_base_dict(), "gateways": [{"type": "binance", "maker_bps": 1.0}]}
    with pytest.raises((ConfigError, Exception)):
        load_config_from_dict(raw)


# ---------------------------------------------------------------------------
# 4.5  build_live_app wires BinanceGateway when creds present
# ---------------------------------------------------------------------------

def test_build_live_app_wires_binance_gateway(monkeypatch) -> None:
    """With creds in env, build_live_app constructs a BinanceGateway."""
    monkeypatch.setenv("BINANCE_API_KEY", "test-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "test-secret")

    raw = {**_base_dict(), "gateways": [{"type": "binance", "testnet": True}]}
    cfg = load_config_from_dict(raw)
    app = build_live_app(cfg)

    assert len(app.gateways) == 1
    from trading.gateways.binance.gateway import BinanceGateway
    assert isinstance(app.gateways[0], BinanceGateway)

    # Three extra services: ListenKeyManager, BinanceUserDataStream, BalanceReconciler
    assert len(app.extra_services) == 3

    from trading.gateways.binance.listen_key import ListenKeyManager
    from trading.gateways.binance.user_data import BinanceUserDataStream
    from trading.gateways.binance.reconciler import BalanceReconciler
    assert isinstance(app.extra_services[0], ListenKeyManager)
    assert isinstance(app.extra_services[1], BinanceUserDataStream)
    assert isinstance(app.extra_services[2], BalanceReconciler)


# ---------------------------------------------------------------------------
# 4.6  build_live_app raises when no instruments match the venue
# ---------------------------------------------------------------------------

def test_build_live_app_binance_no_instruments_raises(monkeypatch) -> None:
    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")

    raw = {
        "instruments": [{
            "symbol": "BTC-USDT", "exchange": "COINBASE", "asset_type": "SPOT",
            "base_currency": "BTC", "quote_currency": "USDT",
            "tick_size": "0.01", "lot_size": "0.00001",
        }],
        "bus": {"backend": "memory"},
        "gateways": [{"type": "binance", "venue": "BINANCE", "testnet": True}],
    }
    cfg = load_config_from_dict(raw)
    with pytest.raises(ConfigError):
        build_live_app(cfg)


# ---------------------------------------------------------------------------
# 4.7  Missing env creds raise ConfigError at build time
# ---------------------------------------------------------------------------

def test_build_live_app_binance_missing_creds_raises(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("BINANCE_API_KEY", "")
    monkeypatch.setenv("BINANCE_API_SECRET", "")

    raw = {**_base_dict(), "gateways": [{"type": "binance", "testnet": True}]}
    cfg = load_config_from_dict(raw)
    with pytest.raises(ConfigError, match="missing Binance API credentials"):
        build_live_app(cfg)


# ---------------------------------------------------------------------------
# 4.8  Simulation-only config: extra_services is empty
# ---------------------------------------------------------------------------

def test_build_live_app_sim_no_extra_services() -> None:
    raw = {
        **_base_dict(),
        "gateways": [{"venue": "BINANCE", "type": "simulation"}],
    }
    cfg = load_config_from_dict(raw)
    app = build_live_app(cfg)
    assert app.extra_services == []


# ---------------------------------------------------------------------------
# 4.9  gateways list holds AbstractGateway instances
# ---------------------------------------------------------------------------

def test_live_app_gateways_are_abstract_gateway() -> None:
    raw = {
        **_base_dict(),
        "gateways": [{"venue": "BINANCE", "type": "simulation"}],
    }
    cfg = load_config_from_dict(raw)
    app = build_live_app(cfg)
    assert all(isinstance(gw, AbstractGateway) for gw in app.gateways)
