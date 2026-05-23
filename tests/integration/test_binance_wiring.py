"""Batch 4: Binance order_gateway schema + builder wiring tests.

Tests:
  4.1  BinanceOrderGatewaySpec parses from dict via AppConfig.
  4.2  SimOrderGatewaySpec still parses; existing 'simulation'/'backtest' types unaffected.
  4.3  OrderGatewaySpec union rejects unknown type.
  4.4  OrderGatewaySpec union rejects extra fields on BinanceOrderGatewaySpec.
  4.5  build_live_app wires BinanceOrderGateway when credentials are present.
  4.6  build_live_app raises ConfigError when no instruments match the venue.
  4.7  build_live_app with Binance spec raises RuntimeError when env creds missing.
  4.8  LiveApp.extra_services is empty for simulation-only config.
  4.9  LiveApp.order_gateways is typed AbstractOrderGateway (no longer SimulationOrderGateway only).
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pytest

from trading.config import (
    AppConfig,
    BinanceOrderGatewaySpec,
    OrderGatewaySpec,
    SimOrderGatewaySpec,
    build_live_app,
    load_config_from_dict,
)
from trading.core.exceptions import ConfigError
from trading.order_gateways.base import AbstractOrderGateway


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
# 4.1  BinanceOrderGatewaySpec parses
# ---------------------------------------------------------------------------

def test_binance_order_gateway_spec_parses() -> None:
    raw = {**_base_dict(), "order_gateways": [{"type": "binance", "testnet": True}]}
    cfg = load_config_from_dict(raw)
    assert len(cfg.order_gateways) == 1
    spec = cfg.order_gateways[0]
    assert isinstance(spec, BinanceOrderGatewaySpec)
    assert spec.testnet is True
    assert spec.venue == "BINANCE"


def test_binance_order_gateway_spec_defaults() -> None:
    spec = BinanceOrderGatewaySpec(type="binance")
    assert spec.testnet is True
    assert spec.reconcile_interval_seconds == 60.0
    assert spec.mismatch_threshold == "0.0001"


# ---------------------------------------------------------------------------
# 4.2  SimOrderGatewaySpec still parses for simulation and backtest types
# ---------------------------------------------------------------------------

def test_sim_order_gateway_spec_simulation_parses() -> None:
    raw = {**_base_dict(), "order_gateways": [{"venue": "SIM", "type": "simulation"}]}
    cfg = load_config_from_dict(raw)
    assert isinstance(cfg.order_gateways[0], SimOrderGatewaySpec)


def test_sim_order_gateway_spec_backtest_parses() -> None:
    raw = {**_base_dict(), "order_gateways": [{"venue": "SIM", "type": "backtest"}]}
    cfg = load_config_from_dict(raw)
    assert isinstance(cfg.order_gateways[0], SimOrderGatewaySpec)


# ---------------------------------------------------------------------------
# 4.3  Unknown order_gateway type rejected
# ---------------------------------------------------------------------------

def test_unknown_order_gateway_type_rejected() -> None:
    raw = {**_base_dict(), "order_gateways": [{"venue": "X", "type": "coinbase"}]}
    with pytest.raises((ConfigError, Exception)):
        load_config_from_dict(raw)


# ---------------------------------------------------------------------------
# 4.4  Extra fields rejected on BinanceOrderGatewaySpec
# ---------------------------------------------------------------------------

def test_binance_spec_rejects_extra_fields() -> None:
    raw = {**_base_dict(), "order_gateways": [{"type": "binance", "maker_bps": 1.0}]}
    with pytest.raises((ConfigError, Exception)):
        load_config_from_dict(raw)


# ---------------------------------------------------------------------------
# 4.5  build_live_app wires BinanceOrderGateway when creds present
# ---------------------------------------------------------------------------

def test_build_live_app_wires_binance_order_gateway(monkeypatch) -> None:
    """With creds in env, build_live_app constructs a BinanceOrderGateway."""
    monkeypatch.setenv("BINANCE_API_KEY", "test-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "test-secret")

    raw = {**_base_dict(), "order_gateways": [{"type": "binance", "testnet": True}]}
    cfg = load_config_from_dict(raw)
    app = build_live_app(cfg)

    assert len(app.order_gateways) == 1
    from trading.order_gateways.binance.order_gateway import BinanceOrderGateway
    assert isinstance(app.order_gateways[0], BinanceOrderGateway)

    # Three extra services: ListenKeyManager, BinanceUserDataStream, BalanceReconciler
    assert len(app.extra_services) == 3

    from trading.order_gateways.binance.listen_key import ListenKeyManager
    from trading.order_gateways.binance.user_data import BinanceUserDataStream
    from trading.order_gateways.binance.reconciler import BalanceReconciler
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
        "order_gateways": [{"type": "binance", "venue": "BINANCE", "testnet": True}],
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

    raw = {**_base_dict(), "order_gateways": [{"type": "binance", "testnet": True}]}
    cfg = load_config_from_dict(raw)
    with pytest.raises(ConfigError, match="missing Binance API credentials"):
        build_live_app(cfg)


# ---------------------------------------------------------------------------
# 4.8  Simulation-only config: extra_services is empty
# ---------------------------------------------------------------------------

def test_build_live_app_sim_no_extra_services() -> None:
    raw = {
        **_base_dict(),
        "order_gateways": [{"venue": "BINANCE", "type": "simulation"}],
    }
    cfg = load_config_from_dict(raw)
    app = build_live_app(cfg)
    assert app.extra_services == []


# ---------------------------------------------------------------------------
# 4.9  order_gateways list holds AbstractOrderGateway instances
# ---------------------------------------------------------------------------

def test_live_app_order_gateways_are_abstract_order_gateway() -> None:
    raw = {
        **_base_dict(),
        "order_gateways": [{"venue": "BINANCE", "type": "simulation"}],
    }
    cfg = load_config_from_dict(raw)
    app = build_live_app(cfg)
    assert all(isinstance(gw, AbstractOrderGateway) for gw in app.order_gateways)
