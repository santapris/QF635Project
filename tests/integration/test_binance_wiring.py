"""Binance order_gateway schema + builder wiring tests.

Tests cover the open ``GatewaySpec`` + plugin dispatch model:
  - Binance gateway parses via the open spec, dispatches to the Binance plugin.
  - Simulation / backtest gateway types still parse and dispatch.
  - Unknown ``type`` is rejected at build time.
  - Unknown params on a Binance gateway are rejected by the plugin's Params model.
  - build_live_app wires BinanceOrderGateway + its extra services.
  - build_live_app raises when no instruments match the venue.
  - build_live_app raises when credentials are missing.
  - Simulation-only configs produce no extra services.
"""

from __future__ import annotations

import pytest

from trading.config import (
    GatewaySpec,
    build_live_app,
    load_config_from_dict,
)
from trading.core.exceptions import ConfigError
from trading.order_gateways.base import AbstractOrderGateway


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
# Binance gateway parses via the open spec
# ---------------------------------------------------------------------------

def test_binance_order_gateway_spec_parses() -> None:
    raw = {
        **_base_dict(),
        "order_gateways": [{"type": "binance", "venue": "BINANCE", "testnet": True}],
    }
    cfg = load_config_from_dict(raw)
    assert len(cfg.order_gateways) == 1
    spec = cfg.order_gateways[0]
    assert isinstance(spec, GatewaySpec)
    assert spec.type == "binance"
    assert spec.venue == "BINANCE"
    # `testnet` was a flat TOML key — collected into params by the model validator.
    assert spec.params.get("testnet") is True


# ---------------------------------------------------------------------------
# Simulation / backtest gateway types parse and dispatch
# ---------------------------------------------------------------------------

def test_sim_order_gateway_spec_simulation_parses() -> None:
    raw = {**_base_dict(), "order_gateways": [{"venue": "SIM", "type": "simulation"}]}
    cfg = load_config_from_dict(raw)
    assert cfg.order_gateways[0].type == "simulation"


def test_sim_order_gateway_spec_backtest_parses() -> None:
    raw = {**_base_dict(), "order_gateways": [{"venue": "SIM", "type": "backtest"}]}
    cfg = load_config_from_dict(raw)
    assert cfg.order_gateways[0].type == "backtest"


# ---------------------------------------------------------------------------
# Unknown gateway type rejected at build time
# ---------------------------------------------------------------------------

def test_unknown_order_gateway_type_rejected() -> None:
    raw = {**_base_dict(), "order_gateways": [{"venue": "X", "type": "coinbase"}]}
    cfg = load_config_from_dict(raw)
    with pytest.raises(ConfigError, match="unknown gateway type 'coinbase'"):
        build_live_app(cfg)


# ---------------------------------------------------------------------------
# Unknown params rejected by the Binance plugin's Params model
# ---------------------------------------------------------------------------

def test_binance_spec_rejects_extra_params(monkeypatch) -> None:
    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    raw = {
        **_base_dict(),
        # `maker_bps` is a simulation field, not a Binance one — should fail.
        "order_gateways": [{"type": "binance", "venue": "BINANCE", "maker_bps": 1.0}],
    }
    cfg = load_config_from_dict(raw)
    with pytest.raises(ConfigError, match="invalid parameters for gateway"):
        build_live_app(cfg)


# ---------------------------------------------------------------------------
# build_live_app wires BinanceOrderGateway when creds present
# ---------------------------------------------------------------------------

def test_build_live_app_wires_binance_order_gateway(monkeypatch) -> None:
    monkeypatch.setenv("BINANCE_API_KEY", "test-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "test-secret")

    raw = {
        **_base_dict(),
        "order_gateways": [{"type": "binance", "venue": "BINANCE", "testnet": True}],
    }
    cfg = load_config_from_dict(raw)
    app = build_live_app(cfg)

    assert len(app.order_gateways) == 1
    from trading.order_gateways.binance.order_gateway import BinanceOrderGateway
    assert isinstance(app.order_gateways[0], BinanceOrderGateway)

    assert len(app.extra_services) == 3
    from trading.order_gateways.binance.listen_key import ListenKeyManager
    from trading.order_gateways.binance.user_data import BinanceUserDataStream
    from trading.order_gateways.binance.reconciler import BalanceReconciler
    assert isinstance(app.extra_services[0], ListenKeyManager)
    assert isinstance(app.extra_services[1], BinanceUserDataStream)
    assert isinstance(app.extra_services[2], BalanceReconciler)


# ---------------------------------------------------------------------------
# build_live_app raises when no instruments match the venue
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
# Missing env creds raise ConfigError at build time
# ---------------------------------------------------------------------------

def test_build_live_app_binance_missing_creds_raises(monkeypatch) -> None:
    monkeypatch.setenv("BINANCE_API_KEY", "")
    monkeypatch.setenv("BINANCE_API_SECRET", "")

    raw = {
        **_base_dict(),
        "order_gateways": [{"type": "binance", "venue": "BINANCE", "testnet": True}],
    }
    cfg = load_config_from_dict(raw)
    with pytest.raises(ConfigError, match="missing Binance API credentials"):
        build_live_app(cfg)


# ---------------------------------------------------------------------------
# Simulation-only config: extra_services is empty
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
# order_gateways list holds AbstractOrderGateway instances
# ---------------------------------------------------------------------------

def test_live_app_order_gateways_are_abstract_order_gateway() -> None:
    raw = {
        **_base_dict(),
        "order_gateways": [{"venue": "BINANCE", "type": "simulation"}],
    }
    cfg = load_config_from_dict(raw)
    app = build_live_app(cfg)
    assert all(isinstance(gw, AbstractOrderGateway) for gw in app.order_gateways)
