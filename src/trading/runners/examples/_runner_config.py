"""Shared config loading for the stage1-4 example runners.

The runners are demo scripts; their venue URLs and the BTC-USDT instrument
come from configs/binance_testnet.toml so nothing is hardcoded in code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from trading.config import load_config
from trading.core import Instrument
from trading.core.exceptions import ConfigError
from trading.order_gateways.binance import BinanceConfig, BinanceCredentials
from trading.order_gateways.binance.plugin import BinanceParams


_DEFAULT_CONFIG = Path(__file__).resolve().parents[4] / "configs" / "binance_testnet.toml"


@dataclass(frozen=True, slots=True)
class RunnerConfig:
    binance: BinanceConfig
    credentials: BinanceCredentials | None
    instruments: list[Instrument]


def load_runner_config(*, require_credentials: bool, futures: bool) -> RunnerConfig:
    cfg = load_config(_DEFAULT_CONFIG)

    spec = next(
        (g for g in cfg.order_gateways if g.type == "binance"),
        None,
    )
    if spec is None:
        raise ConfigError(
            f"no Binance order gateway in {_DEFAULT_CONFIG}",
            path=str(_DEFAULT_CONFIG),
        )

    try:
        params = BinanceParams.model_validate(spec.params)
    except Exception as e:
        raise ConfigError(
            f"invalid Binance gateway params in {_DEFAULT_CONFIG}: {e}",
            path=str(_DEFAULT_CONFIG),
        ) from e

    urls = params.resolved_urls()
    binance = BinanceConfig(
        spot_rest_base=urls["spot_rest_base"],
        spot_ws_base=urls["spot_ws_base"],
        futures_rest_base=urls["futures_rest_base"],
        futures_ws_base=urls["futures_ws_base"],
        futures=futures,
    )

    credentials: BinanceCredentials | None = None
    if require_credentials:
        key = os.environ.get(f"{params.credentials_env}_API_KEY")
        secret = os.environ.get(f"{params.credentials_env}_API_SECRET")
        if not key or not secret:
            raise ConfigError(
                f"missing Binance credentials; set {params.credentials_env}_API_KEY "
                f"and {params.credentials_env}_API_SECRET in the environment"
            )
        credentials = BinanceCredentials(api_key=key, api_secret=secret)

    instruments = [s.to_instrument() for s in cfg.instruments]
    return RunnerConfig(binance=binance, credentials=credentials, instruments=instruments)


__all__ = ["RunnerConfig", "load_runner_config"]
