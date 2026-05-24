"""Configuration: schema, loader, builder.

Importing this package loads the built-in plugins via ``trading.plugins``,
populating the gateway / strategy / rule registries the builder dispatches to.
"""

from .. import plugins  # noqa: F401 — triggers plugin registration
from .builder import BacktestApp, LiveApp, build_backtest_app, build_live_app
from .loader import load_config, load_config_from_dict
from .schema import (
    AppConfig,
    BacktestSpec,
    BusBackend,
    BusConfig,
    FeedHandlerSpec,
    GatewaySpec,
    OMSSpec,
    PositionSpec,
    RiskSpec,
    RuleSpec,
    StrategySpec,
)
from .settings import Settings, load_settings

__all__ = [
    "AppConfig",
    "BacktestApp",
    "BacktestSpec",
    "BusBackend",
    "BusConfig",
    "FeedHandlerSpec",
    "GatewaySpec",
    "LiveApp",
    "OMSSpec",
    "PositionSpec",
    "RiskSpec",
    "RuleSpec",
    "Settings",
    "StrategySpec",
    "build_backtest_app",
    "build_live_app",
    "load_config",
    "load_config_from_dict",
    "load_settings",
]
