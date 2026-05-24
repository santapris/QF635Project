"""Plugin registries for gateways, strategies, and risk rules.

The generic builder dispatches to plugins via these registries; venue and
strategy code lives in its own package and registers itself at import time.
``trading.plugins.builtin`` imports every first-party plugin so registration
happens when this package is first touched.
"""

from .context import BuildContext
from .protocols import GatewayPlugin, RulePlugin, StrategyPlugin
from .registry import (
    gateway_registry,
    rule_registry,
    strategy_registry,
)

# Import builtin plugins to populate the registries.
from . import builtin  # noqa: F401

__all__ = [
    "BuildContext",
    "GatewayPlugin",
    "RulePlugin",
    "StrategyPlugin",
    "gateway_registry",
    "rule_registry",
    "strategy_registry",
]
