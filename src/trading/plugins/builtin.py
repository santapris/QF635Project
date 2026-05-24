"""Import every first-party plugin so registration runs at startup.

Imports here are side-effect only — each module calls ``register()`` at
module scope. Add new built-in plugins to this list.
"""

from ..order_gateways import simulation_plugin  # noqa: F401
from ..order_gateways.binance import plugin as _binance_plugin  # noqa: F401
from ..risk.rules import plugins as _rule_plugins  # noqa: F401
from ..strategy.examples import plugins as _strategy_plugins  # noqa: F401
