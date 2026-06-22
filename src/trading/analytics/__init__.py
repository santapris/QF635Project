"""Analytics library — pure microstructure primitives.

No I/O, no async, no bus dependency. Safe to import from strategy, risk,
or observability without circular dependencies.
"""

from .avellaneda_stoikov import ASQuotes, AvellanedaStoikov
from .classifiers import BVCClassifier, TickRuleClassifier
from .imbalance import OBI, OFI
from .microprice import Microprice
from .position_sizing import (
    ScalingMethod,
    SizingResult,
    atr_position_size,
    cap_to_limits,
    compute_atr,
    fixed_fractional_size,
    fixed_size,
    inventory_skew_adjustment,
    kelly_fraction,
    kelly_position_size,
    signal_scaled_size,
    volatility_target_size,
)
from .quote_filters import passes_min_notional, post_only_guard, round_to_lot, round_to_tick
from .realtime_risk import (
    HISTORICAL_SCENARIOS,
    MarketRiskEngine,
    MarketRiskMetrics,
    StressPosition,
    StressTester,
    StressTestResult,
)
from .service import AnalyticsService
from .volatility import EWMAVolatility, ParkinsonVolatility
from .vpin import VPIN

__all__ = [
    "ASQuotes",
    "AnalyticsService",
    "AvellanedaStoikov",
    "BVCClassifier",
    "EWMAVolatility",
    "HISTORICAL_SCENARIOS",
    "MarketRiskEngine",
    "MarketRiskMetrics",
    "Microprice",
    "OBI",
    "OFI",
    "ParkinsonVolatility",
    "ScalingMethod",
    "SizingResult",
    "StressPosition",
    "StressTester",
    "StressTestResult",
    "TickRuleClassifier",
    "VPIN",
    "atr_position_size",
    "cap_to_limits",
    "compute_atr",
    "fixed_fractional_size",
    "fixed_size",
    "inventory_skew_adjustment",
    "kelly_fraction",
    "kelly_position_size",
    "passes_min_notional",
    "post_only_guard",
    "round_to_lot",
    "round_to_tick",
    "signal_scaled_size",
    "volatility_target_size",
]
