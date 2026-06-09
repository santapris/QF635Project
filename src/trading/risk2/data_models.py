"""
data_models.py
==============
Shared data structures used across the entire risk management package.

Everything that needs to be passed BETWEEN modules lives here so there is a
single, easy-to-find "source of truth" for the shapes of our data.

If you are new to Python: a `@dataclass` is just a lightweight class that
automatically writes the boring boilerplate (the __init__ method, etc.) for
you. You create one like a normal object:

    order = TradeOrder(symbol="AAPL", side="BUY", quantity=100, price=185.0,
                       asset_class="equity")
    print(order.notional)   # -> 18500.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional


# ════════════════════════════════════════════════════════════════════
# Enumerations  (fixed sets of allowed values)
# ════════════════════════════════════════════════════════════════════

class Side(str, Enum):
    """Which direction a trade goes."""
    BUY = "BUY"
    SELL = "SELL"


class AssetClass(str, Enum):
    """Broad category of instrument. Used for concentration limits."""
    EQUITY = "equity"
    FUTURES = "futures"
    FX = "fx"
    CRYPTO = "crypto"
    OPTION = "option"
    BOND = "bond"
    OTHER = "other"


class RiskLevel(str, Enum):
    """
    Hierarchical risk architecture levels (from the lecture notes).
    Limits can be attached at any level of the trading stack.
    """
    FIRM = "firm"               # protect the whole business
    PORTFOLIO = "portfolio"     # overall portfolio risk
    STRATEGY = "strategy"       # a single model / algo
    INSTRUMENT = "instrument"   # a single symbol / sector
    EXECUTION = "execution"     # individual orders
    GATEWAY = "gateway"         # exchange / infrastructure


class LimitType(str, Enum):
    """Soft limits warn; hard limits block."""
    SOFT = "soft"   # early-warning: alert, reduce aggressiveness
    HARD = "hard"   # absolute maximum: reject orders / disable trading


# ════════════════════════════════════════════════════════════════════
# Orders and Positions
# ════════════════════════════════════════════════════════════════════

@dataclass
class TradeOrder:
    """
    A single order the strategy wants to send to the market.

    Fields
    ------
    symbol       : ticker, e.g. "AAPL" or "BTCUSDT"
    side         : "BUY" or "SELL"
    quantity     : number of shares / contracts / coins (always positive)
    price        : the limit price (or expected fill price for market orders)
    asset_class  : one of the AssetClass values ("equity", "crypto", ...)
    strategy_id  : which strategy generated this order (for strategy-level limits)
    order_id     : your own unique id for the order (optional)
    leverage     : leverage applied to this order (1.0 = no leverage / cash)
    """
    symbol: str
    side: str
    quantity: float
    price: float
    asset_class: str = "equity"
    strategy_id: str = "default"
    order_id: str = ""
    leverage: float = 1.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def notional(self) -> float:
        """Cash value of the order = quantity x price (always positive)."""
        return abs(self.quantity * self.price)

    @property
    def signed_quantity(self) -> float:
        """+quantity for a BUY, -quantity for a SELL."""
        return self.quantity if self.side.upper() == "BUY" else -self.quantity

    @property
    def margin_required(self) -> float:
        """How much of our own cash this order ties up, given its leverage."""
        return self.notional / max(self.leverage, 1.0)


@dataclass
class Position:
    """
    A position we currently hold in one symbol.

    quantity is SIGNED:  positive = long,  negative = short.
    """
    symbol: str
    quantity: float
    avg_price: float
    asset_class: str = "equity"
    current_price: float = 0.0
    leverage: float = 1.0
    # Optional: exchange maintenance-margin rate for this symbol (e.g. 0.005 = 0.5%)
    maintenance_margin_rate: float = 0.005

    @property
    def mark_price(self) -> float:
        """Use the live price if we have one, otherwise fall back to entry."""
        return self.current_price if self.current_price else self.avg_price

    @property
    def notional(self) -> float:
        """Current market value of the position (always positive)."""
        return abs(self.quantity * self.mark_price)

    @property
    def signed_notional(self) -> float:
        """Positive for long, negative for short."""
        return self.quantity * self.mark_price

    @property
    def is_long(self) -> bool:
        return self.quantity > 0

    @property
    def is_short(self) -> bool:
        return self.quantity < 0

    @property
    def unrealised_pnl(self) -> float:
        """
        Mark-to-market profit/loss on the open position.
        Long  makes money when price rises; short makes money when price falls.
        """
        return self.quantity * (self.mark_price - self.avg_price)

    @property
    def margin_used(self) -> float:
        """Own capital tied up in this position, given its leverage."""
        return self.notional / max(self.leverage, 1.0)

    @property
    def maintenance_margin(self) -> float:
        """Minimum equity the venue requires to keep this position open."""
        return self.notional * self.maintenance_margin_rate


# ════════════════════════════════════════════════════════════════════
# Risk Limits  (the user-configurable rule book)
# ════════════════════════════════════════════════════════════════════

@dataclass
class RiskLimits:
    """
    All of the pre-trade risk limits, in one place.

    Every monetary value is in your base currency (USD in the examples).
    Percentages are decimals: 0.10 means 10%.

    SOFT vs HARD
    ------------
    For the most important limits we keep BOTH a soft and a hard threshold,
    exactly as described in the lecture notes:
      * soft limit  -> warn, but still allow the order
      * hard limit  -> block the order
    e.g. soft position notional = $160k, hard = $200k.
    """
    # ---- Portfolio / firm level --------------------------------------
    total_portfolio_value: float = 1_000_000.0

    # Gross = long + short notional (leverage).  Net = long - short (direction).
    max_gross_notional_soft: float = 1_600_000.0
    max_gross_notional_hard: float = 2_000_000.0
    max_net_notional_soft: float = 800_000.0
    max_net_notional_hard: float = 1_000_000.0

    # Leverage cap (gross notional / equity)
    max_gross_leverage: float = 4.0

    # ---- Per-trade (execution) limits --------------------------------
    max_single_trade_notional_soft: float = 80_000.0
    max_single_trade_notional_hard: float = 100_000.0
    max_single_trade_pct: float = 0.10          # 10% of portfolio per trade

    # Fat-finger price collar: reject orders whose price is too far from market
    fat_finger_price_collar_pct: float = 0.03   # +/- 3% from reference price

    # ---- Per-symbol (instrument) limits ------------------------------
    max_position_notional_soft: float = 160_000.0
    max_position_notional_hard: float = 200_000.0
    max_position_pct: float = 0.20              # 20% of portfolio per symbol

    # ---- Asset-class concentration limits (fraction of portfolio) ----
    concentration_limits: Dict[str, float] = field(default_factory=lambda: {
        "equity": 0.60,
        "futures": 0.40,
        "fx": 0.30,
        "crypto": 0.15,
        "option": 0.20,
        "bond": 0.50,
        "other": 0.30,
    })

    # ---- Correlation limits ------------------------------------------
    max_correlation_threshold: float = 0.80     # flag pairs above this |corr|
    max_correlated_positions: int = 3           # how many high-corr pairs allowed

    # ---- Margin / leverage risk --------------------------------------
    # If projected account margin-ratio goes above these, warn / block.
    margin_ratio_warning: float = 0.50          # 50% of equity committed -> warn
    margin_ratio_hard: float = 0.80             # 80% -> block new risk-adding orders

    # ---- Compliance --------------------------------------------------
    restricted_symbols: List[str] = field(default_factory=list)
    short_sell_allowed: bool = True
    leveraged_products_allowed: bool = True
    max_order_quantity: float = 1_000_000.0     # sanity cap on raw quantity

    # ---- Strategy-level limits (keyed by strategy_id) ----------------
    # e.g. {"momentum": 300_000} caps total notional that strategy may run
    strategy_notional_limits: Dict[str, float] = field(default_factory=dict)

    def concentration_limit_for(self, asset_class: str) -> float:
        """Return the concentration cap for an asset class (default 0.50)."""
        return self.concentration_limits.get(asset_class, 0.50)


# ════════════════════════════════════════════════════════════════════
# Result objects
# ════════════════════════════════════════════════════════════════════

@dataclass
class CheckResult:
    """Outcome of a single named check."""
    name: str
    passed: bool
    severity: str = "hard"          # "soft" (warning) or "hard" (blocking)
    message: str = ""

    @property
    def is_blocking(self) -> bool:
        return (not self.passed) and self.severity == "hard"

    @property
    def is_warning(self) -> bool:
        return (not self.passed) and self.severity == "soft"


@dataclass
class PreTradeResult:
    """
    Aggregated outcome of ALL pre-trade checks for one order.

    `passed` is True only if no HARD check failed. Soft-limit breaches show up
    as warnings but do not set `passed` to False.
    """
    order: TradeOrder
    results: List[CheckResult] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def passed(self) -> bool:
        return not any(r.is_blocking for r in self.results)

    @property
    def violations(self) -> List[str]:
        return [f"[{r.name}] {r.message}" for r in self.results if r.is_blocking]

    @property
    def warnings(self) -> List[str]:
        return [f"[{r.name}] {r.message}" for r in self.results if r.is_warning]

    def summary(self) -> str:
        status = "APPROVED" if self.passed else "REJECTED"
        lines = [f"[{status}] {self.order.symbol} {self.order.side} "
                 f"{self.order.quantity}@{self.order.price} "
                 f"(notional ${self.order.notional:,.0f})"]
        for v in self.violations:
            lines.append(f"   x VIOLATION: {v}")
        for w in self.warnings:
            lines.append(f"   ! WARNING:   {w}")
        return "\n".join(lines)
