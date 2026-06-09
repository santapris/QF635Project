"""
pre_trade_checks.py
==================
Layer 1 -- Pre-Trade Risk Checks (runs BEFORE every order is sent)

This is the first gate every order passes through. It validates an order
against the rule book in `RiskLimits` and returns a detailed pass/fail report.

What it checks (each is a small method you can read on its own):
  * restricted symbols / compliance watchlist
  * short-selling permission
  * raw quantity sanity cap
  * fat-finger price collar (price too far from market)
  * single-trade size           (SOFT + HARD)
  * per-symbol position limit    (SOFT + HARD)
  * gross notional               (SOFT + HARD)
  * net (directional) notional   (SOFT + HARD)
  * gross leverage cap
  * asset-class concentration
  * strategy-level notional limit
  * correlation concentration
  * projected margin ratio

SOFT vs HARD
------------
A SOFT breach produces a WARNING but still lets the order through.
A HARD breach BLOCKS the order. (Straight from the lecture notes.)
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from data_models import (
    TradeOrder, Position, RiskLimits, CheckResult, PreTradeResult,
)

logger = logging.getLogger(__name__)


class PreTradeRiskEngine:
    """Runs all pre-trade checks for a single order."""

    def __init__(self, limits: RiskLimits):
        self.limits = limits
        self.positions: Dict[str, Position] = {}
        self.returns_history: pd.DataFrame = pd.DataFrame()
        self.account_equity: float = limits.total_portfolio_value
        # remember recent orders to catch accidental duplicates
        self._recent_orders: List[str] = []

    # ── portfolio state ────────────────────────────────────────────

    def update_position(self, position: Position) -> None:
        self.positions[position.symbol] = position

    def load_positions(self, positions: List[Position]) -> None:
        for p in positions:
            self.positions[p.symbol] = p

    def set_returns_history(self, returns_df: pd.DataFrame) -> None:
        """DataFrame of daily returns: columns = symbols, rows = dates."""
        self.returns_history = returns_df

    def set_account_equity(self, equity: float) -> None:
        self.account_equity = equity

    # ── derived portfolio metrics ──────────────────────────────────

    def gross_notional(self) -> float:
        return sum(p.notional for p in self.positions.values())

    def net_notional(self) -> float:
        return sum(p.signed_notional for p in self.positions.values())

    def margin_used(self) -> float:
        return sum(p.margin_used for p in self.positions.values())

    def asset_class_notional(self) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for p in self.positions.values():
            out[p.asset_class] = out.get(p.asset_class, 0.0) + p.notional
        return out

    def strategy_notional(self) -> Dict[str, float]:
        # positions don't carry strategy_id, so this is computed from orders
        # in real systems; here we expose the helper for completeness.
        return {}

    def symbol_notional(self, symbol: str) -> float:
        pos = self.positions.get(symbol)
        return pos.notional if pos else 0.0

    # ── individual checks ──────────────────────────────────────────
    # Each returns a CheckResult. severity decides soft (warn) vs hard (block).

    def _ok(self, name: str) -> CheckResult:
        return CheckResult(name=name, passed=True)

    def check_restricted_symbols(self, order: TradeOrder) -> CheckResult:
        watch = [s.upper() for s in self.limits.restricted_symbols]
        if order.symbol.upper() in watch:
            return CheckResult("restricted_symbols", False, "hard",
                               f"{order.symbol} is on the restricted/compliance watchlist")
        return self._ok("restricted_symbols")

    def check_short_sell(self, order: TradeOrder) -> CheckResult:
        if order.side.upper() == "SELL" and not self.limits.short_sell_allowed:
            pos = self.positions.get(order.symbol)
            current_qty = pos.quantity if pos else 0.0
            if current_qty < order.quantity:
                return CheckResult("short_sell", False, "hard",
                                   f"Short selling not permitted and {order.symbol} "
                                   f"inventory ({current_qty:g}) is insufficient")
        return self._ok("short_sell")

    def check_quantity_sanity(self, order: TradeOrder) -> CheckResult:
        if order.quantity <= 0:
            return CheckResult("quantity_sanity", False, "hard",
                               "Order quantity must be positive")
        if order.quantity > self.limits.max_order_quantity:
            return CheckResult("quantity_sanity", False, "hard",
                               f"Quantity {order.quantity:g} exceeds sanity cap "
                               f"{self.limits.max_order_quantity:g}")
        return self._ok("quantity_sanity")

    def check_fat_finger(self, order: TradeOrder,
                         reference_price: Optional[float]) -> CheckResult:
        """Reject orders whose price is too far from the current market price."""
        if reference_price is None or reference_price <= 0:
            return self._ok("fat_finger")  # no reference -> can't check
        collar = self.limits.fat_finger_price_collar_pct
        deviation = abs(order.price - reference_price) / reference_price
        if deviation > collar:
            return CheckResult("fat_finger", False, "hard",
                               f"Price {order.price:g} is {deviation:.1%} from market "
                               f"{reference_price:g}; collar is +/-{collar:.1%}")
        return self._ok("fat_finger")

    def check_duplicate(self, order: TradeOrder) -> CheckResult:
        """Catch the same order being submitted twice in quick succession."""
        sig = f"{order.symbol}|{order.side}|{order.quantity}|{order.price}"
        if self._recent_orders.count(sig) >= 3:
            return CheckResult("duplicate_order", False, "hard",
                               "More than 3 identical orders detected; suppressing duplicate")
        return self._ok("duplicate_order")

    def check_single_trade_size(self, order: TradeOrder) -> CheckResult:
        n = order.notional
        if n > self.limits.max_single_trade_notional_hard:
            return CheckResult("single_trade_size", False, "hard",
                               f"Trade notional ${n:,.0f} exceeds HARD single-trade "
                               f"limit ${self.limits.max_single_trade_notional_hard:,.0f}")
        pct = n / self.limits.total_portfolio_value
        if pct > self.limits.max_single_trade_pct:
            return CheckResult("single_trade_size", False, "hard",
                               f"Trade is {pct:.1%} of portfolio; HARD limit is "
                               f"{self.limits.max_single_trade_pct:.1%}")
        if n > self.limits.max_single_trade_notional_soft:
            return CheckResult("single_trade_size", False, "soft",
                               f"Trade notional ${n:,.0f} exceeds SOFT single-trade "
                               f"limit ${self.limits.max_single_trade_notional_soft:,.0f}")
        return self._ok("single_trade_size")

    def check_position_limit(self, order: TradeOrder) -> CheckResult:
        existing = self.symbol_notional(order.symbol)
        projected = existing + order.notional
        if projected > self.limits.max_position_notional_hard:
            return CheckResult("position_limit", False, "hard",
                               f"Projected {order.symbol} notional ${projected:,.0f} "
                               f"exceeds HARD limit ${self.limits.max_position_notional_hard:,.0f}")
        pct = projected / self.limits.total_portfolio_value
        if pct > self.limits.max_position_pct:
            return CheckResult("position_limit", False, "hard",
                               f"Projected {order.symbol} is {pct:.1%} of portfolio; "
                               f"HARD limit is {self.limits.max_position_pct:.1%}")
        if projected > self.limits.max_position_notional_soft:
            return CheckResult("position_limit", False, "soft",
                               f"Projected {order.symbol} notional ${projected:,.0f} "
                               f"exceeds SOFT limit ${self.limits.max_position_notional_soft:,.0f}")
        return self._ok("position_limit")

    def check_gross_notional(self, order: TradeOrder) -> CheckResult:
        projected = self.gross_notional() + order.notional
        if projected > self.limits.max_gross_notional_hard:
            return CheckResult("gross_notional", False, "hard",
                               f"Projected gross notional ${projected:,.0f} exceeds HARD "
                               f"limit ${self.limits.max_gross_notional_hard:,.0f}")
        if projected > self.limits.max_gross_notional_soft:
            return CheckResult("gross_notional", False, "soft",
                               f"Projected gross notional ${projected:,.0f} exceeds SOFT "
                               f"limit ${self.limits.max_gross_notional_soft:,.0f}")
        return self._ok("gross_notional")

    def check_net_notional(self, order: TradeOrder) -> CheckResult:
        projected = self.net_notional() + order.signed_quantity * order.price
        a = abs(projected)
        if a > self.limits.max_net_notional_hard:
            return CheckResult("net_notional", False, "hard",
                               f"Projected net notional ${projected:,.0f} exceeds HARD "
                               f"limit ${self.limits.max_net_notional_hard:,.0f}")
        if a > self.limits.max_net_notional_soft:
            return CheckResult("net_notional", False, "soft",
                               f"Projected net notional ${projected:,.0f} exceeds SOFT "
                               f"limit ${self.limits.max_net_notional_soft:,.0f}")
        return self._ok("net_notional")

    def check_gross_leverage(self, order: TradeOrder) -> CheckResult:
        projected_gross = self.gross_notional() + order.notional
        equity = max(self.account_equity, 1.0)
        leverage = projected_gross / equity
        if leverage > self.limits.max_gross_leverage:
            return CheckResult("gross_leverage", False, "hard",
                               f"Projected gross leverage {leverage:.2f}x exceeds "
                               f"limit {self.limits.max_gross_leverage:.2f}x")
        return self._ok("gross_leverage")

    def check_concentration(self, order: TradeOrder) -> CheckResult:
        ac = order.asset_class
        limit = self.limits.concentration_limit_for(ac)
        current = self.asset_class_notional().get(ac, 0.0)
        projected = (current + order.notional) / self.limits.total_portfolio_value
        if projected > limit:
            return CheckResult("concentration", False, "hard",
                               f"Asset class '{ac}' concentration would be {projected:.1%}; "
                               f"limit is {limit:.1%}")
        if projected > limit * 0.85:
            return CheckResult("concentration", False, "soft",
                               f"Asset class '{ac}' concentration approaching limit: "
                               f"{projected:.1%} of {limit:.1%}")
        return self._ok("concentration")

    def check_strategy_limit(self, order: TradeOrder) -> CheckResult:
        cap = self.limits.strategy_notional_limits.get(order.strategy_id)
        if cap is None:
            return self._ok("strategy_limit")
        # we only know the incoming order's notional here; in a full system you
        # would track per-strategy running notional. We check the order itself.
        if order.notional > cap:
            return CheckResult("strategy_limit", False, "hard",
                               f"Strategy '{order.strategy_id}' order ${order.notional:,.0f} "
                               f"exceeds strategy cap ${cap:,.0f}")
        return self._ok("strategy_limit")

    def check_correlation(self, order: TradeOrder) -> CheckResult:
        if (self.returns_history.empty
                or order.symbol not in self.returns_history.columns):
            return self._ok("correlation")
        new_sym = order.symbol
        held = [s for s in self.positions if s != new_sym
                and s in self.returns_history.columns]
        if not held:
            return self._ok("correlation")

        high = []
        for sym in held:
            corr = self.returns_history[new_sym].corr(self.returns_history[sym])
            if pd.notna(corr) and abs(corr) >= self.limits.max_correlation_threshold:
                high.append((sym, round(float(corr), 2)))

        if len(high) > self.limits.max_correlated_positions:
            pairs = ", ".join(f"{s}({c})" for s, c in high)
            return CheckResult("correlation", False, "hard",
                               f"{new_sym} highly correlated with {len(high)} positions: {pairs}")
        if high:
            pairs = ", ".join(f"{s}({c})" for s, c in high)
            return CheckResult("correlation", False, "soft",
                               f"{new_sym} correlated with {pairs}")
        return self._ok("correlation")

    def check_margin_ratio(self, order: TradeOrder) -> CheckResult:
        """
        Projected margin ratio = margin committed / equity.
        Approaching 1.0 means approaching liquidation risk.
        """
        equity = max(self.account_equity, 1.0)
        projected_margin = self.margin_used() + order.margin_required
        ratio = projected_margin / equity
        if ratio > self.limits.margin_ratio_hard:
            return CheckResult("margin_ratio", False, "hard",
                               f"Projected margin ratio {ratio:.1%} exceeds HARD limit "
                               f"{self.limits.margin_ratio_hard:.1%}")
        if ratio > self.limits.margin_ratio_warning:
            return CheckResult("margin_ratio", False, "soft",
                               f"Projected margin ratio {ratio:.1%} exceeds warning "
                               f"{self.limits.margin_ratio_warning:.1%}")
        return self._ok("margin_ratio")

    # ── main entry point ───────────────────────────────────────────

    def run(self, order: TradeOrder,
            reference_price: Optional[float] = None) -> PreTradeResult:
        """
        Run every check and return a PreTradeResult.

        `reference_price` is the current market/mid price, used by the
        fat-finger collar. If you don't have it, that check is skipped.
        """
        checks = [
            self.check_quantity_sanity(order),
            self.check_restricted_symbols(order),
            self.check_short_sell(order),
            self.check_fat_finger(order, reference_price),
            self.check_duplicate(order),
            self.check_single_trade_size(order),
            self.check_position_limit(order),
            self.check_gross_notional(order),
            self.check_net_notional(order),
            self.check_gross_leverage(order),
            self.check_concentration(order),
            self.check_strategy_limit(order),
            self.check_correlation(order),
            self.check_margin_ratio(order),
        ]
        result = PreTradeResult(order=order, results=checks)

        # remember this order signature (keep last 20)
        self._recent_orders.append(
            f"{order.symbol}|{order.side}|{order.quantity}|{order.price}")
        self._recent_orders = self._recent_orders[-20:]

        for r in checks:
            if r.is_blocking:
                logger.warning("PRE-TRADE BLOCK | %s | %s", r.name, r.message)
            elif r.is_warning:
                logger.info("PRE-TRADE WARN | %s | %s", r.name, r.message)

        logger.info("PRE-TRADE %s | %s %s qty=%g notional=$%.0f",
                    "PASS" if result.passed else "FAIL",
                    order.symbol, order.side, order.quantity, order.notional)
        return result


# ════════════════════════════════════════════════════════════════════
# Smoke test
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    limits = RiskLimits(total_portfolio_value=1_000_000,
                        restricted_symbols=["XYZ"])
    engine = PreTradeRiskEngine(limits)
    engine.load_positions([
        Position("AAPL", 500, 180.0, "equity", current_price=182.0),
        Position("MSFT", 300, 320.0, "equity", current_price=325.0),
    ])

    for order, ref in [
        (TradeOrder("AAPL", "BUY", 100, 182.0, "equity"), 182.0),
        (TradeOrder("XYZ", "BUY", 200, 50.0, "equity"), 50.0),
        (TradeOrder("TSLA", "BUY", 5000, 250.0, "equity"), 250.0),
        (TradeOrder("AAPL", "BUY", 100, 250.0, "equity"), 182.0),  # fat finger
    ]:
        print(engine.run(order, reference_price=ref).summary())
        print()
