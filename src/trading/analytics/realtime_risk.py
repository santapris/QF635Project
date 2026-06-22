"""Real-time market risk utilities: VaR, CVaR and stress testing.

Standalone utility classes — no I/O, no async, no bus dependency. Suitable
for use in backtests, paper-trading runners, or offline risk reporting.

Classes
-------
MarketRiskEngine
    VaR (historical and parametric matrix form), CVaR, annualised volatility,
    and beta from a historical returns DataFrame.

StressTester
    Applies historical crash scenarios and custom shocks to a portfolio of
    positions. Includes six pre-built scenarios (2008, COVID, Flash Crash,
    2022 rate hikes, Crypto Winter 2018, Taper Tantrum 2013).

Data structures
---------------
StressPosition
    Lightweight representation of one position for stress testing.
MarketRiskMetrics
    Per-symbol VaR, CVaR, vol, and beta.
StressTestResult
    Output of one scenario run.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# z-scores for common confidence levels
_Z_SCORES: dict[float, float] = {0.90: 1.2816, 0.95: 1.6449, 0.99: 2.3263}


# ════════════════════════════════════════════════════════════════════
# Data containers
# ════════════════════════════════════════════════════════════════════

@dataclass
class StressPosition:
    """One position in a portfolio, for use with StressTester.

    Parameters
    ----------
    symbol:
        Instrument ticker (e.g. "BTC-USDT").
    asset_class:
        Broad class used for scenario shock lookup: "equity", "futures",
        "crypto", "fx", "bond", or "option".
    notional:
        Absolute position notional in quote currency.
    is_long:
        True for a long position, False for short.
    """
    symbol: str
    asset_class: str
    notional: float
    is_long: bool = True


@dataclass
class MarketRiskMetrics:
    """Per-symbol VaR and risk metrics."""
    symbol: str
    var_95: float = 0.0
    var_99: float = 0.0
    cvar_95: float = 0.0
    annualised_vol: float = 0.0
    beta: float = 1.0


@dataclass
class StressTestResult:
    """Result of applying one scenario to a portfolio."""
    scenario_name: str
    pnl_impact: float
    pnl_pct: float
    positions_affected: int
    worst_position: str = ""
    worst_position_loss: float = 0.0
    details: Dict[str, float] = field(default_factory=dict)


# ════════════════════════════════════════════════════════════════════
# Historical scenarios  (from the lecture notes)
# ════════════════════════════════════════════════════════════════════

HISTORICAL_SCENARIOS: Dict[str, Dict[str, float]] = {
    "2008 Financial Crisis": {
        "equity": -0.40, "futures": -0.30, "fx": -0.10, "crypto": 0.0,
        "bond": 0.05, "option": -0.45,
    },
    "COVID Crash Mar 2020": {
        "equity": -0.34, "futures": -0.25, "fx": -0.05, "crypto": -0.50,
        "bond": 0.08, "option": -0.40,
    },
    "Flash Crash May 2010": {
        "equity": -0.09, "futures": -0.07, "fx": -0.02, "crypto": 0.0,
    },
    "2022 Rate Hike Cycle": {
        "equity": -0.25, "futures": -0.15, "fx": 0.05, "crypto": -0.65,
        "bond": -0.15,
    },
    "Crypto Winter 2018": {
        "equity": -0.10, "futures": -0.05, "fx": 0.0, "crypto": -0.80,
    },
    "Taper Tantrum 2013": {
        "equity": -0.05, "futures": -0.08, "fx": -0.03, "crypto": 0.0,
        "bond": -0.10,
    },
}


# ════════════════════════════════════════════════════════════════════
# Market Risk Engine
# ════════════════════════════════════════════════════════════════════

class MarketRiskEngine:
    """VaR, CVaR, volatility and beta from historical daily returns.

    Usage::

        engine = MarketRiskEngine()
        engine.load_returns(returns_df)           # DataFrame: cols=symbols, rows=dates
        var = engine.historical_var("BTC", 10_000)
        port_var, _ = engine.portfolio_var_matrix({"BTC": 10_000, "ETH": 5_000})
    """

    VAR_WINDOW = 252  # days of history used for VaR

    def __init__(self) -> None:
        self.returns_history: pd.DataFrame = pd.DataFrame()
        self.benchmark_returns: pd.Series = pd.Series(dtype=float)

    def load_returns(
        self,
        returns_df: pd.DataFrame,
        benchmark: Optional[pd.Series] = None,
    ) -> None:
        """Load historical returns. Each column is a symbol; each row is a trading day.

        Values should be fractional daily returns (0.01 = +1%).
        """
        self.returns_history = returns_df.dropna(how="all")
        if benchmark is not None:
            self.benchmark_returns = benchmark.dropna()

    # ---- single-asset metrics -----------------------------------------------

    def historical_var(
        self, symbol: str, notional: float, confidence: float = 0.95
    ) -> float:
        """Historical VaR: loss at the (1−confidence) percentile, in currency units."""
        if symbol not in self.returns_history.columns:
            return 0.0
        rets = self.returns_history[symbol].dropna().tail(self.VAR_WINDOW)
        if rets.empty:
            return 0.0
        return abs(float(np.percentile(rets, (1 - confidence) * 100))) * notional

    def historical_cvar(
        self, symbol: str, notional: float, confidence: float = 0.95
    ) -> float:
        """Expected Shortfall (CVaR): average loss beyond the VaR threshold."""
        if symbol not in self.returns_history.columns:
            return 0.0
        rets = self.returns_history[symbol].dropna().tail(self.VAR_WINDOW)
        if rets.empty:
            return 0.0
        cutoff = np.percentile(rets, (1 - confidence) * 100)
        tail = rets[rets <= cutoff]
        return abs(float(tail.mean())) * notional if len(tail) else 0.0

    def annualised_volatility(self, symbol: str) -> float:
        """Annualised volatility from daily returns (×√252)."""
        if symbol not in self.returns_history.columns:
            return 0.0
        rets = self.returns_history[symbol].dropna().tail(self.VAR_WINDOW)
        return float(rets.std() * math.sqrt(self.VAR_WINDOW))

    def beta(self, symbol: str) -> float:
        """Beta vs. the benchmark loaded via load_returns(benchmark=...)."""
        if symbol not in self.returns_history.columns or self.benchmark_returns.empty:
            return 1.0
        sym = self.returns_history[symbol].dropna()
        bench = self.benchmark_returns.reindex(sym.index).dropna()
        sym = sym.reindex(bench.index)
        if len(bench) < 30:
            return 1.0
        var_b = float(np.var(bench))
        return float(np.cov(sym, bench)[0, 1] / var_b) if var_b else 1.0

    def symbol_metrics(self, symbol: str, notional: float) -> MarketRiskMetrics:
        """Compute all per-symbol metrics in one call."""
        return MarketRiskMetrics(
            symbol=symbol,
            var_95=self.historical_var(symbol, notional, 0.95),
            var_99=self.historical_var(symbol, notional, 0.99),
            cvar_95=self.historical_cvar(symbol, notional, 0.95),
            annualised_vol=self.annualised_volatility(symbol),
            beta=self.beta(symbol),
        )

    # ---- portfolio VaR (matrix form from the lecture notes) -----------------

    def portfolio_var_matrix(
        self,
        holdings: Dict[str, float],
        confidence: float = 0.95,
    ) -> Tuple[float, float]:
        """Parametric portfolio VaR using the covariance-matrix formula.

        sigma_p² = w' Σ w  (w = weight vector, Σ = covariance matrix)
        VaR      = z × sigma_p × V  (V = total notional)

        Returns (var, portfolio_std). Accounts for cross-asset correlations.
        """
        symbols = [s for s in holdings if s in self.returns_history.columns]
        if not symbols:
            return 0.0, 0.0
        notionals = np.array([holdings[s] for s in symbols], dtype=float)
        total = notionals.sum()
        if total <= 0:
            return 0.0, 0.0
        w = notionals / total
        rets = self.returns_history[symbols].dropna().tail(self.VAR_WINDOW)
        cov = rets.cov().values
        port_var = float(w @ cov @ w)
        port_std = math.sqrt(max(port_var, 0.0))
        z = _Z_SCORES.get(round(confidence, 2), 1.6449)
        return z * port_std * total, port_std

    def portfolio_cvar(
        self,
        holdings: Dict[str, float],
        confidence: float = 0.95,
    ) -> float:
        """Approximate portfolio CVaR (~1.25× parametric VaR under normality)."""
        var, _ = self.portfolio_var_matrix(holdings, confidence)
        return var * 1.25


# ════════════════════════════════════════════════════════════════════
# Stress Tester
# ════════════════════════════════════════════════════════════════════

class StressTester:
    """Apply historical crash scenarios and custom shocks to a portfolio.

    Usage::

        tester = StressTester(portfolio_value=100_000)
        positions = [StressPosition("BTC-USDT", "crypto", 50_000, is_long=True)]
        results = tester.run_all_historical(positions)
        StressTester.print_report(results)
    """

    def __init__(self, portfolio_value: float) -> None:
        self.portfolio_value = portfolio_value
        self.custom_scenarios: Dict[str, Dict[str, float]] = {}

    def add_custom_scenario(self, name: str, shocks: Dict[str, float]) -> None:
        """Register a custom scenario. shocks maps asset_class → fractional loss."""
        self.custom_scenarios[name] = shocks

    def _apply(
        self,
        name: str,
        shocks: Dict[str, float],
        positions: List[StressPosition],
    ) -> StressTestResult:
        total = 0.0
        details: Dict[str, float] = {}
        worst_pos, worst_loss = "", 0.0
        for pos in positions:
            shock = shocks.get(pos.symbol, shocks.get(pos.asset_class, 0.0))
            # Long position loses when asset drops; short profits.
            direction = 1 if pos.is_long else -1
            loss = pos.notional * shock * direction
            details[pos.symbol] = round(loss, 2)
            total += loss
            if loss < worst_loss:
                worst_loss, worst_pos = loss, pos.symbol
        pv = self.portfolio_value or 1.0
        return StressTestResult(
            scenario_name=name,
            pnl_impact=round(total, 2),
            pnl_pct=round(total / pv * 100, 2),
            positions_affected=sum(1 for v in details.values() if v != 0),
            worst_position=worst_pos,
            worst_position_loss=round(worst_loss, 2),
            details=details,
        )

    def run_all_historical(
        self, positions: List[StressPosition]
    ) -> List[StressTestResult]:
        """Run all six built-in historical scenarios, sorted worst-to-best."""
        results = [
            self._apply(name, shocks, positions)
            for name, shocks in HISTORICAL_SCENARIOS.items()
        ]
        return sorted(results, key=lambda r: r.pnl_impact)

    def run_custom(
        self, positions: List[StressPosition]
    ) -> List[StressTestResult]:
        """Run all custom scenarios registered via add_custom_scenario()."""
        return [
            self._apply(name, shocks, positions)
            for name, shocks in self.custom_scenarios.items()
        ]

    def what_if(
        self,
        positions: List[StressPosition],
        hypothetical: List[StressPosition],
        scenario_name: str = "What-If",
        shocks: Optional[Dict[str, float]] = None,
    ) -> Tuple[StressTestResult, StressTestResult]:
        """Compare current portfolio vs. current + hypothetical new positions."""
        shocks = shocks or {"equity": -0.20, "futures": -0.15, "fx": -0.05, "crypto": -0.40}
        current = self._apply(f"{scenario_name} [Current]", shocks, positions)
        projected = self._apply(f"{scenario_name} [+New]", shocks, positions + hypothetical)
        return current, projected

    @staticmethod
    def print_report(results: List[StressTestResult]) -> None:
        """Print a human-readable stress test summary."""
        print("\n=== STRESS TEST RESULTS ===")
        for r in results:
            print(
                f"  {r.scenario_name:30s} | "
                f"P&L ${r.pnl_impact:>12,.0f} ({r.pnl_pct:+6.1f}%) | "
                f"worst: {r.worst_position}"
            )


__all__ = [
    "HISTORICAL_SCENARIOS",
    "MarketRiskEngine",
    "MarketRiskMetrics",
    "StressPosition",
    "StressTester",
    "StressTestResult",
]
