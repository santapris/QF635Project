"""
realtime_risk_engine.py
=====================
Layer 2 -- Real-Time Risk Engine (runs on every market-data tick)

This module continuously measures the live risk of the portfolio:

  * Market risk    : VaR (historical + parametric matrix form), CVaR,
                     volatility, beta
  * PnL & drawdown : live equity curve, current + maximum drawdown
  * Margin ratio   : leverage utilisation and distance to liquidation
  * Stress testing : pre-built historical crash scenarios
  * What-if        : compare current vs hypothetical portfolio

The matrix-form parametric VaR follows the lecture notes exactly:

    portfolio variance   sigma_p^2 = w' . Sigma . w
    portfolio std        sigma_p   = sqrt(sigma_p^2)
    95% 1-day VaR        VaR_95    = z_0.95 . sigma_p . V      (z_0.95 = 1.65)
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from data_models import Position
import performance_metrics as perf

logger = logging.getLogger(__name__)

# z-scores for common confidence levels
Z_SCORES = {0.90: 1.2816, 0.95: 1.6449, 0.99: 2.3263}


# ════════════════════════════════════════════════════════════════════
# Result containers
# ════════════════════════════════════════════════════════════════════

@dataclass
class MarketRiskMetrics:
    symbol: str
    var_95: float = 0.0
    var_99: float = 0.0
    cvar_95: float = 0.0
    annualised_vol: float = 0.0
    beta: float = 1.0


@dataclass
class PortfolioRiskSnapshot:
    gross_notional: float = 0.0
    net_notional: float = 0.0
    long_notional: float = 0.0
    short_notional: float = 0.0
    total_pnl: float = 0.0
    unrealised_pnl: float = 0.0
    realised_pnl: float = 0.0
    portfolio_var_95: float = 0.0
    portfolio_var_99: float = 0.0
    portfolio_cvar_95: float = 0.0
    portfolio_vol: float = 0.0
    current_drawdown: float = 0.0
    max_drawdown: float = 0.0
    peak_equity: float = 0.0
    current_equity: float = 0.0
    margin_ratio: float = 0.0
    gross_leverage: float = 0.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class StressTestResult:
    scenario_name: str
    pnl_impact: float
    pnl_pct: float
    positions_affected: int
    worst_position: str = ""
    worst_position_loss: float = 0.0
    details: Dict[str, float] = field(default_factory=dict)


# ════════════════════════════════════════════════════════════════════
# Market Risk Engine
# ════════════════════════════════════════════════════════════════════

class MarketRiskEngine:
    """VaR, CVaR, volatility and beta from historical returns."""

    VAR_WINDOW = 252

    def __init__(self):
        self.returns_history: pd.DataFrame = pd.DataFrame()
        self.benchmark_returns: pd.Series = pd.Series(dtype=float)

    def load_returns(self, returns_df: pd.DataFrame,
                     benchmark: Optional[pd.Series] = None) -> None:
        self.returns_history = returns_df.dropna(how="all")
        if benchmark is not None:
            self.benchmark_returns = benchmark.dropna()

    # ---- single-asset metrics -----------------------------------------

    def historical_var(self, symbol: str, notional: float,
                       confidence: float = 0.95) -> float:
        """Loss at the (1-confidence) percentile of historical returns, in $."""
        if symbol not in self.returns_history.columns:
            return 0.0
        rets = self.returns_history[symbol].dropna().tail(self.VAR_WINDOW)
        if rets.empty:
            return 0.0
        percentile = (1 - confidence) * 100
        return abs(float(np.percentile(rets, percentile))) * notional

    def historical_cvar(self, symbol: str, notional: float,
                        confidence: float = 0.95) -> float:
        """Expected shortfall: average loss beyond the VaR threshold."""
        if symbol not in self.returns_history.columns:
            return 0.0
        rets = self.returns_history[symbol].dropna().tail(self.VAR_WINDOW)
        if rets.empty:
            return 0.0
        cutoff = np.percentile(rets, (1 - confidence) * 100)
        tail = rets[rets <= cutoff]
        return abs(float(tail.mean())) * notional if len(tail) else 0.0

    def annualised_volatility(self, symbol: str) -> float:
        if symbol not in self.returns_history.columns:
            return 0.0
        rets = self.returns_history[symbol].dropna().tail(self.VAR_WINDOW)
        return float(rets.std() * math.sqrt(self.VAR_WINDOW))

    def beta(self, symbol: str) -> float:
        if (symbol not in self.returns_history.columns
                or self.benchmark_returns.empty):
            return 1.0
        sym = self.returns_history[symbol].dropna()
        bench = self.benchmark_returns.reindex(sym.index).dropna()
        sym = sym.reindex(bench.index)
        if len(bench) < 30:
            return 1.0
        var_b = float(np.var(bench))
        return float(np.cov(sym, bench)[0, 1] / var_b) if var_b else 1.0

    def symbol_metrics(self, symbol: str, notional: float) -> MarketRiskMetrics:
        return MarketRiskMetrics(
            symbol=symbol,
            var_95=self.historical_var(symbol, notional, 0.95),
            var_99=self.historical_var(symbol, notional, 0.99),
            cvar_95=self.historical_cvar(symbol, notional, 0.95),
            annualised_vol=self.annualised_volatility(symbol),
            beta=self.beta(symbol),
        )

    # ---- portfolio VaR (matrix form from the notes) -------------------

    def portfolio_var_matrix(self, holdings: Dict[str, float],
                             confidence: float = 0.95) -> Tuple[float, float]:
        """
        Parametric portfolio VaR using the lecture-note matrix formula.

            sigma_p^2 = w' Sigma w        (w = weight vector, Sigma = covariance)
            VaR       = z * sigma_p * V   (V = total notional)

        Returns (var, portfolio_std). Accounts for correlations between assets.
        """
        symbols = [s for s in holdings if s in self.returns_history.columns]
        if not symbols:
            return 0.0, 0.0

        notionals = np.array([holdings[s] for s in symbols], dtype=float)
        total = notionals.sum()
        if total <= 0:
            return 0.0, 0.0
        w = notionals / total                                # weight vector

        rets = self.returns_history[symbols].dropna().tail(self.VAR_WINDOW)
        cov = rets.cov().values                              # Sigma

        port_var = float(w @ cov @ w)                        # w' Sigma w
        port_std = math.sqrt(max(port_var, 0.0))             # sigma_p
        z = Z_SCORES.get(round(confidence, 2), 1.6449)
        var = z * port_std * total                           # z * sigma_p * V
        return var, port_std

    def portfolio_cvar(self, holdings: Dict[str, float],
                       confidence: float = 0.95) -> float:
        """Approximate portfolio CVaR (~1.25x the parametric VaR for normal)."""
        var, _ = self.portfolio_var_matrix(holdings, confidence)
        return var * 1.25


# ════════════════════════════════════════════════════════════════════
# PnL & Drawdown Tracker
# ════════════════════════════════════════════════════════════════════

class PnLDrawdownTracker:
    """Maintains the live equity curve and drawdown statistics."""

    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.realised_pnl: float = 0.0
        self.equity_curve: List[Tuple[datetime, float]] = []
        self._peak_equity: float = initial_capital
        self._max_drawdown: float = 0.0

    def update(self, unrealised_pnl: float,
               timestamp: Optional[datetime] = None) -> float:
        ts = timestamp or datetime.now(timezone.utc)
        equity = self.initial_capital + self.realised_pnl + unrealised_pnl
        self.equity_curve.append((ts, equity))
        if equity > self._peak_equity:
            self._peak_equity = equity
        dd = ((self._peak_equity - equity) / self._peak_equity
              if self._peak_equity > 0 else 0.0)
        self._max_drawdown = max(self._max_drawdown, dd)
        return dd

    def add_realised_pnl(self, pnl: float) -> None:
        self.realised_pnl += pnl

    @property
    def current_equity(self) -> float:
        return self.equity_curve[-1][1] if self.equity_curve else self.initial_capital

    @property
    def current_drawdown(self) -> float:
        if self._peak_equity == 0:
            return 0.0
        return (self._peak_equity - self.current_equity) / self._peak_equity

    @property
    def max_drawdown(self) -> float:
        return self._max_drawdown

    @property
    def peak_equity(self) -> float:
        return self._peak_equity

    def equity_series(self) -> pd.Series:
        if not self.equity_curve:
            return pd.Series(dtype=float)
        ts, eq = zip(*self.equity_curve)
        return pd.Series(eq, index=pd.DatetimeIndex(ts))

    def summary(self) -> Dict[str, float]:
        return {
            "initial_capital": self.initial_capital,
            "current_equity": round(self.current_equity, 2),
            "total_pnl": round(self.current_equity - self.initial_capital, 2),
            "realised_pnl": round(self.realised_pnl, 2),
            "current_drawdown_pct": round(self.current_drawdown * 100, 2),
            "max_drawdown_pct": round(self.max_drawdown * 100, 2),
            "peak_equity": round(self.peak_equity, 2),
        }


# ════════════════════════════════════════════════════════════════════
# Margin / Leverage Monitor  (from the "Margin Ratio" slide)
# ════════════════════════════════════════════════════════════════════

class MarginMonitor:
    """
    Tracks how close the account is to a margin call / liquidation.

    margin_ratio = maintenance margin required / account equity
    (1.0 = at the liquidation threshold). We also estimate a per-position
    liquidation price using the standard isolated-margin formula:

        long  : liq = entry * (1 - 1/leverage + maintenance_margin_rate)
        short : liq = entry * (1 + 1/leverage - maintenance_margin_rate)
    """

    def __init__(self, warning: float = 0.50, critical: float = 0.80):
        self.warning = warning
        self.critical = critical

    def account_margin_ratio(self, positions: List[Position],
                             equity: float) -> float:
        if equity <= 0:
            return 1.0
        maintenance = sum(p.maintenance_margin for p in positions)
        return maintenance / equity

    def margin_utilisation(self, positions: List[Position],
                           equity: float) -> float:
        """Fraction of equity tied up as initial margin across positions."""
        if equity <= 0:
            return 1.0
        return sum(p.margin_used for p in positions) / equity

    @staticmethod
    def liquidation_price(position: Position) -> float:
        lev = max(position.leverage, 1.0)
        mmr = position.maintenance_margin_rate
        if position.is_long:
            return position.avg_price * (1 - 1 / lev + mmr)
        return position.avg_price * (1 + 1 / lev - mmr)

    @staticmethod
    def distance_to_liquidation(position: Position) -> float:
        """Fractional distance from current price to liquidation (0.10 = 10%)."""
        liq = MarginMonitor.liquidation_price(position)
        mark = position.mark_price
        if mark <= 0:
            return 0.0
        return abs(mark - liq) / mark

    def status(self, positions: List[Position], equity: float) -> str:
        ratio = self.account_margin_ratio(positions, equity)
        if ratio >= self.critical:
            return "CRITICAL"
        if ratio >= self.warning:
            return "WARNING"
        return "OK"


# ════════════════════════════════════════════════════════════════════
# Stress Testing & What-If
# ════════════════════════════════════════════════════════════════════

HISTORICAL_SCENARIOS: Dict[str, Dict[str, float]] = {
    "2008 Financial Crisis": {
        "equity": -0.40, "futures": -0.30, "fx": -0.10, "crypto": 0.0,
        "bond": 0.05, "option": -0.45,
        "_description": "Lehman collapse; ~40% equity drawdown",
    },
    "COVID Crash Mar 2020": {
        "equity": -0.34, "futures": -0.25, "fx": -0.05, "crypto": -0.50,
        "bond": 0.08, "option": -0.40,
        "_description": "Pandemic selloff; 34% peak-to-trough in 33 days",
    },
    "Flash Crash May 2010": {
        "equity": -0.09, "futures": -0.07, "fx": -0.02, "crypto": 0.0,
        "_description": "Intraday liquidity evaporation",
    },
    "2022 Rate Hike Cycle": {
        "equity": -0.25, "futures": -0.15, "fx": 0.05, "crypto": -0.65,
        "bond": -0.15,
        "_description": "Fed hikes; growth & crypto sold off hard",
    },
    "Crypto Winter 2018": {
        "equity": -0.10, "futures": -0.05, "fx": 0.0, "crypto": -0.80,
        "_description": "BTC -85%; broad crypto collapse",
    },
    "Taper Tantrum 2013": {
        "equity": -0.05, "futures": -0.08, "fx": -0.03, "crypto": 0.0,
        "bond": -0.10,
        "_description": "Bond selloff on Fed tapering signal",
    },
}


class StressTester:
    """Applies historical & custom shocks to the portfolio."""

    def __init__(self, portfolio_value: float):
        self.portfolio_value = portfolio_value
        self.custom_scenarios: Dict[str, Dict[str, float]] = {}

    def add_custom_scenario(self, name: str, shocks: Dict[str, float]) -> None:
        self.custom_scenarios[name] = shocks

    def _apply(self, name: str, shocks: Dict[str, float],
               positions: List[Position]) -> StressTestResult:
        total = 0.0
        details: Dict[str, float] = {}
        worst_pos, worst_loss = "", 0.0
        for pos in positions:
            shock = shocks.get(pos.symbol, shocks.get(pos.asset_class, 0.0))
            # a long position loses when the asset drops; a short gains
            direction = 1 if pos.is_long else -1
            loss = pos.notional * shock * direction
            details[pos.symbol] = round(loss, 2)
            total += loss
            if loss < worst_loss:
                worst_loss, worst_pos = loss, pos.symbol
        return StressTestResult(
            scenario_name=name,
            pnl_impact=round(total, 2),
            pnl_pct=round(total / self.portfolio_value * 100, 2) if self.portfolio_value else 0.0,
            positions_affected=sum(1 for v in details.values() if v != 0),
            worst_position=worst_pos,
            worst_position_loss=round(worst_loss, 2),
            details=details,
        )

    def run_all_historical(self, positions: List[Position]) -> List[StressTestResult]:
        results = []
        for name, shocks in HISTORICAL_SCENARIOS.items():
            clean = {k: v for k, v in shocks.items() if not k.startswith("_")}
            results.append(self._apply(name, clean, positions))
        return sorted(results, key=lambda r: r.pnl_impact)

    def run_custom(self, positions: List[Position]) -> List[StressTestResult]:
        return [self._apply(n, s, positions) for n, s in self.custom_scenarios.items()]

    def what_if(self, positions: List[Position],
                hypothetical: List[Position],
                scenario_name: str = "What-If",
                shocks: Optional[Dict[str, float]] = None
                ) -> Tuple[StressTestResult, StressTestResult]:
        shocks = shocks or {"equity": -0.20, "futures": -0.15,
                            "fx": -0.05, "crypto": -0.40}
        current = self._apply(f"{scenario_name} [Current]", shocks, positions)
        projected = self._apply(f"{scenario_name} [+New]", shocks,
                                positions + hypothetical)
        return current, projected

    def sensitivity(self, positions: List[Position], asset_class: str,
                    shock_range: Tuple[float, float] = (-0.30, 0.10),
                    steps: int = 9) -> pd.DataFrame:
        rows = []
        for shock in np.linspace(shock_range[0], shock_range[1], steps):
            r = self._apply(f"{asset_class} {shock:+.0%}",
                            {asset_class: shock}, positions)
            rows.append({"shock": f"{shock:+.1%}",
                         "pnl_impact_$": r.pnl_impact,
                         "pnl_impact_%": round(r.pnl_pct, 2)})
        return pd.DataFrame(rows)

    @staticmethod
    def print_report(results: List[StressTestResult]) -> None:
        print("\n=== STRESS TEST RESULTS ===")
        for r in results:
            print(f"  {r.scenario_name:30s} | P&L ${r.pnl_impact:>12,.0f} "
                  f"({r.pnl_pct:+6.1f}%) | worst: {r.worst_position}")


# ════════════════════════════════════════════════════════════════════
# Real-Time Risk Monitor  (ties Layer 2 together)
# ════════════════════════════════════════════════════════════════════

class RealTimeRiskMonitor:
    """Unified Layer-2 engine: call tick() on every market-data update."""

    def __init__(self, initial_capital: float,
                 var_warning_pct: float = 0.05,
                 drawdown_alert_pct: float = 0.05,
                 drawdown_halt_pct: float = 0.15,
                 margin_warning: float = 0.50,
                 margin_critical: float = 0.80):
        self.initial_capital = initial_capital
        self.var_warning_pct = var_warning_pct
        self.drawdown_alert_pct = drawdown_alert_pct
        self.drawdown_halt_pct = drawdown_halt_pct

        self.market_risk = MarketRiskEngine()
        self.pnl_tracker = PnLDrawdownTracker(initial_capital)
        self.margin_monitor = MarginMonitor(margin_warning, margin_critical)
        self.stress_tester = StressTester(initial_capital)

        self.positions: List[Position] = []
        self._alerts: List[str] = []

    def load_returns(self, returns_df: pd.DataFrame,
                     benchmark: Optional[pd.Series] = None) -> None:
        self.market_risk.load_returns(returns_df, benchmark)

    def update_positions(self, positions: List[Position]) -> None:
        self.positions = positions

    @property
    def alerts(self) -> List[str]:
        return list(self._alerts)

    def tick(self, market_prices: Dict[str, float]) -> PortfolioRiskSnapshot:
        self._alerts.clear()

        for pos in self.positions:
            if pos.symbol in market_prices:
                pos.current_price = market_prices[pos.symbol]

        unrealised = sum(p.unrealised_pnl for p in self.positions)
        drawdown = self.pnl_tracker.update(unrealised)
        equity = self.pnl_tracker.current_equity

        holdings = {p.symbol: p.notional for p in self.positions}
        pvar95, pstd = self.market_risk.portfolio_var_matrix(holdings, 0.95)
        pvar99, _ = self.market_risk.portfolio_var_matrix(holdings, 0.99)
        pcvar95 = self.market_risk.portfolio_cvar(holdings, 0.95)

        long_notional = sum(p.notional for p in self.positions if p.is_long)
        short_notional = sum(p.notional for p in self.positions if p.is_short)
        gross = long_notional + short_notional
        margin_ratio = self.margin_monitor.account_margin_ratio(self.positions, equity)

        # alerts
        if equity > 0 and pvar95 / equity > self.var_warning_pct:
            self._alerts.append(
                f"Portfolio VaR(95%) ${pvar95:,.0f} > {self.var_warning_pct:.0%} of equity")
        if drawdown >= self.drawdown_halt_pct:
            self._alerts.append(f"DRAWDOWN HALT level reached: {drawdown:.1%}")
        elif drawdown >= self.drawdown_alert_pct:
            self._alerts.append(f"Drawdown alert: {drawdown:.1%}")
        margin_status = self.margin_monitor.status(self.positions, equity)
        if margin_status != "OK":
            self._alerts.append(f"Margin {margin_status}: ratio {margin_ratio:.1%}")

        for a in self._alerts:
            logger.warning("RT ALERT | %s", a)

        return PortfolioRiskSnapshot(
            gross_notional=gross,
            net_notional=sum(p.signed_notional for p in self.positions),
            long_notional=long_notional,
            short_notional=short_notional,
            unrealised_pnl=unrealised,
            realised_pnl=self.pnl_tracker.realised_pnl,
            total_pnl=self.pnl_tracker.realised_pnl + unrealised,
            portfolio_var_95=pvar95,
            portfolio_var_99=pvar99,
            portfolio_cvar_95=pcvar95,
            portfolio_vol=pstd,
            current_drawdown=drawdown,
            max_drawdown=self.pnl_tracker.max_drawdown,
            peak_equity=self.pnl_tracker.peak_equity,
            current_equity=equity,
            margin_ratio=margin_ratio,
            gross_leverage=gross / equity if equity > 0 else 0.0,
        )

    def run_stress_tests(self) -> List[StressTestResult]:
        return self.stress_tester.run_all_historical(self.positions)

    def print_snapshot(self, s: PortfolioRiskSnapshot) -> None:
        print("\n--- REAL-TIME RISK SNAPSHOT ---")
        print(f"  Equity            : ${s.current_equity:>14,.0f}")
        print(f"  Gross / Net Not.  : ${s.gross_notional:>14,.0f} / ${s.net_notional:,.0f}")
        print(f"  Long / Short      : ${s.long_notional:>14,.0f} / ${s.short_notional:,.0f}")
        print(f"  Unrealised P&L    : ${s.unrealised_pnl:>+14,.0f}")
        print(f"  Realised P&L      : ${s.realised_pnl:>+14,.0f}")
        print(f"  Portfolio VaR 95% : ${s.portfolio_var_95:>14,.0f}")
        print(f"  Portfolio VaR 99% : ${s.portfolio_var_99:>14,.0f}")
        print(f"  CVaR 95%          : ${s.portfolio_cvar_95:>14,.0f}")
        print(f"  Current Drawdown  : {s.current_drawdown:>14.1%}")
        print(f"  Max Drawdown      : {s.max_drawdown:>14.1%}")
        print(f"  Gross Leverage    : {s.gross_leverage:>13.2f}x")
        print(f"  Margin Ratio      : {s.margin_ratio:>14.1%}")
        for a in self.alerts:
            print(f"  ! ALERT: {a}")


# ════════════════════════════════════════════════════════════════════
# Smoke test
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s | %(message)s")

    rng = np.random.default_rng(42)
    dates = pd.date_range("2023-01-01", periods=300, freq="B")
    df = pd.DataFrame(rng.normal(0.0005, 0.015, (300, 3)),
                      index=dates, columns=["AAPL", "MSFT", "BTC"])
    bench = pd.Series(rng.normal(0.0004, 0.012, 300), index=dates)

    mon = RealTimeRiskMonitor(1_000_000)
    mon.load_returns(df, bench)
    mon.update_positions([
        Position("AAPL", 500, 180.0, "equity", current_price=185.0),
        Position("MSFT", 300, 320.0, "equity", current_price=310.0),
        Position("BTC", 2, 30000, "crypto", current_price=28000, leverage=3.0),
    ])
    snap = mon.tick({"AAPL": 185, "MSFT": 310, "BTC": 28000})
    mon.print_snapshot(snap)
    StressTester.print_report(mon.run_stress_tests())
