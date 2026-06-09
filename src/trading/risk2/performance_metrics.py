"""
performance_metrics.py
=====================
Performance & Risk-Adjusted Metrics
(maps to "Performance Measures" + "Risk Measures" in the lecture notes)

These functions evaluate how GOOD a strategy is -- not just how much it made,
but how much risk it took to get there. The notes stress that risk-adjusted
metrics (Sharpe, drawdown, VaR) matter more than absolute return.

All functions take a pandas Series or numpy array of *returns* (per-period
fractional returns, e.g. 0.01 = +1%) unless noted otherwise.

Beginner note: "annualised" means scaled to a per-year figure. For daily data
we multiply returns by 252 (trading days) and volatility by sqrt(252).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

TRADING_DAYS = 252
ANNUALISE_VOL = math.sqrt(TRADING_DAYS)


# ════════════════════════════════════════════════════════════════════
# Return metrics
# ════════════════════════════════════════════════════════════════════

def total_return(equity_curve: pd.Series) -> float:
    """Total return from first to last equity value (0.25 = +25%)."""
    if len(equity_curve) < 2 or equity_curve.iloc[0] == 0:
        return 0.0
    return float(equity_curve.iloc[-1] / equity_curve.iloc[0] - 1.0)


def annualised_return(returns: pd.Series) -> float:
    """Geometric mean daily return scaled to a year."""
    returns = pd.Series(returns).dropna()
    if returns.empty:
        return 0.0
    growth = float((1 + returns).prod())
    years = len(returns) / TRADING_DAYS
    if years <= 0 or growth <= 0:
        return 0.0
    return growth ** (1 / years) - 1


def annualised_volatility(returns: pd.Series) -> float:
    """Standard deviation of returns scaled to a year."""
    returns = pd.Series(returns).dropna()
    if returns.empty:
        return 0.0
    return float(returns.std() * ANNUALISE_VOL)


# ════════════════════════════════════════════════════════════════════
# Risk-adjusted ratios
# ════════════════════════════════════════════════════════════════════

def sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.0) -> float:
    """
    Sharpe ratio = (annualised return - risk-free) / annualised volatility.
    Higher is better. A reading above ~1 is decent, above ~2 is strong.
    """
    vol = annualised_volatility(returns)
    if vol == 0:
        return 0.0
    return (annualised_return(returns) - risk_free_rate) / vol


def sortino_ratio(returns: pd.Series, risk_free_rate: float = 0.0,
                  target: float = 0.0) -> float:
    """
    Like Sharpe, but only penalises *downside* volatility (returns below
    `target`). Rewards strategies that are volatile only on the upside.
    """
    returns = pd.Series(returns).dropna()
    if returns.empty:
        return 0.0
    downside = returns[returns < target]
    downside_dev = float(downside.std() * ANNUALISE_VOL) if len(downside) else 0.0
    if downside_dev == 0:
        return 0.0
    return (annualised_return(returns) - risk_free_rate) / downside_dev


def calmar_ratio(returns: pd.Series) -> float:
    """
    Calmar ratio = annualised return / |maximum drawdown|.
    Measures return per unit of worst-case pain.
    """
    equity = (1 + pd.Series(returns).dropna()).cumprod()
    mdd = max_drawdown(equity)["max_drawdown"]
    if mdd == 0:
        return 0.0
    return annualised_return(returns) / abs(mdd)


# ════════════════════════════════════════════════════════════════════
# Drawdown  (uses the exact cummax method from the lecture notes)
# ════════════════════════════════════════════════════════════════════

def max_drawdown(equity_curve: pd.Series) -> Dict[str, float]:
    """
    Maximum drawdown = largest peak-to-trough decline in the equity curve.

    Implementation mirrors the lecture-note code:
        running_peak = equity.cummax()
        drawdown     = (equity - running_peak) / running_peak
        max_dd       = drawdown.min()          # most negative point

    Returns a dict with the worst drawdown (negative number), its percentage,
    and the peak/trough indices and values.
    """
    equity = pd.Series(equity_curve).reset_index(drop=True).astype(float)
    if len(equity) < 2:
        return {"max_drawdown": 0.0, "max_drawdown_pct": 0.0,
                "peak_index": 0, "trough_index": 0,
                "peak_value": float(equity.iloc[0]) if len(equity) else 0.0,
                "trough_value": float(equity.iloc[0]) if len(equity) else 0.0}

    running_peak = equity.cummax()
    drawdown = (equity - running_peak) / running_peak

    max_dd = float(drawdown.min())
    trough_idx = int(drawdown.idxmin())
    peak_idx = int(equity.iloc[:trough_idx + 1].idxmax())

    return {
        "max_drawdown": max_dd,                      # e.g. -0.30
        "max_drawdown_pct": max_dd * 100,            # e.g. -30.0
        "peak_index": peak_idx,
        "trough_index": trough_idx,
        "peak_value": float(equity.iloc[peak_idx]),
        "trough_value": float(equity.iloc[trough_idx]),
    }


def current_drawdown(equity_curve: pd.Series) -> float:
    """How far below the all-time peak we are right now (negative number)."""
    equity = pd.Series(equity_curve).astype(float)
    if equity.empty:
        return 0.0
    peak = float(equity.cummax().iloc[-1])
    if peak == 0:
        return 0.0
    return float((equity.iloc[-1] - peak) / peak)


# ════════════════════════════════════════════════════════════════════
# Trade-level statistics
# ════════════════════════════════════════════════════════════════════

def win_rate(trade_pnls: List[float]) -> float:
    """Fraction of trades that were profitable (0.55 = 55%)."""
    if not trade_pnls:
        return 0.0
    wins = sum(1 for p in trade_pnls if p > 0)
    return wins / len(trade_pnls)


def profit_factor(trade_pnls: List[float]) -> float:
    """
    Gross profit / gross loss. Above 1 means profitable overall.
    A profit factor of 1.5 means you make $1.50 for every $1 you lose.
    """
    gross_profit = sum(p for p in trade_pnls if p > 0)
    gross_loss = abs(sum(p for p in trade_pnls if p < 0))
    if gross_loss == 0:
        return float("inf") if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def average_win_loss(trade_pnls: List[float]) -> Tuple[float, float]:
    """Return (average winning trade, average losing trade)."""
    wins = [p for p in trade_pnls if p > 0]
    losses = [p for p in trade_pnls if p < 0]
    avg_win = float(np.mean(wins)) if wins else 0.0
    avg_loss = float(np.mean(losses)) if losses else 0.0
    return avg_win, avg_loss


def expectancy(trade_pnls: List[float]) -> float:
    """Average profit per trade -- the simplest 'edge' number."""
    return float(np.mean(trade_pnls)) if trade_pnls else 0.0


# ════════════════════════════════════════════════════════════════════
# Execution quality
# ════════════════════════════════════════════════════════════════════

def slippage(expected_price: float, fill_price: float, side: str) -> float:
    """
    Slippage in price terms: how much worse than expected we filled.
    Positive = we paid up / sold low (bad). Negative = price improvement (good).
    """
    if side.upper() == "BUY":
        return fill_price - expected_price      # paid more than expected = +ve
    return expected_price - fill_price          # sold lower than expected = +ve


def slippage_bps(expected_price: float, fill_price: float, side: str) -> float:
    """Slippage expressed in basis points (1 bp = 0.01%)."""
    if expected_price == 0:
        return 0.0
    return slippage(expected_price, fill_price, side) / expected_price * 10_000


# ════════════════════════════════════════════════════════════════════
# One-stop summary
# ════════════════════════════════════════════════════════════════════

@dataclass
class PerformanceReport:
    total_return_pct: float
    annual_return_pct: float
    annual_vol_pct: float
    sharpe: float
    sortino: float
    calmar: float
    max_drawdown_pct: float
    win_rate_pct: float
    profit_factor: float
    expectancy: float
    num_trades: int

    def pretty(self) -> str:
        return (
            "Performance Report\n"
            "------------------\n"
            f"  Total Return     : {self.total_return_pct:>8.2f}%\n"
            f"  Annual Return    : {self.annual_return_pct:>8.2f}%\n"
            f"  Annual Volatility: {self.annual_vol_pct:>8.2f}%\n"
            f"  Sharpe Ratio     : {self.sharpe:>8.2f}\n"
            f"  Sortino Ratio    : {self.sortino:>8.2f}\n"
            f"  Calmar Ratio     : {self.calmar:>8.2f}\n"
            f"  Max Drawdown     : {self.max_drawdown_pct:>8.2f}%\n"
            f"  Win Rate         : {self.win_rate_pct:>8.2f}%\n"
            f"  Profit Factor    : {self.profit_factor:>8.2f}\n"
            f"  Expectancy/Trade : {self.expectancy:>8.2f}\n"
            f"  Number of Trades : {self.num_trades:>8d}"
        )


def build_report(equity_curve: pd.Series, returns: pd.Series,
                 trade_pnls: Optional[List[float]] = None,
                 risk_free_rate: float = 0.0) -> PerformanceReport:
    """Compute every metric at once and return a tidy PerformanceReport."""
    trade_pnls = trade_pnls or []
    pf = profit_factor(trade_pnls)
    return PerformanceReport(
        total_return_pct=total_return(equity_curve) * 100,
        annual_return_pct=annualised_return(returns) * 100,
        annual_vol_pct=annualised_volatility(returns) * 100,
        sharpe=sharpe_ratio(returns, risk_free_rate),
        sortino=sortino_ratio(returns, risk_free_rate),
        calmar=calmar_ratio(returns),
        max_drawdown_pct=max_drawdown(equity_curve)["max_drawdown_pct"],
        win_rate_pct=win_rate(trade_pnls) * 100,
        profit_factor=pf if pf != float("inf") else 999.99,
        expectancy=expectancy(trade_pnls),
        num_trades=len(trade_pnls),
    )


# ════════════════════════════════════════════════════════════════════
# Smoke test  (reproduces the lecture-note MDD example: -41.67%)
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Lecture-note example equity curve
    equity = pd.Series([100, 110, 120, 105, 95, 130, 125, 90, 140])
    mdd = max_drawdown(equity)
    print("MDD example:", round(mdd["max_drawdown_pct"], 2), "%")

    # Synthetic returns for ratio demo
    rng = np.random.default_rng(0)
    rets = pd.Series(rng.normal(0.0006, 0.012, 252))
    eq = 1_000_000 * (1 + rets).cumprod()
    trades = list(rng.normal(50, 400, 120))
    print()
    print(build_report(eq, rets, trades, risk_free_rate=0.04).pretty())
