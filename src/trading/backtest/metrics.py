"""Backtest performance metrics.

All inputs are simple Python lists. Inputs/outputs are floats (not
Decimal) because metrics are statistics, not money. Decimal-precision
PnL stays in fills and positions; once it gets here, we convert.

Annualisation factor depends on the bar frequency:

- 252 for daily equity bars (trading days/year)
- 365 * 24 * 60 = 525,600 for minute bars in 24/7 markets
- caller provides; this module makes no assumption.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

from ..core.events import FillEvent
from ..core.types import Side


@dataclass(frozen=True, slots=True)
class PerformanceMetrics:
    total_return: float
    annualized_return: float
    annualized_volatility: float
    sharpe_ratio: float
    sortino_ratio: float
    max_drawdown: float
    max_drawdown_pct: float
    num_trades: int
    win_rate: float
    profit_factor: float


def compute_returns(equity: Sequence[float]) -> list[float]:
    """Simple returns from an equity curve. Length = len(equity) - 1."""
    if len(equity) < 2:
        return []
    return [
        (equity[i] - equity[i - 1]) / equity[i - 1] if equity[i - 1] != 0 else 0.0
        for i in range(1, len(equity))
    ]


def sharpe_ratio(
    returns: Sequence[float],
    *,
    risk_free_rate: float = 0.0,
    periods_per_year: float = 252,
) -> float:
    """Annualised Sharpe ratio. Returns 0 for empty/zero-stddev inputs."""
    if not returns:
        return 0.0
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / max(1, len(returns) - 1)
    std = math.sqrt(var)
    if std == 0:
        return 0.0
    rf_per_period = risk_free_rate / periods_per_year
    excess = mean - rf_per_period
    return excess / std * math.sqrt(periods_per_year)


def sortino_ratio(
    returns: Sequence[float],
    *,
    risk_free_rate: float = 0.0,
    periods_per_year: float = 252,
) -> float:
    """Like Sharpe but uses downside deviation instead of total stddev."""
    if not returns:
        return 0.0
    rf_per_period = risk_free_rate / periods_per_year
    mean_excess = sum(r - rf_per_period for r in returns) / len(returns)
    downside = [min(0.0, r - rf_per_period) for r in returns]
    downside_var = sum(d * d for d in downside) / len(downside)
    downside_std = math.sqrt(downside_var)
    if downside_std == 0:
        return 0.0
    return mean_excess / downside_std * math.sqrt(periods_per_year)


def max_drawdown(equity: Sequence[float]) -> tuple[float, float]:
    """Return ``(absolute_drawdown, pct_drawdown)``.

    Both are positive numbers. ``absolute_drawdown`` is the peak-to-trough
    dollar loss; ``pct_drawdown`` is that loss divided by the peak.
    """
    if not equity:
        return 0.0, 0.0
    peak = equity[0]
    max_dd = 0.0
    max_dd_pct = 0.0
    for value in equity:
        if value > peak:
            peak = value
        dd = peak - value
        if dd > max_dd:
            max_dd = dd
            max_dd_pct = dd / peak if peak > 0 else 0.0
    return max_dd, max_dd_pct


def trade_statistics(fills: Sequence[FillEvent]) -> tuple[int, float, float]:
    """Return ``(num_round_trips, win_rate, profit_factor)``.

    Defines a "round trip" by FIFO matching: each opposite-side fill
    closes some quantity of the most-recent same-side open. PnL on the
    closing leg is signed by side. This is a simplified accounting
    sufficient for win-rate and profit-factor stats; the position
    engine remains the authoritative PnL source.
    """
    from collections import deque

    # Per-instrument lot queues, keyed by (strategy, instrument_id).
    queues: dict[tuple[str, str], deque[tuple[float, float, Side]]] = {}
    pnls: list[float] = []

    for fill in fills:
        key = (str(fill.strategy_id), fill.instrument.instrument_id)
        queue = queues.setdefault(key, deque())
        qty_remaining = float(fill.fill_quantity)
        price = float(fill.fill_price)
        # Close opposite-side lots until queue empty or fill exhausted.
        while queue and qty_remaining > 0:
            open_price, open_qty, open_side = queue[0]
            if open_side == fill.side:
                break  # same side: add to queue
            close_qty = min(open_qty, qty_remaining)
            sign = 1 if open_side is Side.BUY else -1
            pnls.append(sign * (price - open_price) * close_qty)
            qty_remaining -= close_qty
            if close_qty < open_qty:
                queue[0] = (open_price, open_qty - close_qty, open_side)
            else:
                queue.popleft()
        if qty_remaining > 0:
            queue.append((price, qty_remaining, fill.side))

    if not pnls:
        return 0, 0.0, 0.0
    wins = [p for p in pnls if p > 0]
    losses = [-p for p in pnls if p < 0]
    win_rate = len(wins) / len(pnls)
    gross_wins = sum(wins)
    gross_losses = sum(losses)
    profit_factor = gross_wins / gross_losses if gross_losses > 0 else float("inf")
    return len(pnls), win_rate, profit_factor


def compute_metrics(
    *,
    equity_curve: Sequence[float],
    fills: Sequence[FillEvent],
    periods_per_year: float = 252,
    risk_free_rate: float = 0.0,
) -> PerformanceMetrics:
    """Compute the full metric pack from an equity curve and fill list."""
    if not equity_curve:
        return PerformanceMetrics(
            total_return=0.0, annualized_return=0.0,
            annualized_volatility=0.0, sharpe_ratio=0.0, sortino_ratio=0.0,
            max_drawdown=0.0, max_drawdown_pct=0.0,
            num_trades=0, win_rate=0.0, profit_factor=0.0,
        )

    returns = compute_returns(equity_curve)
    total_return = (
        (equity_curve[-1] - equity_curve[0]) / equity_curve[0]
        if equity_curve[0] != 0 else 0.0
    )
    annualized_return = (
        ((1 + total_return) ** (periods_per_year / max(1, len(returns)))) - 1
        if returns else 0.0
    )
    if returns:
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / max(1, len(returns) - 1)
        ann_vol = math.sqrt(var) * math.sqrt(periods_per_year)
    else:
        ann_vol = 0.0
    sharpe = sharpe_ratio(returns, risk_free_rate=risk_free_rate, periods_per_year=periods_per_year)
    sortino = sortino_ratio(returns, risk_free_rate=risk_free_rate, periods_per_year=periods_per_year)
    mdd, mdd_pct = max_drawdown(equity_curve)
    num_trades, win_rate, profit_factor = trade_statistics(fills)

    return PerformanceMetrics(
        total_return=total_return,
        annualized_return=annualized_return,
        annualized_volatility=ann_vol,
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        max_drawdown=mdd,
        max_drawdown_pct=mdd_pct,
        num_trades=num_trades,
        win_rate=win_rate,
        profit_factor=profit_factor,
    )


__all__ = [
    "PerformanceMetrics",
    "compute_metrics",
    "compute_returns",
    "max_drawdown",
    "sharpe_ratio",
    "sortino_ratio",
    "trade_statistics",
]
