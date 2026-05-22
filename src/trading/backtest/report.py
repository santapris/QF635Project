"""Backtest report.

Captures the run-result triple: equity curve, fill log, and computed
metrics. Renders to either plain text (default) or a Python dict for
JSON serialisation.

Equity is sampled at each :class:`PnLSnapshotEvent` published by the
position engine. Fills are accumulated by topic subscription.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from ..core.events import FillEvent, PnLSnapshotEvent
from ..core.types import Timestamp
from .metrics import PerformanceMetrics, compute_metrics


@dataclass(frozen=True, slots=True)
class EquityPoint:
    ts_ns: Timestamp
    total_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    gross_exposure: float
    net_exposure: float


@dataclass(slots=True)
class BacktestReport:
    """Result of one backtest run."""

    fills: list[FillEvent] = field(default_factory=list)
    equity_points: list[EquityPoint] = field(default_factory=list)
    metrics: PerformanceMetrics | None = None

    # --- Recording (called by the engine) --------------------------------

    def record_fill(self, fill: FillEvent) -> None:
        self.fills.append(fill)

    def record_pnl_snapshot(self, snapshot: PnLSnapshotEvent) -> None:
        self.equity_points.append(EquityPoint(
            ts_ns=snapshot.ts_event,
            total_pnl=float(snapshot.total_pnl),
            realized_pnl=float(snapshot.realized_pnl),
            unrealized_pnl=float(snapshot.unrealized_pnl),
            gross_exposure=float(snapshot.gross_exposure),
            net_exposure=float(snapshot.net_exposure),
        ))

    # --- Finalisation ----------------------------------------------------

    def finalize(
        self,
        *,
        initial_equity: float = 0.0,
        periods_per_year: float = 252,
        risk_free_rate: float = 0.0,
    ) -> PerformanceMetrics:
        """Compute metrics from recorded data. Idempotent — call once after run."""
        equity_curve = [initial_equity + p.total_pnl for p in self.equity_points]
        if equity_curve and equity_curve[0] != initial_equity:
            # If the first sample isn't the start of the run, prepend the
            # initial value so total_return calculation is sensible.
            equity_curve = [initial_equity] + equity_curve
        self.metrics = compute_metrics(
            equity_curve=equity_curve,
            fills=self.fills,
            periods_per_year=periods_per_year,
            risk_free_rate=risk_free_rate,
        )
        return self.metrics

    # --- Rendering -------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "num_fills": len(self.fills),
            "num_equity_points": len(self.equity_points),
            "metrics": (
                {
                    "total_return": self.metrics.total_return,
                    "annualized_return": self.metrics.annualized_return,
                    "annualized_volatility": self.metrics.annualized_volatility,
                    "sharpe_ratio": self.metrics.sharpe_ratio,
                    "sortino_ratio": self.metrics.sortino_ratio,
                    "max_drawdown": self.metrics.max_drawdown,
                    "max_drawdown_pct": self.metrics.max_drawdown_pct,
                    "num_trades": self.metrics.num_trades,
                    "win_rate": self.metrics.win_rate,
                    "profit_factor": self.metrics.profit_factor,
                }
                if self.metrics is not None else None
            ),
            "first_fill_ts": self.fills[0].ts_event if self.fills else None,
            "last_fill_ts": self.fills[-1].ts_event if self.fills else None,
        }

    def summary(self) -> str:
        """One-page plain-text summary."""
        if self.metrics is None:
            return "BacktestReport (not finalized)"
        m = self.metrics
        lines = [
            "=" * 60,
            "BACKTEST REPORT",
            "=" * 60,
            f"Fills:                  {len(self.fills)}",
            f"Equity samples:         {len(self.equity_points)}",
            f"Trade round-trips:      {m.num_trades}",
            "",
            f"Total return:           {m.total_return:>10.2%}",
            f"Annualized return:      {m.annualized_return:>10.2%}",
            f"Annualized volatility:  {m.annualized_volatility:>10.2%}",
            f"Sharpe ratio:           {m.sharpe_ratio:>10.2f}",
            f"Sortino ratio:          {m.sortino_ratio:>10.2f}",
            "",
            f"Max drawdown:           {m.max_drawdown:>10.2f}",
            f"Max drawdown (%):       {m.max_drawdown_pct:>10.2%}",
            "",
            f"Win rate:               {m.win_rate:>10.2%}",
            f"Profit factor:          {m.profit_factor:>10.2f}",
            "=" * 60,
        ]
        return "\n".join(lines)


__all__ = ["BacktestReport", "EquityPoint"]
