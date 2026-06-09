"""
main_risk_manager.py
=====================
Master Risk Management Orchestrator

This is the SINGLE entry point that wires the three layers together:

    Layer 1  pre_trade_checks.py     -> validate every order
    Layer 2  realtime_risk_engine.py -> live VaR / PnL / drawdown / margin
    Layer 3  risk_controls.py        -> throttle / circuit breaker / kill switch

plus the helper modules:

    position_sizing.py     -> how big should the trade be
    market_making.py       -> Avellaneda-Stoikov inventory-aware quotes
    performance_metrics.py -> Sharpe / Sortino / drawdown / win rate

Typical lifecycle (see example_usage.py for a full runnable demo):

    rm = MasterRiskManager(RiskManagerConfig(initial_capital=1_000_000))
    rm.load_returns_history(returns_df, benchmark)
    rm.load_positions(positions)

    # every market tick:
    snapshot = rm.tick(latest_prices)

    # before every order:
    approved, reason, result = rm.approve_order(order, reference_price=mid)
    if approved:
        broker.send(order); rm.on_order_sent(order)

    # after every fill:
    rm.on_trade_result(realised_pnl)

Run modes:
    python main_risk_manager.py --demo            # built-in demo
    python main_risk_manager.py --config cfg.json # load a saved config
    python main_risk_manager.py                   # interactive setup
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from data_models import TradeOrder, Position, RiskLimits, PreTradeResult, CheckResult
from pre_trade_checks import PreTradeRiskEngine
from realtime_risk_engine import (
    RealTimeRiskMonitor, StressTester, PortfolioRiskSnapshot, StressTestResult,
)
from risk_controls import (
    RiskControlsManager, ThrottleConfig, CircuitBreakerConfig,
    ControlState, ControlEvent,
)
import position_sizing as sizing
import performance_metrics as perf
from market_making import AvellanedaStoikovQuoter, AvellanedaStoikovParams, Quote


# ════════════════════════════════════════════════════════════════════
# Logging
# ════════════════════════════════════════════════════════════════════

def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        d = os.path.dirname(log_file)
        if d:
            os.makedirs(d, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        handlers=handlers, force=True)

logger = logging.getLogger("RiskManager")


# ════════════════════════════════════════════════════════════════════
# Configuration
# ════════════════════════════════════════════════════════════════════

@dataclass
class RiskManagerConfig:
    initial_capital: float = 1_000_000.0
    risk_limits: dict = field(default_factory=dict)

    # real-time thresholds
    var_warning_pct: float = 0.05
    drawdown_alert_pct: float = 0.05
    drawdown_halt_pct: float = 0.15
    margin_warning: float = 0.50
    margin_critical: float = 0.80

    # circuit breaker
    breaker_drawdown_warning: float = 0.05
    breaker_drawdown_halt: float = 0.10
    breaker_drawdown_kill: float = 0.20
    breaker_daily_loss_halt: float = 0.05
    breaker_daily_loss_kill: float = 0.10
    breaker_var_halt: float = 0.08
    breaker_margin_halt: float = 0.80
    breaker_consecutive_loss_halt: int = 5
    breaker_auto_reset_seconds: float = 0.0

    # throttle
    throttle_max_orders_per_second: float = 10.0
    throttle_max_orders_per_minute: float = 200.0
    throttle_max_notional_per_minute: float = 5_000_000.0
    throttle_max_notional_per_day: float = 50_000_000.0
    throttle_max_open_order_notional: float = 50_000_000.0
    throttle_max_active_orders: int = 5_000

    # data
    max_data_staleness_seconds: float = 5.0

    log_level: str = "INFO"
    log_file: Optional[str] = "logs/risk_manager.log"

    def save(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2, default=str)

    @classmethod
    def load(cls, path: str) -> "RiskManagerConfig":
        with open(path) as f:
            data = json.load(f)
        cfg = cls()
        for k, v in data.items():
            if hasattr(cfg, k):
                setattr(cfg, k, v)
        return cfg


# ════════════════════════════════════════════════════════════════════
# Master Risk Manager
# ════════════════════════════════════════════════════════════════════

class MasterRiskManager:
    VERSION = "2.0.0"

    def __init__(self, config: RiskManagerConfig):
        self.config = config
        setup_logging(config.log_level, config.log_file)
        logger.info("MasterRiskManager v%s starting (capital $%s)",
                    self.VERSION, f"{config.initial_capital:,.0f}")

        # build risk limits
        limits = RiskLimits(total_portfolio_value=config.initial_capital)
        for k, v in (config.risk_limits or {}).items():
            if hasattr(limits, k):
                setattr(limits, k, v)
        self.limits = limits

        # Layer 1
        self.pre_trade = PreTradeRiskEngine(limits)
        self.pre_trade.set_account_equity(config.initial_capital)

        # Layer 2
        self.rt_monitor = RealTimeRiskMonitor(
            initial_capital=config.initial_capital,
            var_warning_pct=config.var_warning_pct,
            drawdown_alert_pct=config.drawdown_alert_pct,
            drawdown_halt_pct=config.drawdown_halt_pct,
            margin_warning=config.margin_warning,
            margin_critical=config.margin_critical)

        # Layer 3
        self.controls = RiskControlsManager(
            throttle_config=ThrottleConfig(
                max_orders_per_second=config.throttle_max_orders_per_second,
                max_orders_per_minute=config.throttle_max_orders_per_minute,
                max_notional_per_minute=config.throttle_max_notional_per_minute,
                max_notional_per_day=config.throttle_max_notional_per_day,
                max_open_order_notional=config.throttle_max_open_order_notional,
                max_active_orders=config.throttle_max_active_orders),
            breaker_config=CircuitBreakerConfig(
                capital=config.initial_capital,
                drawdown_warning_pct=config.breaker_drawdown_warning,
                drawdown_halt_pct=config.breaker_drawdown_halt,
                drawdown_kill_pct=config.breaker_drawdown_kill,
                daily_loss_halt_pct=config.breaker_daily_loss_halt,
                daily_loss_kill_pct=config.breaker_daily_loss_kill,
                var_halt_pct=config.breaker_var_halt,
                margin_halt=config.breaker_margin_halt,
                consecutive_loss_halt=config.breaker_consecutive_loss_halt,
                auto_reset_seconds=config.breaker_auto_reset_seconds),
            cancel_all_fn=self._cancel_all_orders,
            max_data_staleness=config.max_data_staleness_seconds)
        self.controls.circuit_breaker.register_callback(self._on_state_change)

        # helpers
        self.quoter = AvellanedaStoikovQuoter()

        self._lock = threading.RLock()
        self._latest_snapshot: Optional[PortfolioRiskSnapshot] = None
        self._trade_pnls: List[float] = []
        logger.info("MasterRiskManager ready.")

    # ── data loading ───────────────────────────────────────────────

    def load_positions(self, positions: List[Position]) -> None:
        with self._lock:
            self.pre_trade.load_positions(positions)
            self.rt_monitor.update_positions(positions)
        logger.info("Loaded %d positions", len(positions))

    def load_returns_history(self, returns_df: pd.DataFrame,
                             benchmark: Optional[pd.Series] = None) -> None:
        with self._lock:
            self.pre_trade.set_returns_history(returns_df)
            self.rt_monitor.load_returns(returns_df, benchmark)
        logger.info("Returns history: %d symbols x %d days",
                    len(returns_df.columns), len(returns_df))

    def update_position(self, position: Position) -> None:
        with self._lock:
            self.pre_trade.update_position(position)
            self.rt_monitor.update_positions(list(self.pre_trade.positions.values()))

    # ── order workflow ─────────────────────────────────────────────

    def approve_order(self, order: TradeOrder,
                      reference_price: Optional[float] = None
                      ) -> Tuple[bool, str, PreTradeResult]:
        """Full gate: Layer-3 controls first, then Layer-1 pre-trade checks."""
        with self._lock:
            ok, msg = self.controls.check_order(order.notional)
            if not ok:
                logger.warning("ORDER BLOCKED (controls) | %s | %s", order.symbol, msg)
                res = PreTradeResult(order=order, results=[
                    CheckResult("controls", False, "hard", msg)])
                return False, msg, res

            res = self.pre_trade.run(order, reference_price=reference_price)
            if not res.passed:
                reasons = "; ".join(res.violations)
                logger.warning("ORDER BLOCKED (pre-trade) | %s | %s", order.symbol, reasons)
                return False, reasons, res
            return True, "APPROVED", res

    def on_order_sent(self, order: TradeOrder) -> None:
        self.controls.on_order_sent(order.notional)

    def on_order_filled_or_cancelled(self, order: TradeOrder) -> None:
        self.controls.on_order_filled_or_cancelled(order.notional)

    def on_order_error(self) -> None:
        self.controls.on_order_error()

    def on_market_data(self) -> None:
        """Call when fresh market data arrives (resets the staleness timer)."""
        self.controls.on_market_data()

    def on_trade_result(self, pnl: float) -> None:
        """Call after each fill; updates circuit breaker streak + realised PnL."""
        self._trade_pnls.append(pnl)
        self.controls.update_risk_metrics(
            trade_result="win" if pnl >= 0 else "loss")
        self.rt_monitor.pnl_tracker.add_realised_pnl(pnl)

    # ── real-time tick ─────────────────────────────────────────────

    def tick(self, market_prices: Dict[str, float]) -> PortfolioRiskSnapshot:
        with self._lock:
            self.controls.on_market_data()
            snap = self.rt_monitor.tick(market_prices)
            self._latest_snapshot = snap
            self.pre_trade.set_account_equity(snap.current_equity)
            cap = self.config.initial_capital
            self.controls.update_risk_metrics(
                drawdown_pct=snap.current_drawdown,
                daily_pnl=snap.total_pnl,
                var_pct=snap.portfolio_var_95 / cap if cap else 0.0,
                margin_ratio=snap.margin_ratio)
            for a in self.rt_monitor.alerts:
                logger.warning("RT ALERT | %s", a)
            return snap

    # ── position sizing helpers ────────────────────────────────────

    def size_by_atr(self, atr: float, risk_pct: float = 0.01,
                    multiplier: float = 2.0) -> float:
        return sizing.atr_position_size(
            self.config.initial_capital, risk_pct, atr, multiplier)

    def size_by_volatility_target(self, asset_vol: float, price: float,
                                  target_vol: float = 0.10) -> float:
        return sizing.volatility_target_size(
            self.config.initial_capital, target_vol, asset_vol, price,
            self.limits.max_gross_leverage)

    def size_by_signal(self, max_position: float, signal: float,
                       method: str = "tanh") -> float:
        return sizing.signal_scaled_size(max_position, signal, method)

    # ── market-making helper ───────────────────────────────────────

    def make_quote(self, mid_price: float, inventory: float,
                   time_elapsed: float = 0.0,
                   params: Optional[AvellanedaStoikovParams] = None) -> Quote:
        if params:
            self.quoter.params = params
        return self.quoter.quote(mid_price, inventory, time_elapsed)

    # ── stress / what-if ───────────────────────────────────────────

    def run_stress_tests(self) -> List[StressTestResult]:
        with self._lock:
            return self.rt_monitor.run_stress_tests()

    def what_if_analysis(self, hypothetical: List[Position],
                         scenario_name: str = "What-If",
                         shocks: Optional[Dict[str, float]] = None
                         ) -> Tuple[StressTestResult, StressTestResult]:
        with self._lock:
            return self.rt_monitor.stress_tester.what_if(
                list(self.rt_monitor.positions), hypothetical, scenario_name, shocks)

    def sensitivity_report(self, asset_class: str) -> pd.DataFrame:
        with self._lock:
            return self.rt_monitor.stress_tester.sensitivity(
                list(self.rt_monitor.positions), asset_class)

    # ── performance ────────────────────────────────────────────────

    def performance_report(self, risk_free_rate: float = 0.04) -> perf.PerformanceReport:
        equity = self.rt_monitor.pnl_tracker.equity_series()
        returns = equity.pct_change().dropna() if len(equity) > 1 else pd.Series(dtype=float)
        return perf.build_report(equity, returns, self._trade_pnls, risk_free_rate)

    # ── manual controls ────────────────────────────────────────────

    def kill(self, reason: str = "Manual kill", by: str = "operator") -> None:
        self.controls.manual_kill(reason, by)

    def halt(self, reason: str = "Manual halt") -> None:
        self.controls.manual_halt(reason)

    def resume(self, by: str = "operator") -> None:
        self.controls.manual_resume(by)

    def full_reset(self, by: str = "operator") -> None:
        self.controls.full_reset(by)

    # ── reporting ──────────────────────────────────────────────────

    def print_full_status(self) -> None:
        print("\n" + "=" * 64)
        print(f"  MASTER RISK MANAGER v{self.VERSION} | "
              f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC")
        print("=" * 64)
        self.controls.print_status()
        if self._latest_snapshot:
            self.rt_monitor.print_snapshot(self._latest_snapshot)
        self.controls.audit.print_recent(8)

    def generate_risk_report(self) -> Dict:
        with self._lock:
            s = self._latest_snapshot
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "version": self.VERSION,
                "controls": self.controls.status(),
                "pnl": self.rt_monitor.pnl_tracker.summary(),
                "snapshot": {
                    "gross_notional": s.gross_notional if s else 0,
                    "net_notional": s.net_notional if s else 0,
                    "unrealised_pnl": s.unrealised_pnl if s else 0,
                    "var_95": s.portfolio_var_95 if s else 0,
                    "drawdown_pct": s.current_drawdown if s else 0,
                    "margin_ratio": s.margin_ratio if s else 0,
                },
                "positions": [
                    {"symbol": p.symbol, "quantity": p.quantity,
                     "notional": p.notional, "unrealised_pnl": p.unrealised_pnl}
                    for p in self.rt_monitor.positions],
            }

    # ── internal ───────────────────────────────────────────────────

    def _on_state_change(self, event: ControlEvent) -> None:
        if event.state in (ControlState.HALTED, ControlState.KILLED):
            logger.critical("TRADING HALTED -- %s", event.message)

    def _cancel_all_orders(self) -> None:
        """Replace with your broker's cancel-all API in production."""
        logger.critical("KILL SWITCH: cancel_all_orders() called (wire your broker here)")


# ════════════════════════════════════════════════════════════════════
# Interactive setup
# ════════════════════════════════════════════════════════════════════

def interactive_setup() -> MasterRiskManager:
    print("\n" + "=" * 60)
    print("  ALGORITHMIC TRADING -- RISK MANAGEMENT SETUP")
    print("=" * 60)

    def _f(prompt, default):
        raw = input(f"{prompt} [{default}]: ").strip()
        return float(raw) if raw else default

    capital = _f("\nTotal portfolio capital ($)", 1_000_000)
    cfg = RiskManagerConfig(initial_capital=capital)

    print("\n--- Circuit Breaker Thresholds (fractions, e.g. 0.10 = 10%) ---")
    cfg.breaker_drawdown_halt = _f("Drawdown HALT", 0.10)
    cfg.breaker_drawdown_kill = _f("Drawdown KILL", 0.20)
    cfg.breaker_daily_loss_halt = _f("Daily loss HALT", 0.05)
    cfg.breaker_daily_loss_kill = _f("Daily loss KILL", 0.10)

    print("\n--- Order Throttle ---")
    cfg.throttle_max_orders_per_second = _f("Max orders/second", 10)
    cfg.throttle_max_notional_per_day = _f("Max notional/day ($)", 50_000_000)

    restricted = input("\nRestricted symbols (comma-separated) []: ").strip()
    cfg.risk_limits = {"restricted_symbols":
                       [s.strip().upper() for s in restricted.split(",") if s.strip()]}

    save = input("\nSave config to file (blank to skip): ").strip()
    rm = MasterRiskManager(cfg)
    if save:
        cfg.save(save)
        print(f"Config saved to {save}")
    return rm


# ════════════════════════════════════════════════════════════════════
# Demo
# ════════════════════════════════════════════════════════════════════

def run_demo() -> None:
    print("\n" + "=" * 64)
    print("  RISK MANAGEMENT DEMO (v2)")
    print("=" * 64)

    cfg = RiskManagerConfig(initial_capital=1_000_000,
                            log_level="WARNING", log_file=None)
    rm = MasterRiskManager(cfg)

    rng = np.random.default_rng(7)
    dates = pd.date_range("2023-01-01", periods=300, freq="B")
    syms = ["AAPL", "MSFT", "GOOGL", "BTC", "EURUSD"]
    df = pd.DataFrame(rng.normal(0.0005, 0.015, (300, len(syms))),
                      index=dates, columns=syms)
    df["MSFT"] = df["AAPL"] * 0.9 + df["MSFT"] * 0.1  # force high correlation
    bench = pd.Series(rng.normal(0.0004, 0.011, 300), index=dates)
    rm.load_returns_history(df, bench)

    rm.load_positions([
        Position("AAPL", 500, 180.0, "equity", current_price=185.0),
        Position("MSFT", 300, 320.0, "equity", current_price=310.0),
        Position("EURUSD", 50_000, 1.08, "fx", current_price=1.079),
    ])

    print("\n[1] Initial tick")
    rm.rt_monitor.print_snapshot(rm.tick({"AAPL": 185, "MSFT": 310, "EURUSD": 1.079}))

    print("\n[2] Position sizing")
    print("   ATR size (ATR=500):           ", rm.size_by_atr(500))
    print("   Vol-target ($185, vol 25%):   ", rm.size_by_volatility_target(0.25, 185))
    print("   Signal tanh (max 100, s=0.5): ", rm.size_by_signal(100, 0.5, "tanh"))

    print("\n[3] Market-making quote (long inventory skews down)")
    q = rm.make_quote(100.0, inventory=40, time_elapsed=0.2)
    print(f"   bid={q.bid:.4f} ask={q.ask:.4f} reservation={q.reservation_price:.4f}")

    print("\n[4] Pre-trade checks")
    rm.pre_trade.limits.restricted_symbols = ["XYZ"]
    for o, ref in [(TradeOrder("AAPL", "BUY", 100, 185, "equity"), 185),
                   (TradeOrder("TSLA", "BUY", 5000, 250, "equity"), 250),
                   (TradeOrder("XYZ", "SELL", 100, 50, "equity"), 50),
                   (TradeOrder("AAPL", "BUY", 50, 250, "equity"), 185)]:
        ok, reason, _ = rm.approve_order(o, reference_price=ref)
        print(f"   {'OK ' if ok else 'BLK'} {o.symbol:7} {o.side:4} -> {reason[:70]}")

    print("\n[5] Stress tests")
    StressTester.print_report(rm.run_stress_tests()[:4])

    print("\n[6] Circuit breaker @ 12% drawdown")
    rm.controls.update_risk_metrics(drawdown_pct=0.12)
    print("   state:", rm.controls.circuit_breaker.state.name,
          "| order:", rm.controls.check_order(10_000))

    print("\n[7] Kill switch + reset")
    rm.kill("demo test", by="demo")
    print("   after kill:", rm.controls.check_order(1000))
    rm.full_reset(by="demo")
    rm.on_market_data()
    print("   after reset:", rm.controls.check_order(1000))

    print("\nDemo complete.\n")


# ════════════════════════════════════════════════════════════════════
# Entry point
# ════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="Algo Trading Risk Management")
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--config", type=str)
    args = parser.parse_args()

    if args.demo:
        setup_logging("WARNING", None)
        run_demo()
    elif args.config:
        rm = MasterRiskManager(RiskManagerConfig.load(args.config))
        rm.print_full_status()
    else:
        rm = interactive_setup()
        rm.print_full_status()


if __name__ == "__main__":
    main()
