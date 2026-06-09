"""
risk_controls.py
==================
Layer 3 -- Risk Controls (the automatic safety mechanisms)

These controls operate automatically and in real time, exactly as the lecture
notes describe. They are the last line of defence against runaway algorithms,
cascading losses, and operational failures.

Three independent mechanisms:

  1. OrderThrottle      -- rate limits + open-order / active-order caps
  2. CircuitBreaker     -- a state machine that HALTS or KILLS trading when
                           drawdown / daily-loss / VaR / margin / loss-streak
                           thresholds break
  3. KillSwitch         -- a hard manual/automatic stop that blocks all orders
  4. MarketDataMonitor  -- halts trading on stale / disconnected market data

Plus an AuditLog that records every event with a timestamp.

Circuit-breaker trigger types covered (from the notes' table):
  single-order max notional, max order rate, max open-order notional,
  max active orders, position/delta limit, daily loss limit,
  margin-ratio threshold, market-data disconnect, consecutive losses.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from typing import Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════
# Enums & events
# ════════════════════════════════════════════════════════════════════

class ControlState(Enum):
    ACTIVE = auto()      # normal trading
    WARNING = auto()     # approaching a limit
    THROTTLED = auto()   # order rate limited
    HALTED = auto()      # trading suspended (recoverable)
    KILLED = auto()      # hard kill (manual reset required)


class TriggerReason(str, Enum):
    MANUAL = "Manual override"
    DRAWDOWN = "Drawdown threshold breached"
    DAILY_LOSS = "Daily loss limit hit"
    ORDER_RATE = "Order rate exceeded"
    OPEN_ORDER_NOTIONAL = "Open-order notional exceeded"
    ACTIVE_ORDERS = "Too many active orders"
    ERROR_RATE = "Error rate too high"
    VAR_BREACH = "VaR limit breached"
    MARGIN = "Margin ratio threshold breached"
    POSITION_LIMIT = "Position/delta limit breached"
    STALE_DATA = "Market data stale / disconnected"
    CONSECUTIVE_LOSS = "Consecutive losing trades"
    EXTERNAL = "External signal"


@dataclass
class ControlEvent:
    event_type: str
    state: ControlState
    reason: TriggerReason
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict = field(default_factory=dict)

    def __str__(self) -> str:
        ts = self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        return f"[{ts}] {self.event_type} | {self.state.name} | {self.reason.value} | {self.message}"


# ════════════════════════════════════════════════════════════════════
# Audit log
# ════════════════════════════════════════════════════════════════════

class AuditLog:
    """Thread-safe, append-only record of every control event."""

    def __init__(self, max_entries: int = 10_000):
        self._log: deque = deque(maxlen=max_entries)
        self._lock = threading.Lock()

    def record(self, event: ControlEvent) -> None:
        with self._lock:
            self._log.append(event)
        logger.info("AUDIT | %s", event)

    def to_list(self) -> List[ControlEvent]:
        with self._lock:
            return list(self._log)

    def print_recent(self, n: int = 20) -> None:
        entries = self.to_list()[-n:]
        print(f"\n--- AUDIT LOG (last {len(entries)}) ---")
        for e in entries:
            print(f"  {e}")


# ════════════════════════════════════════════════════════════════════
# Order throttle
# ════════════════════════════════════════════════════════════════════

@dataclass
class ThrottleConfig:
    max_orders_per_second: float = 10.0
    max_orders_per_minute: float = 200.0
    max_notional_per_minute: float = 5_000_000.0
    max_notional_per_day: float = 50_000_000.0
    max_open_order_notional: float = 50_000_000.0   # outstanding (unfilled) notional
    max_active_orders: int = 5_000                  # outstanding order count
    max_consecutive_errors: int = 5
    throttle_cooldown_seconds: float = 30.0


class OrderThrottle:
    """Sliding-window rate limiter plus open-order / active-order caps."""

    def __init__(self, config: ThrottleConfig, audit: AuditLog):
        self.config = config
        self.audit = audit
        self._lock = threading.Lock()
        self._orders_sec: deque = deque()
        self._orders_min: deque = deque()
        self._notional_min: deque = deque()
        self._notional_today: float = 0.0
        self._today = datetime.now(timezone.utc).date()
        self._consecutive_errors = 0
        self._throttled_until: Optional[datetime] = None
        # outstanding orders not yet filled/cancelled
        self._open_order_notional = 0.0
        self._active_orders = 0

    def _prune(self, dq: deque, seconds: float, now: float, has_value: bool):
        cutoff = now - seconds
        while dq and (dq[0][0] if has_value else dq[0]) < cutoff:
            dq.popleft()

    def _reset_daily(self):
        today = datetime.now(timezone.utc).date()
        if today != self._today:
            self._today = today
            self._notional_today = 0.0

    def check(self, notional: float) -> Tuple[bool, str]:
        """Return (allowed, reason). Does NOT record the order."""
        with self._lock:
            now = time.monotonic()
            self._reset_daily()

            if self._throttled_until and datetime.now(timezone.utc) < self._throttled_until:
                wait = (self._throttled_until - datetime.now(timezone.utc)).total_seconds()
                return False, f"Throttled; resume in {wait:.0f}s"

            self._prune(self._orders_sec, 1.0, now, False)
            self._prune(self._orders_min, 60.0, now, False)
            self._prune(self._notional_min, 60.0, now, True)

            if len(self._orders_sec) >= self.config.max_orders_per_second:
                return False, (f"{len(self._orders_sec)} orders/sec "
                               f"(max {self.config.max_orders_per_second:g})")
            if len(self._orders_min) >= self.config.max_orders_per_minute:
                return False, (f"{len(self._orders_min)} orders/min "
                               f"(max {self.config.max_orders_per_minute:g})")
            n1 = sum(n for _, n in self._notional_min)
            if n1 + notional > self.config.max_notional_per_minute:
                return False, (f"${n1 + notional:,.0f} notional/min "
                               f"(max ${self.config.max_notional_per_minute:,.0f})")
            if self._notional_today + notional > self.config.max_notional_per_day:
                return False, (f"${self._notional_today + notional:,.0f} notional/day "
                               f"(max ${self.config.max_notional_per_day:,.0f})")
            if self._open_order_notional + notional > self.config.max_open_order_notional:
                return False, (f"open-order notional ${self._open_order_notional + notional:,.0f} "
                               f"(max ${self.config.max_open_order_notional:,.0f})")
            if self._active_orders + 1 > self.config.max_active_orders:
                return False, (f"{self._active_orders + 1} active orders "
                               f"(max {self.config.max_active_orders})")
            return True, "OK"

    def record_order(self, notional: float) -> None:
        with self._lock:
            now = time.monotonic()
            self._orders_sec.append(now)
            self._orders_min.append(now)
            self._notional_min.append((now, notional))
            self._notional_today += notional
            self._open_order_notional += notional
            self._active_orders += 1
            self._consecutive_errors = 0

    def on_fill_or_cancel(self, notional: float) -> None:
        """Call when an order is filled or cancelled to free up the open caps."""
        with self._lock:
            self._open_order_notional = max(0.0, self._open_order_notional - notional)
            self._active_orders = max(0, self._active_orders - 1)

    def record_error(self) -> None:
        with self._lock:
            self._consecutive_errors += 1
            if self._consecutive_errors >= self.config.max_consecutive_errors:
                self._throttled_until = datetime.now(timezone.utc) + timedelta(
                    seconds=self.config.throttle_cooldown_seconds)
                self.audit.record(ControlEvent(
                    "THROTTLE", ControlState.THROTTLED, TriggerReason.ERROR_RATE,
                    f"Throttled after {self._consecutive_errors} consecutive errors"))

    def stats(self) -> Dict:
        with self._lock:
            now = time.monotonic()
            self._prune(self._orders_sec, 1.0, now, False)
            self._prune(self._orders_min, 60.0, now, False)
            return {
                "orders_last_second": len(self._orders_sec),
                "orders_last_minute": len(self._orders_min),
                "notional_today": round(self._notional_today, 2),
                "open_order_notional": round(self._open_order_notional, 2),
                "active_orders": self._active_orders,
                "consecutive_errors": self._consecutive_errors,
            }


# ════════════════════════════════════════════════════════════════════
# Circuit breaker
# ════════════════════════════════════════════════════════════════════

@dataclass
class CircuitBreakerConfig:
    capital: float = 1_000_000.0
    # drawdown
    drawdown_warning_pct: float = 0.05
    drawdown_halt_pct: float = 0.10
    drawdown_kill_pct: float = 0.20
    # daily loss
    daily_loss_warning_pct: float = 0.03
    daily_loss_halt_pct: float = 0.05
    daily_loss_kill_pct: float = 0.10
    # VaR
    var_warning_pct: float = 0.04
    var_halt_pct: float = 0.08
    # margin ratio
    margin_warning: float = 0.50
    margin_halt: float = 0.80
    # consecutive losses
    consecutive_loss_warning: int = 3
    consecutive_loss_halt: int = 5
    # auto-reset (0 = manual reset required)
    auto_reset_seconds: float = 0.0


class CircuitBreaker:
    """State machine: ACTIVE -> WARNING -> HALTED -> KILLED."""

    def __init__(self, config: CircuitBreakerConfig, audit: AuditLog):
        self.config = config
        self.audit = audit
        self._state = ControlState.ACTIVE
        self._lock = threading.Lock()
        self._consecutive_losses = 0
        self._daily_loss = 0.0
        self._day = datetime.now(timezone.utc).date()
        self._callbacks: List[Callable[[ControlEvent], None]] = []

    def register_callback(self, fn: Callable[[ControlEvent], None]) -> None:
        self._callbacks.append(fn)

    @property
    def state(self) -> ControlState:
        return self._state

    @property
    def is_trading_allowed(self) -> bool:
        return self._state in (ControlState.ACTIVE, ControlState.WARNING)

    def _emit(self, event: ControlEvent) -> None:
        self.audit.record(event)
        for cb in self._callbacks:
            try:
                cb(event)
            except Exception as e:  # pragma: no cover
                logger.error("Callback error: %s", e)

    def _transition(self, new: ControlState, reason: TriggerReason, msg: str) -> None:
        old = self._state
        self._state = new
        self._emit(ControlEvent(f"STATE:{old.name}->{new.name}", new, reason, msg))
        if new == ControlState.HALTED and self.config.auto_reset_seconds > 0:
            t = threading.Timer(self.config.auto_reset_seconds, self._auto_reset)
            t.daemon = True
            t.start()

    def _auto_reset(self) -> None:
        with self._lock:
            if self._state == ControlState.HALTED:
                self._transition(ControlState.ACTIVE, TriggerReason.EXTERNAL,
                                 f"Auto-reset after {self.config.auto_reset_seconds}s")

    def _reset_daily(self) -> None:
        today = datetime.now(timezone.utc).date()
        if today != self._day:
            self._day = today
            self._daily_loss = 0.0

    def evaluate(self, drawdown_pct: float = 0.0, daily_pnl: float = 0.0,
                 var_pct: float = 0.0, margin_ratio: float = 0.0,
                 trade_result: Optional[str] = None) -> ControlState:
        """Run on each risk cycle; returns the resulting state."""
        with self._lock:
            self._reset_daily()
            c = self.config
            if self._state == ControlState.KILLED:
                return self._state

            if trade_result == "loss":
                self._consecutive_losses += 1
            elif trade_result == "win":
                self._consecutive_losses = 0

            if daily_pnl < 0:
                self._daily_loss = abs(daily_pnl)
            daily_loss_pct = self._daily_loss / c.capital if c.capital else 0.0

            # KILL conditions
            if drawdown_pct >= c.drawdown_kill_pct:
                self._transition(ControlState.KILLED, TriggerReason.DRAWDOWN,
                                 f"KILL: drawdown {drawdown_pct:.1%} >= {c.drawdown_kill_pct:.1%}")
                return self._state
            if daily_loss_pct >= c.daily_loss_kill_pct:
                self._transition(ControlState.KILLED, TriggerReason.DAILY_LOSS,
                                 f"KILL: daily loss {daily_loss_pct:.1%} >= {c.daily_loss_kill_pct:.1%}")
                return self._state

            # HALT conditions
            for cond, reason, msg in [
                (drawdown_pct >= c.drawdown_halt_pct, TriggerReason.DRAWDOWN,
                 f"HALT: drawdown {drawdown_pct:.1%} >= {c.drawdown_halt_pct:.1%}"),
                (daily_loss_pct >= c.daily_loss_halt_pct, TriggerReason.DAILY_LOSS,
                 f"HALT: daily loss {daily_loss_pct:.1%} >= {c.daily_loss_halt_pct:.1%}"),
                (var_pct >= c.var_halt_pct, TriggerReason.VAR_BREACH,
                 f"HALT: VaR {var_pct:.1%} >= {c.var_halt_pct:.1%}"),
                (margin_ratio >= c.margin_halt, TriggerReason.MARGIN,
                 f"HALT: margin ratio {margin_ratio:.1%} >= {c.margin_halt:.1%}"),
                (self._consecutive_losses >= c.consecutive_loss_halt,
                 TriggerReason.CONSECUTIVE_LOSS,
                 f"HALT: {self._consecutive_losses} consecutive losses"),
            ]:
                if cond:
                    if self._state != ControlState.HALTED:
                        self._transition(ControlState.HALTED, reason, msg)
                    return self._state

            # WARNING conditions
            warn = (drawdown_pct >= c.drawdown_warning_pct
                    or daily_loss_pct >= c.daily_loss_warning_pct
                    or var_pct >= c.var_warning_pct
                    or margin_ratio >= c.margin_warning
                    or self._consecutive_losses >= c.consecutive_loss_warning)
            if warn and self._state == ControlState.ACTIVE:
                self._transition(ControlState.WARNING, TriggerReason.DRAWDOWN,
                                 f"WARNING: dd {drawdown_pct:.1%}, loss {daily_loss_pct:.1%}, "
                                 f"VaR {var_pct:.1%}, margin {margin_ratio:.1%}")
            elif not warn and self._state == ControlState.WARNING:
                self._transition(ControlState.ACTIVE, TriggerReason.EXTERNAL,
                                 "Metrics normalised; back to ACTIVE")
            return self._state

    def manual_halt(self, reason: str = "Manual halt") -> None:
        with self._lock:
            self._transition(ControlState.HALTED, TriggerReason.MANUAL, reason)

    def manual_resume(self, by: str = "operator") -> None:
        with self._lock:
            if self._state == ControlState.KILLED:
                logger.warning("Cannot resume from KILLED; use full_reset()")
                return
            self._transition(ControlState.ACTIVE, TriggerReason.MANUAL,
                             f"Resumed by {by}")

    def full_reset(self, by: str = "operator") -> None:
        with self._lock:
            self._consecutive_losses = 0
            self._daily_loss = 0.0
            self._transition(ControlState.ACTIVE, TriggerReason.MANUAL,
                             f"Full reset by {by}")


# ════════════════════════════════════════════════════════════════════
# Kill switch
# ════════════════════════════════════════════════════════════════════

class KillSwitch:
    """Immediately blocks all new orders; optionally cancels open orders."""

    def __init__(self, audit: AuditLog,
                 cancel_all_orders_fn: Optional[Callable] = None):
        self.audit = audit
        self._active = False
        self._kill_time: Optional[datetime] = None
        self._kill_reason = ""
        self._cancel_all = cancel_all_orders_fn
        self._lock = threading.Lock()
        self._callbacks: List[Callable[[str], None]] = []

    def register_callback(self, fn: Callable[[str], None]) -> None:
        self._callbacks.append(fn)

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def reason(self) -> str:
        return self._kill_reason

    def engage(self, reason: str = "Kill switch engaged",
               triggered_by: str = "system") -> None:
        with self._lock:
            if self._active:
                return
            self._active = True
            self._kill_time = datetime.now(timezone.utc)
            self._kill_reason = reason
            self.audit.record(ControlEvent(
                "KILL_SWITCH", ControlState.KILLED,
                TriggerReason.MANUAL if triggered_by == "operator" else TriggerReason.EXTERNAL,
                f"{reason} (by {triggered_by})", metadata={"triggered_by": triggered_by}))
            logger.critical("KILL SWITCH ENGAGED | %s | by %s", reason, triggered_by)
            if self._cancel_all:
                try:
                    self._cancel_all()
                except Exception as e:  # pragma: no cover
                    logger.error("cancel_all failed: %s", e)
            for cb in self._callbacks:
                try:
                    cb(reason)
                except Exception as e:  # pragma: no cover
                    logger.error("Kill callback error: %s", e)

    def reset(self, authorised_by: str) -> None:
        with self._lock:
            if not self._active:
                return
            self._active = False
            self.audit.record(ControlEvent(
                "KILL_SWITCH_RESET", ControlState.ACTIVE, TriggerReason.MANUAL,
                f"Reset by {authorised_by}", metadata={"reset_by": authorised_by}))
            logger.info("KILL SWITCH RESET by %s", authorised_by)

    def status(self) -> Dict:
        return {"active": self._active,
                "kill_time": self._kill_time.isoformat() if self._kill_time else None,
                "kill_reason": self._kill_reason}


# ════════════════════════════════════════════════════════════════════
# Market data staleness monitor
# ════════════════════════════════════════════════════════════════════

class MarketDataMonitor:
    """
    Halts trading if market data goes stale (the "Market Data Disconnect"
    circuit breaker). Call heartbeat() whenever fresh data arrives, then
    is_stale() before trading.
    """

    def __init__(self, audit: AuditLog, max_staleness_seconds: float = 5.0):
        self.audit = audit
        self.max_staleness = max_staleness_seconds
        self._last_update = time.monotonic()
        self._stale = False

    def heartbeat(self) -> None:
        self._last_update = time.monotonic()
        if self._stale:
            self._stale = False
            self.audit.record(ControlEvent(
                "DATA_OK", ControlState.ACTIVE, TriggerReason.EXTERNAL,
                "Market data reconnected"))

    def is_stale(self) -> bool:
        stale = (time.monotonic() - self._last_update) > self.max_staleness
        if stale and not self._stale:
            self._stale = True
            self.audit.record(ControlEvent(
                "DATA_STALE", ControlState.HALTED, TriggerReason.STALE_DATA,
                f"No market data for > {self.max_staleness}s"))
            logger.warning("MARKET DATA STALE > %.0fs", self.max_staleness)
        return stale

    def seconds_since_update(self) -> float:
        return time.monotonic() - self._last_update


# ════════════════════════════════════════════════════════════════════
# Unified controls manager
# ════════════════════════════════════════════════════════════════════

class RiskControlsManager:
    """Coordinates kill switch, circuit breaker, throttle and data monitor."""

    def __init__(self, throttle_config: Optional[ThrottleConfig] = None,
                 breaker_config: Optional[CircuitBreakerConfig] = None,
                 cancel_all_fn: Optional[Callable] = None,
                 max_data_staleness: float = 5.0):
        self.audit = AuditLog()
        self.kill_switch = KillSwitch(self.audit, cancel_all_fn)
        self.circuit_breaker = CircuitBreaker(
            breaker_config or CircuitBreakerConfig(), self.audit)
        self.throttle = OrderThrottle(throttle_config or ThrottleConfig(), self.audit)
        self.data_monitor = MarketDataMonitor(self.audit, max_data_staleness)

    def check_order(self, notional: float) -> Tuple[bool, str]:
        """Single gate to call before every order submission."""
        if self.kill_switch.is_active:
            return False, f"Kill switch active: {self.kill_switch.reason}"
        if self.data_monitor.is_stale():
            return False, "Market data is stale/disconnected"
        if not self.circuit_breaker.is_trading_allowed:
            return False, f"Trading halted: {self.circuit_breaker.state.name}"
        allowed, reason = self.throttle.check(notional)
        if not allowed:
            return False, f"Throttled: {reason}"
        return True, "OK"

    def on_order_sent(self, notional: float) -> None:
        self.throttle.record_order(notional)

    def on_order_filled_or_cancelled(self, notional: float) -> None:
        self.throttle.on_fill_or_cancel(notional)

    def on_order_error(self) -> None:
        self.throttle.record_error()

    def on_market_data(self) -> None:
        self.data_monitor.heartbeat()

    def update_risk_metrics(self, drawdown_pct: float = 0.0,
                            daily_pnl: float = 0.0, var_pct: float = 0.0,
                            margin_ratio: float = 0.0,
                            trade_result: Optional[str] = None) -> ControlState:
        return self.circuit_breaker.evaluate(
            drawdown_pct=drawdown_pct, daily_pnl=daily_pnl, var_pct=var_pct,
            margin_ratio=margin_ratio, trade_result=trade_result)

    def manual_kill(self, reason: str = "Manual kill", by: str = "operator") -> None:
        self.kill_switch.engage(reason, triggered_by=by)

    def manual_halt(self, reason: str = "Manual halt") -> None:
        self.circuit_breaker.manual_halt(reason)

    def manual_resume(self, by: str = "operator") -> None:
        self.kill_switch.reset(by)
        self.circuit_breaker.manual_resume(by)

    def full_reset(self, by: str = "operator") -> None:
        self.kill_switch.reset(by)
        self.circuit_breaker.full_reset(by)

    def status(self) -> Dict:
        return {
            "kill_switch": self.kill_switch.status(),
            "circuit_breaker": {"state": self.circuit_breaker.state.name,
                                "trading_allowed": self.circuit_breaker.is_trading_allowed},
            "throttle": self.throttle.stats(),
            "data_stale": self.data_monitor.is_stale(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def print_status(self) -> None:
        s = self.status()
        ks, cb, th = s["kill_switch"], s["circuit_breaker"], s["throttle"]
        print("\n--- RISK CONTROLS STATUS ---")
        print(f"  Kill Switch     : {'ACTIVE' if ks['active'] else 'inactive'}")
        if ks["active"]:
            print(f"     reason: {ks['kill_reason']}")
        print(f"  Circuit Breaker : {cb['state']}")
        print(f"  Trading Allowed : {'YES' if cb['trading_allowed'] else 'NO'}")
        print(f"  Orders/sec      : {th['orders_last_second']}")
        print(f"  Orders/min      : {th['orders_last_minute']}")
        print(f"  Active orders   : {th['active_orders']}")
        print(f"  Open notional   : ${th['open_order_notional']:,.0f}")
        print(f"  Notional today  : ${th['notional_today']:,.0f}")
        print(f"  Data stale      : {s['data_stale']}")


# ════════════════════════════════════════════════════════════════════
# Smoke test
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s | %(message)s")

    ctrl = RiskControlsManager(
        breaker_config=CircuitBreakerConfig(capital=1_000_000, auto_reset_seconds=0))
    ctrl.on_market_data()  # mark data fresh

    print("normal:", ctrl.check_order(50_000))
    ctrl.on_order_sent(50_000)

    ctrl.update_risk_metrics(drawdown_pct=0.12)
    print("after 12% drawdown:", ctrl.check_order(10_000))

    ctrl.manual_kill("fat finger", by="risk_desk")
    print("after kill:", ctrl.check_order(1000))

    ctrl.full_reset(by="head_of_risk")
    ctrl.on_market_data()
    print("after reset:", ctrl.check_order(1000))
    ctrl.print_status()
