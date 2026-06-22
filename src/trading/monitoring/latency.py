"""LatencyCollector — per-stage internal pipeline latency measurement.

Measures four stages:

    tick → signal      Strategy processing time per tick.
                       Correlation: StrategyRegistry._signal_tick_map (shared dict reference).
    signal → decision  Risk evaluation time.
                       Correlation: RiskDecision.signal_event_id vs cached signal.ts_ingest.
    decision → order   OMS reconciliation time.
                       Correlation: OrderRequest.upstream_ts_ns (threaded by OMS).
    order → fill       Venue round-trip (external, not in our control).
                       Computation: FillEvent.ts_ingest - FillEvent.ts_event.

All measurements are rolling deques (configurable window). snapshot() returns
p50/p95/p99 in milliseconds. Handlers run concurrently in the asyncio event
loop — they never block the hot path.
"""

from __future__ import annotations

import structlog
from collections import deque
from typing import TYPE_CHECKING

from ..core.events import (
    BaseEvent,
    FillEvent,
    OrderRequest,
    RiskDecision,
    SignalEvent,
)
from ..event_bus.base import AbstractEventBus, Topic

if TYPE_CHECKING:
    pass

_log = structlog.get_logger(__name__)

_SIG_CACHE_MAX = 256


class LatencyCollector:
    """Collects per-stage pipeline latency statistics with near-zero hot-path overhead.

    Parameters
    ----------
    bus:
        The event bus to subscribe on.
    signal_tick_map:
        Shared dict reference from StrategyRegistry.signal_tick_map.
        Maps signal.event_id → tick.ts_ingest. The LatencyCollector pops
        entries as it consumes them — no model copies or extra allocations
        on the tick→signal hot path.
    window:
        Rolling sample window size per stage. Default 200.
    """

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        signal_tick_map: dict,
        window: int = 200,
    ) -> None:
        self._bus = bus
        self._signal_tick_map = signal_tick_map
        self._stages: dict[str, deque[float]] = {
            "tick_to_signal":     deque(maxlen=window),
            "signal_to_decision": deque(maxlen=window),
            "decision_to_order":  deque(maxlen=window),
            "order_to_fill":      deque(maxlen=window),
        }
        # Internal cache for signal→decision correlation.
        self._sig_cache: dict[object, int] = {}

    # --- Lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        await self._bus.subscribe(Topic.SIGNALS, self._on_signal)
        await self._bus.subscribe(Topic.RISK_DECISIONS, self._on_risk_decision)
        await self._bus.subscribe(Topic.ORDERS, self._on_order)
        await self._bus.subscribe(Topic.FILLS, self._on_fill)

    async def stop(self) -> None:
        pass  # AsyncioBus does not expose unsubscribe; lifecycle ends with the runner.

    # --- Event handlers ------------------------------------------------------

    async def _on_signal(self, event: BaseEvent) -> None:
        if not isinstance(event, SignalEvent):
            return
        # Cache for signal→decision correlation.
        if len(self._sig_cache) >= _SIG_CACHE_MAX:
            self._sig_cache.pop(next(iter(self._sig_cache)))
        self._sig_cache[event.event_id] = event.ts_ingest

        # Tick→signal: pop from the shared side dict (zero model-copy overhead).
        tick_ts = self._signal_tick_map.pop(event.event_id, None)
        if tick_ts is not None:
            delta = event.ts_ingest - tick_ts
            if delta >= 0:
                self._stages["tick_to_signal"].append(float(delta))

    async def _on_risk_decision(self, event: BaseEvent) -> None:
        if not isinstance(event, RiskDecision):
            return
        sig_ts = self._sig_cache.pop(event.signal_event_id, None)
        if sig_ts is not None:
            delta = event.ts_ingest - sig_ts
            if delta >= 0:
                self._stages["signal_to_decision"].append(float(delta))

    async def _on_order(self, event: BaseEvent) -> None:
        if not isinstance(event, OrderRequest):
            return
        if event.upstream_ts_ns is not None:
            delta = event.ts_ingest - event.upstream_ts_ns
            if delta >= 0:
                self._stages["decision_to_order"].append(float(delta))

    async def _on_fill(self, event: BaseEvent) -> None:
        if not isinstance(event, FillEvent):
            return
        if event.ts_event > 0:
            delta = event.ts_ingest - event.ts_event
            if delta >= 0:
                self._stages["order_to_fill"].append(float(delta))

    # --- Snapshot ------------------------------------------------------------

    def snapshot(self) -> dict:
        """Return per-stage latency percentiles in milliseconds.

        Returns None values for stages with no samples yet (cold start).
        Uses sorted(list(deque)) — GIL-atomic under CPython.
        """
        result: dict[str, dict] = {}
        for stage, dq in self._stages.items():
            samples = sorted(dq)
            n = len(samples)
            if n == 0:
                result[stage] = {"p50_ms": None, "p95_ms": None, "p99_ms": None, "count": 0}
            else:
                def _ms(ns: float) -> float:
                    return round(ns / 1_000_000, 4)
                result[stage] = {
                    "p50_ms": _ms(samples[int(0.50 * n)]),
                    "p95_ms": _ms(samples[min(int(0.95 * n), n - 1)]),
                    "p99_ms": _ms(samples[min(int(0.99 * n), n - 1)]),
                    "count": n,
                }
        return result


__all__ = ["LatencyCollector"]
