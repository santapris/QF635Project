"""Strategy registry.

The registry is the only thing that talks to the bus on a strategy's
behalf. Strategies emit signals by *returning* them; the registry
collects and publishes. This separation means a strategy is a pure
function of the events it has seen — easy to test, easy to replay.

Responsibilities:

- Hold registered strategies, indexed by ``strategy_id`` and by the
  instruments each subscribes to.
- Subscribe to bus topics on ``start()``; unsubscribe on ``stop()``.
- Dispatch each event only to strategies that subscribe to that event's
  instrument.
- Catch handler exceptions per-strategy so one broken strategy cannot
  affect the others.
- Publish returned signals to the ``signals`` topic.
- Maintain per-strategy parameter dicts that can be hot-reloaded
  (in-memory; production wires this to Redis).
"""

from __future__ import annotations

import structlog
from collections import defaultdict
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from ..core.clock import Clock
from ..core.events import (
    BaseEvent,
    FillEvent,
    OrderBookEvent,
    PositionUpdateEvent,
    SignalEvent,
    StrategyDiagnosticsEvent,
    TickEvent,
    TradeEvent,
)
from ..core.exceptions import ConfigError
from ..core.types import StrategyId
from ..event_bus.base import AbstractEventBus, Topic
from .base import AbstractStrategy
from .context import PortfolioView, StrategyContext

_log = structlog.get_logger(__name__)


_DispatchHandler = Callable[
    [AbstractStrategy, BaseEvent, StrategyContext], Awaitable[list[SignalEvent]]
]


async def _on_tick(s: AbstractStrategy, e: BaseEvent, c: StrategyContext) -> list[SignalEvent]:
    return await s.on_tick(e, c)  # type: ignore[arg-type]


async def _on_trade(s: AbstractStrategy, e: BaseEvent, c: StrategyContext) -> list[SignalEvent]:
    return await s.on_trade(e, c)  # type: ignore[arg-type]


async def _on_book(s: AbstractStrategy, e: BaseEvent, c: StrategyContext) -> list[SignalEvent]:
    return await s.on_book(e, c)  # type: ignore[arg-type]


async def _on_fill(s: AbstractStrategy, e: BaseEvent, c: StrategyContext) -> list[SignalEvent]:
    return await s.on_fill(e, c)  # type: ignore[arg-type]


async def _on_position_update(
    s: AbstractStrategy, e: BaseEvent, c: StrategyContext
) -> list[SignalEvent]:
    return await s.on_position_update(e, c)  # type: ignore[arg-type]


class StrategyRegistry:
    """Manages strategies and routes bus events to them."""

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        clock: Clock,
        portfolio: PortfolioView,
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._portfolio = portfolio

        self._strategies: dict[StrategyId, AbstractStrategy] = {}
        self._instrument_index: dict[str, list[StrategyId]] = defaultdict(list)
        self._parameters: dict[StrategyId, dict[str, Any]] = {}
        self._loggers: dict[StrategyId, structlog.BoundLogger] = {}
        # Strategies whose signal dispatch is currently suppressed (operator
        # pause via the dashboard). Mutated only from the trading loop — the
        # dashboard marshals pause/resume calls onto it via
        # run_coroutine_threadsafe — so no lock is needed.
        self._paused: set[StrategyId] = set()
        self._started = False
        # Side-channel for tick→signal latency: signal.event_id → tick.ts_ingest.
        # Bounded to prevent growth if strategies emit signals faster than they're consumed.
        self._signal_tick_map: dict = {}
        self._SIGNAL_MAP_MAX = 128

    # --- Registration ------------------------------------------------------

    def register(
        self,
        strategy: AbstractStrategy,
        parameters: Mapping[str, Any] | None = None,
    ) -> None:
        """Add a strategy to the registry. Must be called before ``start()``."""
        if self._started:
            raise ConfigError("cannot register after start()")
        if strategy.strategy_id in self._strategies:
            raise ConfigError(
                f"strategy_id already registered: {strategy.strategy_id}"
            )
        self._strategies[strategy.strategy_id] = strategy
        self._parameters[strategy.strategy_id] = dict(parameters or {})
        self._loggers[strategy.strategy_id] = structlog.get_logger(
            f"strategy.{strategy.strategy_id}"
        )
        for instrument in strategy.instruments:
            self._instrument_index[instrument.instrument_id].append(
                strategy.strategy_id
            )

    def set_parameters(
        self, strategy_id: StrategyId, parameters: Mapping[str, Any]
    ) -> None:
        """Replace a strategy's parameter dict. Hot-reload entry point."""
        if strategy_id not in self._strategies:
            raise KeyError(strategy_id)
        self._parameters[strategy_id] = dict(parameters)

    def get(self, strategy_id: StrategyId) -> AbstractStrategy:
        return self._strategies[strategy_id]

    @property
    def strategy_ids(self) -> list[StrategyId]:
        return list(self._strategies)

    # --- Pause / resume ----------------------------------------------------
    # Operator controls surfaced on the dashboard. Pausing suppresses signal
    # dispatch for a strategy (see _invoke); the OMS-side cancellation of any
    # resting orders is driven separately by the dashboard so the strategy goes
    # fully quiet. Held inventory is untouched.

    def pause(self, strategy_id: StrategyId) -> None:
        """Suppress signal dispatch for a strategy. Idempotent."""
        if strategy_id not in self._strategies:
            raise KeyError(strategy_id)
        if strategy_id not in self._paused:
            self._paused.add(strategy_id)
            _log.warning("strategy_paused", strategy_id=strategy_id)

    def resume(self, strategy_id: StrategyId) -> None:
        """Re-enable signal dispatch for a paused strategy. Idempotent."""
        if strategy_id not in self._strategies:
            raise KeyError(strategy_id)
        if strategy_id in self._paused:
            self._paused.discard(strategy_id)
            _log.warning("strategy_resumed", strategy_id=strategy_id)

    def is_paused(self, strategy_id: StrategyId) -> bool:
        return strategy_id in self._paused

    @property
    def paused_ids(self) -> list[StrategyId]:
        return list(self._paused)

    @property
    def signal_tick_map(self) -> dict:
        """Shared reference used by LatencyCollector for tick→signal timing."""
        return self._signal_tick_map

    # --- Lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        # Subscribe to all relevant topics. One handler each — the
        # dispatch fan-out happens inside us.
        await self._bus.subscribe(Topic.MARKET_DATA, self._handle_market_data)
        await self._bus.subscribe(Topic.FILLS, self._handle_fill)
        await self._bus.subscribe(Topic.POSITIONS, self._handle_position_update)

        # Run on_start for each strategy. Errors here are config bugs;
        # we let them propagate so misconfiguration fails loud at boot.
        for sid, strategy in self._strategies.items():
            await strategy.on_start(self._make_context(sid))

    async def stop(self) -> None:
        if not self._started:
            return
        for sid, strategy in self._strategies.items():
            try:
                await strategy.on_stop(self._make_context(sid))
            except Exception:
                _log.exception("strategy_on_stop_raised", strategy_id=sid)
        self._started = False

    # --- Dispatch ----------------------------------------------------------

    async def _handle_market_data(self, event: BaseEvent) -> None:
        instrument = getattr(event, "instrument", None)
        if instrument is None:
            return  # Not a per-instrument event.
        if isinstance(event, TickEvent):
            await self._dispatch(event, instrument.instrument_id, _on_tick)
        elif isinstance(event, TradeEvent):
            await self._dispatch(event, instrument.instrument_id, _on_trade)
        elif isinstance(event, OrderBookEvent):
            await self._dispatch(event, instrument.instrument_id, _on_book)

    async def _handle_fill(self, event: BaseEvent) -> None:
        if not isinstance(event, FillEvent):
            return
        # Fills are routed by strategy_id — only the originating strategy
        # sees them. (Different strategies trade independently.)
        await self._dispatch_to_strategy(
            event, event.strategy_id, _on_fill
        )

    async def _handle_position_update(self, event: BaseEvent) -> None:
        if not isinstance(event, PositionUpdateEvent):
            return
        await self._dispatch_to_strategy(
            event, event.strategy_id, _on_position_update
        )

    async def _dispatch(
        self,
        event: BaseEvent,
        instrument_id: str,
        handler: _DispatchHandler,
    ) -> None:
        for sid in self._instrument_index.get(instrument_id, ()):
            await self._invoke(sid, event, handler)

    async def _dispatch_to_strategy(
        self,
        event: BaseEvent,
        strategy_id: StrategyId,
        handler: _DispatchHandler,
    ) -> None:
        if strategy_id in self._strategies:
            await self._invoke(strategy_id, event, handler)

    async def _invoke(
        self,
        sid: StrategyId,
        event: BaseEvent,
        handler: _DispatchHandler,
    ) -> None:
        # Paused strategies are skipped entirely: no signals, and no diagnostics
        # publish below. The strategy still receives no events, so it stays inert
        # until resumed (its accumulated internal state is preserved).
        if sid in self._paused:
            return
        strategy = self._strategies[sid]
        ctx = self._make_context(sid)
        try:
            signals = await handler(strategy, event, ctx)
        except Exception:
            _log.exception(
                "strategy_raised_in_handler_isolating",
                strategy_id=sid, event_type=type(event).__name__,
            )
            return
        for signal in signals or ():
            # Bounded insert for tick→signal latency tracking (zero model-copy overhead).
            if len(self._signal_tick_map) >= self._SIGNAL_MAP_MAX:
                self._signal_tick_map.pop(next(iter(self._signal_tick_map)))
            self._signal_tick_map[signal.event_id] = event.ts_ingest
            await self._bus.publish(Topic.SIGNALS, signal)

        if isinstance(event, TickEvent):
            diagnostics = strategy.get_strategy_diagnostics()
            if diagnostics is not None:
                try:
                    await self._bus.publish(Topic.ANALYTICS, StrategyDiagnosticsEvent(**diagnostics))
                except Exception:
                    _log.warning(
                        "strategy_diagnostics_publish_failed",
                        strategy_id=sid,
                        exc_info=True,
                    )

    # --- Helpers -----------------------------------------------------------

    def _make_context(self, sid: StrategyId) -> StrategyContext:
        return StrategyContext(
            strategy_id=sid,
            clock=self._clock,
            portfolio=self._portfolio,
            logger=self._loggers[sid],
            parameters=self._parameters[sid],
        )


__all__ = ["StrategyRegistry"]
