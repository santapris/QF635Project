"""Strategy base class.

The :class:`AbstractStrategy` defines six handlers — five for events and
one for lifecycle. All have safe no-op defaults so a concrete strategy
overrides only the handlers it cares about.

Handlers return ``list[SignalEvent]``. The registry collects them and
publishes to the bus. Strategies must not import the bus or the OMS;
the only output of a strategy is its return value.

Per the architecture's reproducibility rule: strategies must derive
all timing from ``ctx.clock`` and never call wall-clock APIs directly.
"""

from __future__ import annotations

from abc import ABC

from ..core.events import (
    FillEvent,
    OrderBookEvent,
    PositionUpdateEvent,
    SignalEvent,
    TickEvent,
    TradeEvent,
)
from ..core.instruments import Instrument
from ..core.types import StrategyId
from .context import StrategyContext


class AbstractStrategy(ABC):
    """Base class for all trading strategies.

    Subclasses set ``strategy_id`` and ``instruments`` either as class
    attributes or via ``__init__``, then override the handlers they need.
    """

    strategy_id: StrategyId
    instruments: list[Instrument]

    def __init__(
        self,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
    ) -> None:
        self.strategy_id = strategy_id
        self.instruments = list(instruments)

    # --- Lifecycle --------------------------------------------------------

    async def on_start(self, ctx: StrategyContext) -> None:
        """Called once before any event is dispatched. Override for setup."""

    async def on_stop(self, ctx: StrategyContext) -> None:
        """Called once during shutdown. Override to flush state."""

    # --- Event handlers ---------------------------------------------------
    # Every handler gets the same shape so the registry can dispatch
    # uniformly. Default implementations return no signals.

    async def on_tick(
        self, event: TickEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        return []

    async def on_trade(
        self, event: TradeEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        return []

    async def on_book(
        self, event: OrderBookEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        return []

    async def on_fill(
        self, event: FillEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        return []

    async def on_position_update(
        self, event: PositionUpdateEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        return []

    # --- Helpers ----------------------------------------------------------

    def trades_instrument(self, instrument: Instrument) -> bool:
        """True if this strategy subscribes to events on ``instrument``."""
        return any(i.instrument_id == instrument.instrument_id for i in self.instruments)


__all__ = ["AbstractStrategy"]
