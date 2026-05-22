"""PortfolioView adapter.

The strategy registry (Batch 4) accepts any object satisfying the
:class:`~trading.strategy.context.PortfolioView` protocol. This adapter
exposes the position engine through that interface — letting the same
strategies run against either the test ``StaticPortfolioView`` or this
production view, with no code changes.

It's a thin wrapper, not a re-implementation. The engine is the single
source of truth; the adapter just reshapes its API.
"""

from __future__ import annotations

from ..core.instruments import Instrument
from ..core.positions import Position
from ..core.types import StrategyId
from .engine import PositionEngine


class EnginePortfolioView:
    """PortfolioView backed by a live :class:`PositionEngine`."""

    def __init__(self, engine: PositionEngine) -> None:
        self._engine = engine

    def get_position(
        self, instrument: Instrument, strategy_id: StrategyId
    ) -> Position | None:
        return self._engine.get_position(strategy_id, instrument)

    def get_positions(
        self, strategy_id: StrategyId
    ) -> dict[Instrument, Position]:
        return self._engine.get_positions(strategy_id)


__all__ = ["EnginePortfolioView"]
