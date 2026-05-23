"""Strategy execution context.

Every handler call receives a :class:`StrategyContext`. It exposes the
*minimum* surface a strategy needs to do its job:

- :attr:`clock` — for any time-of-day logic (always go through this, not
  ``time.time()`` or ``datetime.now()`` — see core/clock.py).
- :attr:`portfolio` — read-only view of current positions.
- :attr:`logger` — per-strategy structured logger.
- :attr:`parameters` — hot-reloadable knobs (key/value).

Crucially the context does **not** expose the bus, the OMS, or any way
to send orders directly. Strategies emit signals by returning them from
their handlers; the registry is the only thing that talks to the bus.
That separation is what keeps strategies pure and replayable.
"""

from __future__ import annotations

import structlog
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from ..core.clock import Clock
from ..core.instruments import Instrument
from ..core.positions import Position
from ..core.types import StrategyId


@runtime_checkable
class PortfolioView(Protocol):
    """Read-only view over current positions.

    The concrete implementation lives in ``position/`` (Batch 6). Until
    then, :class:`StaticPortfolioView` provides a useful test stub.
    """

    def get_position(
        self, instrument: Instrument, strategy_id: StrategyId
    ) -> Position | None:
        """Position for ``instrument`` held by ``strategy_id``, if any."""
        ...

    def get_positions(
        self, strategy_id: StrategyId
    ) -> dict[Instrument, Position]:
        """All non-flat positions for ``strategy_id``."""
        ...


class StaticPortfolioView:
    """Test/backtest stub — positions provided up front, not derived from fills.

    Production code uses the position engine's real implementation.
    """

    def __init__(
        self,
        positions: Mapping[tuple[StrategyId, Instrument], Position] | None = None,
    ) -> None:
        self._positions: dict[tuple[StrategyId, Instrument], Position] = dict(
            positions or {}
        )

    def get_position(
        self, instrument: Instrument, strategy_id: StrategyId
    ) -> Position | None:
        return self._positions.get((strategy_id, instrument))

    def get_positions(
        self, strategy_id: StrategyId
    ) -> dict[Instrument, Position]:
        return {
            inst: pos
            for (sid, inst), pos in self._positions.items()
            if sid == strategy_id and not pos.is_flat
        }

    def set_position(self, position: Position) -> None:
        """Test helper — install a position for inspection by handlers."""
        self._positions[(position.strategy_id, position.instrument)] = position


@dataclass(frozen=True, slots=True)
class StrategyContext:
    """Per-handler-call context. Frozen — handlers must not mutate it."""

    strategy_id: StrategyId
    clock: Clock
    portfolio: PortfolioView
    logger: structlog.BoundLogger
    parameters: Mapping[str, Any]

    def get_param(self, key: str, default: Any = None) -> Any:
        return self.parameters.get(key, default)


__all__ = ["PortfolioView", "StaticPortfolioView", "StrategyContext"]
