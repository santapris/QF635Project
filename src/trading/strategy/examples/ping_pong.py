"""Ping-pong strategy — alternates BUY/SELL on a fixed interval.

For end-to-end dashboard / demo use only. Ignores price action: every
``interval_seconds`` it flips side, so signals, orders, fills, positions
and PnL all update predictably.

Parameters (read from ``ctx.parameters`` so they hot-reload):

- ``target_quantity`` (Decimal as string, default ``"0.0001"``)
- ``interval_seconds`` (float, default ``10.0``)
"""

from __future__ import annotations

from decimal import Decimal

from ...core.events import SignalEvent, TickEvent
from ...core.instruments import Instrument
from ...core.types import OrderType, Side, StrategyId, TimeInForce
from ..base import AbstractStrategy
from ..context import StrategyContext


class PingPongStrategy(AbstractStrategy):
    """Alternates BUY/SELL every N seconds. Demo only — bleeds on fees."""

    def __init__(
        self,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        interval_seconds: float = 10.0,
    ) -> None:
        super().__init__(strategy_id=strategy_id, instruments=instruments)
        self._default_interval_ns = int(interval_seconds * 1e9)
        self._last_emit_ns: dict[str, int] = {
            i.instrument_id: 0 for i in instruments
        }
        self._next_side: dict[str, Side] = {
            i.instrument_id: Side.BUY for i in instruments
        }

    async def on_tick(
        self, event: TickEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        iid = event.instrument.instrument_id

        interval_seconds = float(ctx.get_param("interval_seconds", 10.0))
        interval_ns = int(interval_seconds * 1e9)

        if event.ts_event - self._last_emit_ns[iid] < interval_ns:
            return []

        self._last_emit_ns[iid] = event.ts_event
        side = self._next_side[iid]
        self._next_side[iid] = Side.SELL if side == Side.BUY else Side.BUY

        target_quantity = Decimal(str(ctx.get_param("target_quantity", "0.0001")))
        return [
            SignalEvent(
                ts_event=event.ts_event,
                ts_ingest=ctx.clock.now_ns(),
                source=f"strategy:{ctx.strategy_id}",
                strategy_id=ctx.strategy_id,
                instrument=event.instrument,
                side=side,
                target_quantity=target_quantity,
                order_type=OrderType.MARKET,
                time_in_force=TimeInForce.IOC,
                rationale=f"ping-pong {side.name} every {interval_seconds:g}s",
            )
        ]


__all__ = ["PingPongStrategy"]
