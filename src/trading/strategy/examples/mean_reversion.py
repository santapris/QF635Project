"""Mean-reversion strategy.

Fades extremes: enters short when price tags the upper Bollinger band,
long when it tags the lower band, and exits to flat once price has
returned to the middle band (the SMA).

Uses the portfolio view to decide whether to enter or exit — a classic
case of a strategy that needs to read its own position.
"""

from __future__ import annotations

from decimal import Decimal

from ...core.events import SignalEvent, TickEvent
from ...core.instruments import Instrument
from ...core.types import OrderType, Side, StrategyId, TimeInForce
from ..base import AbstractStrategy
from ..context import StrategyContext
from ..indicator_lib import Bollinger


class MeanReversionStrategy(AbstractStrategy):
    """Bollinger band fade with mean-touch exit."""

    def __init__(
        self,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        period: int = 20,
        num_std: float = 2.0,
    ) -> None:
        super().__init__(strategy_id=strategy_id, instruments=instruments)
        self._bands: dict[str, Bollinger] = {
            i.instrument_id: Bollinger(period=period, num_std=num_std)
            for i in instruments
        }

    async def on_tick(
        self, event: TickEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        iid = event.instrument.instrument_id
        bands = self._bands[iid]
        mid = float(event.mid)
        out = bands.update(mid)
        if out is None or not bands.is_ready:
            return []

        position = ctx.portfolio.get_position(event.instrument, ctx.strategy_id)
        is_flat = position is None or position.is_flat
        is_long = position is not None and position.is_long
        is_short = position is not None and position.is_short

        target_quantity = Decimal(str(ctx.get_param("target_quantity", "1")))

        # --- Entry: fade the extreme. Enter only when flat.
        if is_flat:
            if mid >= out.upper:
                return [
                    self._signal(
                        ctx, event, Side.SELL, target_quantity,
                        f"upper band fade (mid={mid:.4f} ≥ upper={out.upper:.4f})",
                    )
                ]
            if mid <= out.lower:
                return [
                    self._signal(
                        ctx, event, Side.BUY, target_quantity,
                        f"lower band fade (mid={mid:.4f} ≤ lower={out.lower:.4f})",
                    )
                ]
            return []

        # --- Exit: close once price has reverted to the mean.
        # Use a small dead-band so a single tick crossing back doesn't
        # immediately re-enter on the opposite side.
        if is_long and mid >= out.middle:
            assert position is not None
            return [
                self._signal(
                    ctx, event, Side.SELL, abs(position.quantity),
                    f"mean-touch exit long (mid={mid:.4f} ≥ mid_band={out.middle:.4f})",
                )
            ]
        if is_short and mid <= out.middle:
            assert position is not None
            return [
                self._signal(
                    ctx, event, Side.BUY, abs(position.quantity),
                    f"mean-touch exit short (mid={mid:.4f} ≤ mid_band={out.middle:.4f})",
                )
            ]
        return []

    def _signal(
        self,
        ctx: StrategyContext,
        event: TickEvent,
        side: Side,
        quantity: Decimal,
        rationale: str,
    ) -> SignalEvent:
        return SignalEvent(
            ts_event=event.ts_event,
            ts_ingest=ctx.clock.now_ns(),
            source=f"strategy:{ctx.strategy_id}",
            strategy_id=ctx.strategy_id,
            instrument=event.instrument,
            side=side,
            target_quantity=quantity,
            order_type=OrderType.MARKET,
            time_in_force=TimeInForce.IOC,
            rationale=rationale,
        )


__all__ = ["MeanReversionStrategy"]
