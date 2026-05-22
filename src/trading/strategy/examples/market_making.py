"""Market-making strategy.

Quotes both sides around the mid: a bid below and an ask above, with a
spread set by ``target_spread_bps``. Quotes are skewed away from the
side the strategy is already heavy on, so inventory mean-reverts.

The simplification: this strategy emits *fresh* quotes on every tick.
A real market maker would amend or cancel-replace existing quotes
rather than firing new ones; that's an OMS concern. Here we model it
as the strategy expressing its current desired quotes; the OMS in
batch 7 will be responsible for not double-quoting.

Inventory management:

- ``max_position`` is the absolute inventory limit (in base units).
- When inventory is positive (long), the bid price is shaded down by
  ``inventory_skew_bps * (inventory / max_position)`` and the ask is
  brought in by the same amount, biasing fills towards the sell side.
- Symmetric on the short side.
- Once inventory hits the limit on a side, that side's quote is
  withdrawn entirely (returns no signal for that side this tick).
"""

from __future__ import annotations

from decimal import Decimal

from ...core.events import FillEvent, SignalEvent, TickEvent
from ...core.instruments import Instrument
from ...core.types import OrderType, Quantity, Side, StrategyId, TimeInForce
from ..base import AbstractStrategy
from ..context import StrategyContext


class MarketMakingStrategy(AbstractStrategy):
    """Two-sided quoting with linear inventory skew."""

    def __init__(
        self,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        quote_size: Decimal = Decimal("0.01"),
        target_spread_bps: float = 10.0,
        max_position: Decimal = Decimal("0.5"),
        inventory_skew_bps: float = 5.0,
    ) -> None:
        super().__init__(strategy_id=strategy_id, instruments=instruments)
        if max_position <= 0:
            raise ValueError("max_position must be positive")
        if quote_size <= 0:
            raise ValueError("quote_size must be positive")
        self._quote_size = quote_size
        self._half_spread_frac = target_spread_bps / 2.0 / 10_000.0
        self._max_position = max_position
        self._inventory_skew_frac = inventory_skew_bps / 10_000.0

    async def on_tick(
        self, event: TickEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        instrument = event.instrument
        position = ctx.portfolio.get_position(instrument, ctx.strategy_id)
        inventory = position.quantity if position is not None else Quantity(Decimal(0))

        # Skew runs from -1 (max long) to +1 (max short). Positive skew
        # widens the bid (we don't want to buy more) and tightens the ask.
        skew_ratio = float(-inventory / self._max_position)
        skew_ratio = max(-1.0, min(1.0, skew_ratio))

        mid = event.mid
        # Decimal math from here onwards — these become order prices.
        half_spread = Decimal(str(self._half_spread_frac)) * mid
        skew = Decimal(str(skew_ratio * self._inventory_skew_frac)) * mid

        bid_price = instrument.round_price(mid - half_spread + skew)
        ask_price = instrument.round_price(mid + half_spread + skew)

        signals: list[SignalEvent] = []

        # Withdraw a side once we hit the inventory cap.
        if inventory < self._max_position:
            signals.append(
                self._quote(ctx, event, Side.BUY, bid_price)
            )
        if inventory > -self._max_position:
            signals.append(
                self._quote(ctx, event, Side.SELL, ask_price)
            )
        return signals

    async def on_fill(
        self, event: FillEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        # Re-quoting on every fill is the OMS's job (cancel-replace) rather
        # than the strategy's. We log here for audit but emit no signals;
        # the next tick will produce updated quotes via on_tick.
        ctx.logger.info(
            "filled %s %s @ %s (leaves=%s)",
            event.side.value, event.fill_quantity, event.fill_price,
            event.leaves_quantity,
        )
        return []

    def _quote(
        self,
        ctx: StrategyContext,
        event: TickEvent,
        side: Side,
        price: Decimal,
    ) -> SignalEvent:
        return SignalEvent(
            ts_event=event.ts_event,
            ts_ingest=ctx.clock.now_ns(),
            source=f"strategy:{ctx.strategy_id}",
            strategy_id=ctx.strategy_id,
            instrument=event.instrument,
            side=side,
            target_quantity=self._quote_size,
            suggested_price=price,
            order_type=OrderType.POST_ONLY,
            time_in_force=TimeInForce.GTC,
            rationale=f"market-make {side.value} @ {price}",
        )


__all__ = ["MarketMakingStrategy"]
