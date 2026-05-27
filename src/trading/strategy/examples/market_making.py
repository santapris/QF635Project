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

Quote-rate control (two independent gates — both must pass):

- ``min_quote_interval_s``: hard cooldown between any requote.  Even if
  the mid moves continuously, we never emit faster than once per interval.
- ``requote_threshold_bps``: only requote when the mid has moved by at
  least this many bps since the last quote.  Filters out sub-tick noise
  that otherwise spams orders on a busy book-ticker stream.

Set ``min_quote_interval_s=0`` and ``requote_threshold_bps=0`` to
restore the original every-tick behaviour (useful for backtesting).
"""

from __future__ import annotations

from decimal import Decimal

from ...core.events import FillEvent, SignalEvent, TickEvent
from ...core.instruments import Instrument
from ...core.types import OrderType, Quantity, Side, StrategyId, TimeInForce
from ..base import AbstractStrategy
from ..context import StrategyContext

_NS_PER_SECOND = 1_000_000_000


class MarketMakingStrategy(AbstractStrategy):
    """Two-sided quoting with linear inventory skew and rate control."""

    def __init__(
        self,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        quote_size: Decimal = Decimal("0.01"),
        target_spread_bps: float = 10.0,
        max_position: Decimal = Decimal("0.5"),
        inventory_skew_bps: float = 5.0,
        min_quote_interval_s: float = 1.0,
        requote_threshold_bps: float = 2.0,
    ) -> None:
        super().__init__(strategy_id=strategy_id, instruments=instruments)
        if max_position <= 0:
            raise ValueError("max_position must be positive")
        if quote_size <= 0:
            raise ValueError("quote_size must be positive")
        if min_quote_interval_s < 0:
            raise ValueError("min_quote_interval_s must be >= 0")
        if requote_threshold_bps < 0:
            raise ValueError("requote_threshold_bps must be >= 0")
        self._quote_size = quote_size
        self._half_spread_frac = target_spread_bps / 2.0 / 10_000.0
        self._max_position = max_position
        self._inventory_skew_frac = inventory_skew_bps / 10_000.0
        self._min_quote_interval_ns = int(min_quote_interval_s * _NS_PER_SECOND)
        self._requote_threshold_frac = requote_threshold_bps / 10_000.0

        # Per-instrument state: last quote timestamp and last quoted mid.
        self._last_quote_ns: dict[str, int] = {}
        self._last_quoted_mid: dict[str, Decimal] = {}

    def _should_requote(self, instrument_id: str, mid: Decimal, now_ns: int) -> bool:
        """Return True only when both rate gates are satisfied."""
        last_ns = self._last_quote_ns.get(instrument_id, 0)

        # Gate 1: hard time cooldown.
        if self._min_quote_interval_ns > 0:
            if now_ns - last_ns < self._min_quote_interval_ns:
                return False

        # Gate 2: mid has moved enough to warrant a new quote.
        if self._requote_threshold_frac > 0:
            last_mid = self._last_quoted_mid.get(instrument_id)
            if last_mid is not None and last_mid > 0:
                move = abs(mid - last_mid) / last_mid
                if move < Decimal(str(self._requote_threshold_frac)):
                    return False

        return True

    async def on_tick(
        self, event: TickEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        instrument = event.instrument
        mid = event.mid
        now_ns = ctx.clock.now_ns()
        instrument_id = instrument.instrument_id

        if not self._should_requote(instrument_id, mid, now_ns):
            return []

        position = ctx.portfolio.get_position(instrument, ctx.strategy_id)
        inventory = position.quantity if position is not None else Quantity(Decimal(0))

        # Skew runs from -1 (max long) to +1 (max short). Positive skew
        # widens the bid (we don't want to buy more) and tightens the ask.
        skew_ratio = float(-inventory / self._max_position)
        skew_ratio = max(-1.0, min(1.0, skew_ratio))

        # Decimal math from here onwards — these become order prices.
        half_spread = Decimal(str(self._half_spread_frac)) * mid
        skew = Decimal(str(skew_ratio * self._inventory_skew_frac)) * mid

        bid_price = instrument.round_price(mid - half_spread + skew)
        ask_price = instrument.round_price(mid + half_spread + skew)

        signals: list[SignalEvent] = []

        # Withdraw a side once we hit the inventory cap.
        if inventory < self._max_position:
            signals.append(self._quote(ctx, event, Side.BUY, bid_price))
        if inventory > -self._max_position:
            signals.append(self._quote(ctx, event, Side.SELL, ask_price))

        if signals:
            self._last_quote_ns[instrument_id] = now_ns
            self._last_quoted_mid[instrument_id] = mid

        return signals

    async def on_fill(
        self, event: FillEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        # Re-quoting on every fill is the OMS's job (cancel-replace) rather
        # than the strategy's. We log here for audit but emit no signals;
        # the next tick will produce updated quotes via on_tick.
        ctx.logger.info(
            "filled",
            side=event.side.value, fill_quantity=event.fill_quantity,
            fill_price=event.fill_price, leaves_quantity=event.leaves_quantity,
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
