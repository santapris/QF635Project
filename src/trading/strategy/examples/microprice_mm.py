"""Microprice market-making strategy.

The minimal "fair value beats mid" baseline: anchor both quotes to the
size-weighted microprice rather than the arithmetic mid, with a fixed
half-spread and an optional linear inventory skew.

Microprice = (bid * ask_size + ask * bid_size) / (bid_size + ask_size).
When the book is imbalanced it leans toward the side a marginal taker would
hit, so quoting around it reduces adverse selection versus the naive mid
(see ``analytics/microprice.py``). Everything else mirrors the simple
``MarketMakingStrategy``: snapshot ``SignalEvent`` semantics, POST_ONLY/GTC
legs, side withdrawal at the inventory cap, and the two-gate requote control.
"""

from __future__ import annotations

from decimal import Decimal

from ...analytics.microprice import Microprice
from ...core.events import FillEvent, OrderLeg, SignalEvent, TickEvent
from ...core.instruments import Instrument
from ...core.types import OrderType, Quantity, Side, StrategyId, TimeInForce
from ..base import AbstractStrategy
from ..context import StrategyContext

_NS_PER_SECOND = 1_000_000_000


class MicropriceMMStrategy(AbstractStrategy):
    """Two-sided quoting anchored to the microprice."""

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

        self._microprice = Microprice()
        self._last_quote_ns: dict[str, int] = {}
        self._last_quoted_fv: dict[str, Decimal] = {}

    @classmethod
    def from_config(
        cls,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        parameters: dict[str, str],
    ) -> "MicropriceMMStrategy":
        def f(key: str, default: float) -> float:
            return float(parameters.get(key, default))

        return cls(
            strategy_id=strategy_id,
            instruments=instruments,
            quote_size=Decimal(parameters.get("quote_size", "0.01")),
            target_spread_bps=f("target_spread_bps", 10.0),
            max_position=Decimal(parameters.get("max_position", "0.5")),
            inventory_skew_bps=f("inventory_skew_bps", 5.0),
            min_quote_interval_s=f("min_quote_interval_s", 1.0),
            requote_threshold_bps=f("requote_threshold_bps", 2.0),
        )

    def _should_requote(
        self, instrument_id: str, fv: Decimal, now_ns: int
    ) -> bool:
        last_ns = self._last_quote_ns.get(instrument_id, 0)
        if self._min_quote_interval_ns > 0:
            if now_ns - last_ns < self._min_quote_interval_ns:
                return False
        if self._requote_threshold_frac > 0:
            last_fv = self._last_quoted_fv.get(instrument_id)
            if last_fv is not None and last_fv > 0:
                move = abs(fv - last_fv) / last_fv
                if move < Decimal(str(self._requote_threshold_frac)):
                    return False
        return True

    async def on_tick(
        self, event: TickEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        instrument = event.instrument
        instrument_id = instrument.instrument_id
        now_ns = ctx.clock.now_ns()

        micro = self._microprice.update(
            float(event.bid_price), float(event.bid_size),
            float(event.ask_price), float(event.ask_size),
        )
        fv = Decimal(str(micro)) if micro is not None else event.mid

        if not self._should_requote(instrument_id, fv, now_ns):
            return []

        position = ctx.portfolio.get_position(instrument, ctx.strategy_id)
        inventory = position.quantity if position is not None else Quantity(Decimal(0))

        # Skew runs -1 (max long) .. +1 (max short): positive skew shifts both
        # quotes up to discourage further selling / encourage buying.
        skew_ratio = float(-inventory / self._max_position)
        skew_ratio = max(-1.0, min(1.0, skew_ratio))

        half_spread = Decimal(str(self._half_spread_frac)) * fv
        skew = Decimal(str(skew_ratio * self._inventory_skew_frac)) * fv

        bid_price = instrument.round_price(fv - half_spread + skew)
        ask_price = instrument.round_price(fv + half_spread + skew)

        legs: list[OrderLeg] = []
        if inventory < self._max_position:
            legs.append(OrderLeg(
                side=Side.BUY, price=bid_price, quantity=self._quote_size,
                order_type=OrderType.POST_ONLY, time_in_force=TimeInForce.GTC,
            ))
        if inventory > -self._max_position:
            legs.append(OrderLeg(
                side=Side.SELL, price=ask_price, quantity=self._quote_size,
                order_type=OrderType.POST_ONLY, time_in_force=TimeInForce.GTC,
            ))

        self._last_quote_ns[instrument_id] = now_ns
        self._last_quoted_fv[instrument_id] = fv

        return [SignalEvent(
            ts_event=event.ts_event,
            ts_ingest=now_ns,
            source=f"strategy:{ctx.strategy_id}",
            strategy_id=ctx.strategy_id,
            instrument=instrument,
            legs=tuple(legs),
            rationale=f"microprice-mm fv={fv}",
        )]

    async def on_fill(
        self, event: FillEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        ctx.logger.info(
            "filled",
            side=event.side.value, fill_quantity=event.fill_quantity,
            fill_price=event.fill_price, leaves_quantity=event.leaves_quantity,
        )
        return []

    def serialize_state(self) -> dict:
        return {"microprice": self._microprice.serialize()}

    def restore_state(self, d: dict) -> None:
        if "microprice" in d:
            self._microprice.restore(d["microprice"])


__all__ = ["MicropriceMMStrategy"]
