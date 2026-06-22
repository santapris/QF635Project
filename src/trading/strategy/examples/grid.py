"""Grid (ladder) market-making strategy.

Posts a symmetric ladder of resting limit orders around a center price:
``n_levels`` bids stepped progressively below it and ``n_levels`` asks
stepped above it, each separated by ``grid_step_bps`` of the center. The
whole grid is shifted by a linear inventory skew so the maker leans against
its position (long → ladder shifts down, eager to sell).

No fair-value model — this is the classic crypto "grid bot" expressed in the
platform's snapshot ``SignalEvent`` form: every tick emits the full desired
set of legs and the OMS reconciles them against resting orders (unchanged
legs keep their queue position). The two-gate requote control from the simple
market maker caps churn on a busy book.
"""

from __future__ import annotations

from decimal import Decimal

from ...analytics.microprice import Microprice
from ...analytics.quote_filters import post_only_guard
from ...core.events import FillEvent, OrderLeg, SignalEvent, TickEvent
from ...core.instruments import Instrument
from ...core.types import OrderType, Quantity, Side, StrategyId, TimeInForce
from ..base import AbstractStrategy
from ..context import StrategyContext

_NS_PER_SECOND = 1_000_000_000


class GridStrategy(AbstractStrategy):
    """Symmetric inventory-skewed order ladder."""

    def __init__(
        self,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        quote_size: Decimal = Decimal("0.01"),
        n_levels: int = 3,
        grid_step_bps: float = 5.0,
        max_position: Decimal = Decimal("0.5"),
        inventory_skew_bps: float = 5.0,
        use_microprice: bool = False,
        min_quote_interval_s: float = 1.0,
        requote_threshold_bps: float = 2.0,
    ) -> None:
        super().__init__(strategy_id=strategy_id, instruments=instruments)
        if max_position <= 0:
            raise ValueError("max_position must be positive")
        if quote_size <= 0:
            raise ValueError("quote_size must be positive")
        if n_levels < 1:
            raise ValueError("n_levels must be >= 1")
        if grid_step_bps <= 0:
            raise ValueError("grid_step_bps must be positive")
        if min_quote_interval_s < 0:
            raise ValueError("min_quote_interval_s must be >= 0")
        if requote_threshold_bps < 0:
            raise ValueError("requote_threshold_bps must be >= 0")
        self._quote_size = quote_size
        self._n_levels = n_levels
        self._step_frac = grid_step_bps / 10_000.0
        self._max_position = max_position
        self._inventory_skew_frac = inventory_skew_bps / 10_000.0
        self._use_microprice = use_microprice
        self._min_quote_interval_ns = int(min_quote_interval_s * _NS_PER_SECOND)
        self._requote_threshold_frac = requote_threshold_bps / 10_000.0

        self._microprice = Microprice()
        self._last_quote_ns: dict[str, int] = {}
        self._last_quoted_center: dict[str, Decimal] = {}

    @classmethod
    def from_config(
        cls,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        parameters: dict[str, str],
    ) -> "GridStrategy":
        def f(key: str, default: float) -> float:
            return float(parameters.get(key, default))

        return cls(
            strategy_id=strategy_id,
            instruments=instruments,
            quote_size=Decimal(parameters.get("quote_size", "0.01")),
            n_levels=int(f("n_levels", 3)),
            grid_step_bps=f("grid_step_bps", 5.0),
            max_position=Decimal(parameters.get("max_position", "0.5")),
            inventory_skew_bps=f("inventory_skew_bps", 5.0),
            use_microprice=str(parameters.get("use_microprice", "false")).lower()
            in ("1", "true", "yes"),
            min_quote_interval_s=f("min_quote_interval_s", 1.0),
            requote_threshold_bps=f("requote_threshold_bps", 2.0),
        )

    def _should_requote(
        self, instrument_id: str, center: Decimal, now_ns: int
    ) -> bool:
        last_ns = self._last_quote_ns.get(instrument_id, 0)
        if self._min_quote_interval_ns > 0:
            if now_ns - last_ns < self._min_quote_interval_ns:
                return False
        if self._requote_threshold_frac > 0:
            last_center = self._last_quoted_center.get(instrument_id)
            if last_center is not None and last_center > 0:
                move = abs(center - last_center) / last_center
                if move < Decimal(str(self._requote_threshold_frac)):
                    return False
        return True

    async def on_tick(
        self, event: TickEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        instrument = event.instrument
        instrument_id = instrument.instrument_id
        now_ns = ctx.clock.now_ns()

        if self._use_microprice:
            micro = self._microprice.update(
                float(event.bid_price), float(event.bid_size),
                float(event.ask_price), float(event.ask_size),
            )
            center = Decimal(str(micro)) if micro is not None else event.mid
        else:
            center = event.mid

        if not self._should_requote(instrument_id, center, now_ns):
            return []

        position = ctx.portfolio.get_position(instrument, ctx.strategy_id)
        inventory = position.quantity if position is not None else Quantity(Decimal(0))

        skew_ratio = float(-inventory / self._max_position)
        skew_ratio = max(-1.0, min(1.0, skew_ratio))
        skew = Decimal(str(skew_ratio * self._inventory_skew_frac)) * center

        best_bid = event.bid_price
        best_ask = event.ask_price
        step = Decimal(str(self._step_frac))

        legs: list[OrderLeg] = []
        for i in range(1, self._n_levels + 1):
            offset = center * step * Decimal(i)
            # Buy ladder below center; stop once at the long cap.
            if inventory < self._max_position:
                bid_price = instrument.round_price(center - offset + skew)
                if post_only_guard(Side.BUY, bid_price, best_bid, best_ask):
                    legs.append(OrderLeg(
                        side=Side.BUY, price=bid_price, quantity=self._quote_size,
                        order_type=OrderType.POST_ONLY,
                        time_in_force=TimeInForce.GTC,
                    ))
            # Sell ladder above center; stop once at the short cap.
            if inventory > -self._max_position:
                ask_price = instrument.round_price(center + offset + skew)
                if post_only_guard(Side.SELL, ask_price, best_bid, best_ask):
                    legs.append(OrderLeg(
                        side=Side.SELL, price=ask_price, quantity=self._quote_size,
                        order_type=OrderType.POST_ONLY,
                        time_in_force=TimeInForce.GTC,
                    ))

        self._last_quote_ns[instrument_id] = now_ns
        self._last_quoted_center[instrument_id] = center

        if not legs:
            return []

        return [SignalEvent(
            ts_event=event.ts_event,
            ts_ingest=now_ns,
            source=f"strategy:{ctx.strategy_id}",
            strategy_id=ctx.strategy_id,
            instrument=instrument,
            legs=tuple(legs),
            rationale=f"grid center={center} levels={self._n_levels}",
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


__all__ = ["GridStrategy"]
