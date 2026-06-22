"""Order-book-imbalance alpha market-making strategy.

Quotes around the microprice but tilts the fair value by a short-horizon
price-prediction signal built from order-book / order-flow imbalance
(Cont, Kukanov & Stoikov, 2014: imbalance is near-linearly related to the
next price move). The maker then leans its quotes in the predicted direction
and captures the spread on the side more likely to be hit.

Per tick:
    fv = microprice + obi_alpha * OBI + ofi_alpha * OFI
    fv += inventory_skew                 # lean against current position
    bid = fv - half_spread,  ask = fv + half_spread

OBI ∈ [-1, +1] is the instantaneous top-of-book pressure; OFI is the rolling
signed flow over ``ofi_window_seconds``. ``obi_alpha`` / ``ofi_alpha`` are in
price units per unit signal and default to 0 (pure microprice quoting) so the
alpha is opt-in. The ``min_price_move_ticks`` hold idiom (shared with the A-S
strategy) caps amend churn without ever dropping a resting leg.
"""

from __future__ import annotations

from decimal import Decimal

from ...analytics.imbalance import OBI, OFI
from ...analytics.microprice import Microprice
from ...analytics.quote_filters import post_only_guard, round_to_tick
from ...core.events import FillEvent, OrderLeg, SignalEvent, TickEvent
from ...core.instruments import Instrument
from ...core.types import OrderType, Quantity, Side, StrategyId, TimeInForce
from ..base import AbstractStrategy
from ..context import StrategyContext


class OBIAlphaStrategy(AbstractStrategy):
    """Microprice quoting tilted by order-book/flow imbalance alpha."""

    def __init__(
        self,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        quote_size: Decimal = Decimal("0.01"),
        target_spread_bps: float = 10.0,
        max_position: Decimal = Decimal("0.5"),
        inventory_skew_bps: float = 5.0,
        obi_alpha: float = 0.0,
        ofi_alpha: float = 0.0,
        ofi_window_seconds: float = 10.0,
        min_price_move_ticks: int = 1,
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
        self._obi_alpha = obi_alpha
        self._ofi_alpha = ofi_alpha
        self._ofi_window_seconds = ofi_window_seconds
        self._min_price_move_ticks = min_price_move_ticks

        self._microprice = Microprice()
        self._obi = OBI()
        self._ofi: OFI | None = None  # OFI needs clock; lazy-init on first tick
        self._last_bid: Decimal | None = None
        self._last_ask: Decimal | None = None

    @classmethod
    def from_config(
        cls,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        parameters: dict[str, str],
    ) -> "OBIAlphaStrategy":
        def f(key: str, default: float) -> float:
            return float(parameters.get(key, default))

        return cls(
            strategy_id=strategy_id,
            instruments=instruments,
            quote_size=Decimal(parameters.get("quote_size", "0.01")),
            target_spread_bps=f("target_spread_bps", 10.0),
            max_position=Decimal(parameters.get("max_position", "0.5")),
            inventory_skew_bps=f("inventory_skew_bps", 5.0),
            obi_alpha=f("obi_alpha", 0.0),
            ofi_alpha=f("ofi_alpha", 0.0),
            ofi_window_seconds=f("ofi_window_seconds", 10.0),
            min_price_move_ticks=int(f("min_price_move_ticks", 1)),
        )

    async def on_tick(
        self, event: TickEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        if self._ofi is None:
            self._ofi = OFI(self._ofi_window_seconds, ctx.clock)

        bid = float(event.bid_price)
        ask = float(event.ask_price)
        bid_size = float(event.bid_size)
        ask_size = float(event.ask_size)

        micro = self._microprice.update(bid, bid_size, ask, ask_size)
        if micro is None:
            return []

        obi_val = self._obi.update(bid_size, ask_size) or 0.0
        ofi_val = self._ofi.update(bid, bid_size, ask, ask_size, event.ts_event) or 0.0

        instrument = event.instrument
        position = ctx.portfolio.get_position(instrument, ctx.strategy_id)
        inventory = position.quantity if position is not None else Quantity(Decimal(0))

        # Fair value tilted by the imbalance alpha, then by inventory skew.
        fv = micro + self._obi_alpha * obi_val + self._ofi_alpha * ofi_val
        fv_dec = Decimal(str(fv))
        skew_ratio = float(-inventory / self._max_position)
        skew_ratio = max(-1.0, min(1.0, skew_ratio))
        skew = Decimal(str(skew_ratio * self._inventory_skew_frac)) * fv_dec
        half_spread = Decimal(str(self._half_spread_frac)) * fv_dec

        tick = instrument.tick_size
        bid_price = round_to_tick(fv_dec - half_spread + skew, tick)
        ask_price = round_to_tick(fv_dec + half_spread + skew, tick)

        best_bid = event.bid_price
        best_ask = event.ask_price
        min_move = tick * self._min_price_move_ticks

        # Hold the prior price when the new one hasn't moved a full gate — the
        # OMS no-ops on an exact match and keeps queue position. A dropped side
        # would read as a withdraw, so we re-emit at the last price instead.
        bid_price = (
            bid_price
            if (self._last_bid is None or abs(bid_price - self._last_bid) >= min_move)
            else self._last_bid
        )
        ask_price = (
            ask_price
            if (self._last_ask is None or abs(ask_price - self._last_ask) >= min_move)
            else self._last_ask
        )

        buy_guard = post_only_guard(Side.BUY, bid_price, best_bid, best_ask)
        sell_guard = post_only_guard(Side.SELL, ask_price, best_bid, best_ask)

        legs: list[OrderLeg] = []
        if inventory < self._max_position and buy_guard:
            legs.append(OrderLeg(
                side=Side.BUY, price=bid_price, quantity=self._quote_size,
                order_type=OrderType.POST_ONLY, time_in_force=TimeInForce.GTC,
            ))
            self._last_bid = bid_price
        else:
            self._last_bid = None

        if inventory > -self._max_position and sell_guard:
            legs.append(OrderLeg(
                side=Side.SELL, price=ask_price, quantity=self._quote_size,
                order_type=OrderType.POST_ONLY, time_in_force=TimeInForce.GTC,
            ))
            self._last_ask = ask_price
        else:
            self._last_ask = None

        if not legs:
            return []

        return [SignalEvent(
            ts_event=event.ts_event,
            ts_ingest=ctx.clock.now_ns(),
            source=f"strategy:{ctx.strategy_id}",
            strategy_id=ctx.strategy_id,
            instrument=instrument,
            legs=tuple(legs),
            rationale=f"obi-alpha obi={obi_val:.4f} ofi={ofi_val:.4f}",
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
        return {
            "microprice": self._microprice.serialize(),
            "obi": self._obi.serialize(),
            "ofi": self._ofi.serialize() if self._ofi else {},
        }

    def restore_state(self, d: dict) -> None:
        if "microprice" in d:
            self._microprice.restore(d["microprice"])
        if "obi" in d:
            self._obi.restore(d["obi"])


__all__ = ["OBIAlphaStrategy"]
