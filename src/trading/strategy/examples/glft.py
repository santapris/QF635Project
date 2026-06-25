"""Guéant-Lehalle-Fernandez-Tapia (GLFT) market-making strategy.

The asymptotic, closed-form successor to Avellaneda-Stoikov: instead of an
explicit terminal horizon it quotes the stationary optimal bid/ask for a
maker facing exponential order arrival ``A * exp(-k * delta)``. See
``analytics/glft.py`` for the formulae.

Quote logic per tick mirrors the A-S strategy's plumbing so the two share
idioms (microprice fair value, EWMA vol converted to absolute per-second
units, OFI tilt, post-only guard, inventory side-suppression, and the
``min_price_move_ticks`` hold gate that caps amend churn without dropping a
resting leg).

Grid variant: with ``n_levels > 1`` the strategy lays a ladder stepped
``grid_step_bps`` outward from the GLFT bid/ask, ``quote_size`` per level.
``n_levels == 1`` quotes a single bid/ask.
"""

from __future__ import annotations

import math
from decimal import Decimal

from ...analytics.glft import GLFT
from ...analytics.imbalance import OFI
from ...analytics.microprice import Microprice
from ...analytics.quote_filters import post_only_guard, round_to_tick
from ...analytics.volatility import EWMAVolatility
from ...core.events import FillEvent, OrderLeg, SignalEvent, TickEvent
from ...core.instruments import Instrument
from ...core.types import OrderType, Side, StrategyId, TimeInForce
from ..base import AbstractStrategy
from ..context import StrategyContext

# Seconds in a 365-day year — matches EWMAVolatility's default annualization
# so we can invert it back to per-second vol (same convention as the A-S strat).
_SECONDS_PER_YEAR = 365 * 24 * 3600.0


class GLFTStrategy(AbstractStrategy):
    """Adaptive market-making via the GLFT closed-form optimal quotes."""

    def __init__(
        self,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        gamma: float = 0.2,
        k: float = 1.5,
        A: float = 140.0,
        half_life_seconds: float = 60.0,
        ofi_window_seconds: float = 10.0,
        ofi_alpha: float = 0.0,
        quote_size: Decimal = Decimal("0.01"),
        max_position: Decimal = Decimal("0.5"),
        min_vol: float = 0.5,
        min_price_move_ticks: int = 1,
        n_levels: int = 1,
        grid_step_bps: float = 5.0,
    ) -> None:
        super().__init__(strategy_id=strategy_id, instruments=instruments)
        if max_position <= 0:
            raise ValueError("max_position must be positive")
        if quote_size <= 0:
            raise ValueError("quote_size must be positive")
        if n_levels < 1:
            raise ValueError("n_levels must be >= 1")
        self._ofi_alpha = ofi_alpha
        self._quote_size = quote_size
        self._max_position = max_position
        self._min_vol = min_vol
        self._min_price_move_ticks = min_price_move_ticks
        self._n_levels = n_levels
        self._step_frac = grid_step_bps / 10_000.0

        self._microprice = Microprice()
        self._vol = EWMAVolatility(half_life_seconds=half_life_seconds)
        self._glft = GLFT(gamma=gamma, k=k, A=A)
        self._ofi: OFI | None = None
        self._ofi_window_seconds = ofi_window_seconds
        self._last_bid: Decimal | None = None
        self._last_ask: Decimal | None = None
        self._latest_diagnostics: dict | None = None

    @classmethod
    def from_config(
        cls,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        parameters: dict[str, str],
    ) -> "GLFTStrategy":
        def f(key: str, default: float) -> float:
            return float(parameters.get(key, default))

        return cls(
            strategy_id=strategy_id,
            instruments=instruments,
            gamma=f("gamma", 0.2),
            k=f("k", 1.5),
            A=f("A", 140.0),
            half_life_seconds=f("half_life_seconds", 60.0),
            ofi_window_seconds=f("ofi_window_seconds", 10.0),
            ofi_alpha=f("ofi_alpha", 0.0),
            quote_size=Decimal(parameters.get("quote_size", "0.01")),
            max_position=Decimal(parameters.get("max_position", "0.5")),
            min_vol=f("min_vol", 0.5),
            min_price_move_ticks=int(f("min_price_move_ticks", 1)),
            n_levels=int(f("n_levels", 1)),
            grid_step_bps=f("grid_step_bps", 5.0),
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
        ts_ns = event.ts_event

        micro = self._microprice.update(bid, bid_size, ask, ask_size)
        if micro is None:
            return []

        # EWMA returns annualized *relative* vol; convert to absolute per-second
        # so gamma*sigma terms land in price units (same as the A-S strategy).
        sigma_ann = (
            max(self._min_vol, sigma_raw)
            if (sigma_raw := self._vol.update(micro, ts_ns)) is not None
            else self._min_vol
        )
        sigma = sigma_ann * micro / math.sqrt(_SECONDS_PER_YEAR)

        ofi_val = self._ofi.update(bid, bid_size, ask, ask_size, ts_ns) or 0.0

        instrument = event.instrument
        position = ctx.portfolio.get_position(instrument, ctx.strategy_id)
        inventory = float(position.quantity) if position is not None else 0.0

        quotes = self._glft.quotes(mid=micro, inventory=inventory, sigma=sigma)
        reservation = quotes.reservation + self._ofi_alpha * ofi_val
        half_spread = quotes.half_spread

        tick = instrument.tick_size
        base_bid = round_to_tick(Decimal(str(reservation - half_spread)), tick)
        base_ask = round_to_tick(Decimal(str(reservation + half_spread)), tick)

        best_bid = event.bid_price
        best_ask = event.ask_price
        min_move = tick * self._min_price_move_ticks

        # Hold the prior base price when the new one hasn't moved a full gate.
        base_bid = (
            base_bid
            if (self._last_bid is None or abs(base_bid - self._last_bid) >= min_move)
            else self._last_bid
        )
        base_ask = (
            base_ask
            if (self._last_ask is None or abs(base_ask - self._last_ask) >= min_move)
            else self._last_ask
        )

        center = Decimal(str(micro))
        step = center * Decimal(str(self._step_frac))

        buy_ok = inventory < float(self._max_position)
        sell_ok = inventory > -float(self._max_position)
        buy_guard = post_only_guard(Side.BUY, base_bid, best_bid, best_ask)
        sell_guard = post_only_guard(Side.SELL, base_ask, best_bid, best_ask)

        legs: list[OrderLeg] = []
        emitted_buy = emitted_sell = False
        for i in range(self._n_levels):
            if buy_ok:
                bid_price = round_to_tick(base_bid - step * Decimal(i), tick)
                if post_only_guard(Side.BUY, bid_price, best_bid, best_ask):
                    legs.append(self._leg(Side.BUY, bid_price))
                    emitted_buy = True
            if sell_ok:
                ask_price = round_to_tick(base_ask + step * Decimal(i), tick)
                if post_only_guard(Side.SELL, ask_price, best_bid, best_ask):
                    legs.append(self._leg(Side.SELL, ask_price))
                    emitted_sell = True

        # Remember the (possibly held) base prices for the next move-gate check;
        # forget a side that emitted nothing so it places fresh when it resumes.
        self._last_bid = base_bid if emitted_buy else None
        self._last_ask = base_ask if emitted_sell else None

        # Diagnostics conform to StrategyDiagnosticsEvent (no spread widening in
        # GLFT, so the *_raw fields equal the finals; vpin_widened is always
        # False — GLFT has no toxicity gate).
        self._latest_diagnostics = {
            "ts_event": event.ts_event,
            "ts_ingest": ctx.clock.now_ns(),
            "source": f"strategy:{ctx.strategy_id}",
            "strategy_id": ctx.strategy_id,
            "instrument": event.instrument,
            "inventory": inventory,
            "reservation_raw": quotes.reservation,
            "reservation": reservation,
            "half_spread_raw": half_spread,
            "half_spread": half_spread,
            "bid_quote": float(base_bid) if emitted_buy else None,
            "ask_quote": float(base_ask) if emitted_sell else None,
            "buy_guard": buy_guard,
            "sell_guard": sell_guard,
            "n_legs": len(legs),
            "vpin_widened": False,
        }

        if not legs:
            return []

        return [SignalEvent(
            ts_event=event.ts_event,
            ts_ingest=ctx.clock.now_ns(),
            source=f"strategy:{ctx.strategy_id}",
            strategy_id=ctx.strategy_id,
            instrument=instrument,
            legs=tuple(legs),
            rationale=f"glft inv={inventory:.6f} hs={half_spread:.4f}",
        )]

    async def on_fill(
        self, event: FillEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        ctx.logger.info(
            "glft_fill",
            side=event.side.value, fill_price=event.fill_price,
            fill_quantity=event.fill_quantity,
        )
        return []

    def get_strategy_diagnostics(self) -> dict | None:
        return self._latest_diagnostics

    def _leg(self, side: Side, price: Decimal) -> OrderLeg:
        return OrderLeg(
            side=side,
            quantity=self._quote_size,
            price=price,
            order_type=OrderType.POST_ONLY,
            time_in_force=TimeInForce.GTC,
        )

    def serialize_state(self) -> dict:
        return {
            "microprice": self._microprice.serialize(),
            "vol": self._vol.serialize(),
            "ofi": self._ofi.serialize() if self._ofi else {},
        }

    def restore_state(self, d: dict) -> None:
        if "microprice" in d:
            self._microprice.restore(d["microprice"])
        if "vol" in d:
            self._vol.restore(d["vol"])


__all__ = ["GLFTStrategy"]
