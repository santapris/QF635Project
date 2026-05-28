"""Avellaneda-Stoikov market-making strategy.

Composes the analytics layer (microprice, EWMA vol, OFI, VPIN, A-S
calculator) into a complete adaptive market-making strategy.

Quote logic per tick:
1. Microprice as mid (beats arithmetic mid on imbalanced books).
2. EWMAVolatility to size the spread to current regime.
3. A-S formula: reservation = microprice - inv * γ * σ² * τ
4. OFI tilt on reservation: reservation += alpha * OFI
5. VPIN toxicity gate: if VPIN > threshold, widen half_spread by factor.
6. Post-only guard before emitting: reject if quote would cross.

Parameters (all from TOML config as str → parsed in __init__):
    gamma               : A-S risk aversion (default 0.3)
    k                   : order arrival intensity (default 1.5)
    tau_seconds         : quoting horizon in seconds (default 300)
    half_life_seconds   : EWMA vol half-life (default 60)
    ofi_window_seconds  : OFI rolling window (default 10)
    ofi_alpha           : OFI tilt coefficient on reservation (default 0)
    vpin_bucket_volume  : VPIN bucket size in base units (default 1.0)
    vpin_threshold      : VPIN level to trigger widen (default 0.7)
    vpin_widen_factor   : spread multiplier when VPIN > threshold (default 3.0)
    quote_size          : base units per quote (default 0.01)
    max_position        : max abs inventory before side suppressed (default 0.5)
    min_vol             : vol floor — prevents zero spread on flat mkt (default 0.5)
    min_price_move_ticks: only re-quote if price moved ≥ N ticks (default 1)
                          throttles order spam since OMS has no cancel-replace yet
"""

from __future__ import annotations

from decimal import Decimal

from ...analytics.avellaneda_stoikov import AvellanedaStoikov
from ...analytics.imbalance import OBI, OFI
from ...analytics.microprice import Microprice
from ...analytics.quote_filters import post_only_guard, round_to_tick
from ...analytics.volatility import EWMAVolatility
from ...analytics.vpin import VPIN
from ...core.events import FillEvent, OrderLeg, SignalEvent, TickEvent, TradeEvent
from ...core.instruments import Instrument
from ...core.types import OrderType, Quantity, Side, StrategyId, TimeInForce
from ..base import AbstractStrategy
from ..context import StrategyContext


class AvellanedaStoikovStrategy(AbstractStrategy):
    """Adaptive market-making via Avellaneda-Stoikov optimal quoting."""

    def __init__(
        self,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        gamma: float = 0.3,
        k: float = 1.5,
        tau_seconds: float = 300.0,
        half_life_seconds: float = 60.0,
        ofi_window_seconds: float = 10.0,
        ofi_alpha: float = 0.0,
        vpin_bucket_volume: float = 1.0,
        vpin_threshold: float = 0.7,
        vpin_widen_factor: float = 3.0,
        quote_size: Decimal = Decimal("0.01"),
        max_position: Decimal = Decimal("0.5"),
        min_vol: float = 0.5,
        min_price_move_ticks: int = 1,
    ) -> None:
        super().__init__(strategy_id=strategy_id, instruments=instruments)
        self._gamma = gamma
        self._ofi_alpha = ofi_alpha
        self._vpin_threshold = vpin_threshold
        self._vpin_widen_factor = vpin_widen_factor
        self._quote_size = quote_size
        self._max_position = max_position
        self._min_vol = min_vol
        self._min_price_move_ticks = min_price_move_ticks
        # Last emitted quote prices — throttle re-quoting if price unchanged
        self._last_bid: Decimal | None = None
        self._last_ask: Decimal | None = None

        # Stateful analytics — one set per strategy (single instrument assumed)
        self._microprice = Microprice()
        self._vol = EWMAVolatility(half_life_seconds=half_life_seconds)
        self._as = AvellanedaStoikov(gamma=gamma, k=k, tau_seconds=tau_seconds)
        self._obi = OBI()
        self._vpin = VPIN(bucket_volume=vpin_bucket_volume)
        # OFI needs clock; injected on first on_tick via context
        self._ofi: OFI | None = None
        self._ofi_window_seconds = ofi_window_seconds

    @classmethod
    def from_config(
        cls,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        parameters: dict[str, str],
    ) -> "AvellanedaStoikovStrategy":
        """Construct from TOML parameters dict (all values are strings)."""
        def f(key: str, default: float) -> float:
            return float(parameters.get(key, default))

        return cls(
            strategy_id=strategy_id,
            instruments=instruments,
            gamma=f("gamma", 0.3),
            k=f("k", 1.5),
            tau_seconds=f("tau_seconds", 300.0),
            half_life_seconds=f("half_life_seconds", 60.0),
            ofi_window_seconds=f("ofi_window_seconds", 10.0),
            ofi_alpha=f("ofi_alpha", 0.0),
            vpin_bucket_volume=f("vpin_bucket_volume", 1.0),
            vpin_threshold=f("vpin_threshold", 0.7),
            vpin_widen_factor=f("vpin_widen_factor", 3.0),
            quote_size=Decimal(parameters.get("quote_size", "0.01")),
            max_position=Decimal(parameters.get("max_position", "0.5")),
            min_vol=f("min_vol", 0.5),
            min_price_move_ticks=int(f("min_price_move_ticks", 1)),
        )

    async def on_tick(
        self, event: TickEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        if self._ofi is None:
            from ...analytics.imbalance import OFI as _OFI
            self._ofi = _OFI(self._ofi_window_seconds, ctx.clock)

        bid = float(event.bid_price)
        ask = float(event.ask_price)
        bid_size = float(event.bid_size)
        ask_size = float(event.ask_size)
        ts_ns = event.ts_event

        micro = self._microprice.update(bid, bid_size, ask, ask_size)
        if micro is None:
            return []

        sigma_raw = self._vol.update(micro, ts_ns)
        sigma = max(self._min_vol, sigma_raw) if sigma_raw is not None else self._min_vol

        ofi_val = self._ofi.update(bid, bid_size, ask, ask_size, ts_ns) or 0.0
        self._obi.update(bid_size, ask_size)

        quotes = self._as.quotes(mid=micro, inventory=self._inventory(event, ctx), sigma=sigma)

        reservation = quotes.reservation + self._ofi_alpha * ofi_val
        half_spread = quotes.half_spread

        vpin_val = self._vpin.value
        if vpin_val is not None and vpin_val > self._vpin_threshold:
            half_spread *= self._vpin_widen_factor

        instrument = event.instrument
        tick = instrument.tick_size

        bid_price = round_to_tick(Decimal(str(reservation - half_spread)), tick)
        ask_price = round_to_tick(Decimal(str(reservation + half_spread)), tick)

        best_bid = event.bid_price
        best_ask = event.ask_price

        legs: list[OrderLeg] = []
        inventory = self._inventory(event, ctx)

        buy_guard = post_only_guard(Side.BUY, bid_price, best_bid, best_ask)
        sell_guard = post_only_guard(Side.SELL, ask_price, best_bid, best_ask)

        min_move = instrument.tick_size * self._min_price_move_ticks

        bid_moved = (
            self._last_bid is None
            or abs(bid_price - self._last_bid) >= min_move
        )
        ask_moved = (
            self._last_ask is None
            or abs(ask_price - self._last_ask) >= min_move
        )

        if inventory < float(self._max_position) and buy_guard and bid_moved:
            legs.append(self._make_leg(Side.BUY, bid_price))
            self._last_bid = bid_price

        if inventory > -float(self._max_position) and sell_guard and ask_moved:
            legs.append(self._make_leg(Side.SELL, ask_price))
            self._last_ask = ask_price

        ctx.logger.debug(
            "as_tick",
            micro=round(micro, 4),
            arith_mid=round((bid + ask) / 2, 4),
            sigma=round(sigma, 6),
            ofi=round(ofi_val, 4),
            obi=round(self._obi.value or 0.0, 4),
            vpin=round(vpin_val, 4) if vpin_val is not None else None,
            vpin_widened=vpin_val is not None and vpin_val > self._vpin_threshold,
            reservation=round(quotes.reservation, 4),
            half_spread=round(half_spread, 4),
            bid_quote=float(bid_price),
            ask_quote=float(ask_price),
            inventory=round(inventory, 6),
            buy_guard=buy_guard,
            sell_guard=sell_guard,
            n_legs=len(legs),
        )

        if not legs:
            return []

        return [SignalEvent(
            ts_event=event.ts_event,
            ts_ingest=ctx.clock.now_ns(),
            source=f"strategy:{ctx.strategy_id}",
            strategy_id=ctx.strategy_id,
            instrument=event.instrument,
            legs=tuple(legs),
            rationale=f"as-mm inv={inventory:.6f}",
        )]

    async def on_trade(
        self, event: TradeEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        # Feed VPIN from public trades
        price = float(event.price)
        volume = float(event.quantity)
        self._vpin.update(price, volume)
        return []

    async def on_fill(
        self, event: FillEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        ctx.logger.info(
            "as_fill",
            side=event.side.value,
            fill_price=event.fill_price,
            fill_qty=event.fill_quantity,
        )
        return []

    def _inventory(self, event: TickEvent, ctx: StrategyContext) -> float:
        position = ctx.portfolio.get_position(event.instrument, ctx.strategy_id)
        if position is None:
            return 0.0
        return float(position.quantity)

    def _make_leg(self, side: Side, price: Decimal) -> OrderLeg:
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
            "obi": self._obi.serialize(),
            "vpin": self._vpin.serialize(),
        }

    def restore_state(self, d: dict) -> None:
        if "microprice" in d:
            self._microprice.restore(d["microprice"])
        if "vol" in d:
            self._vol.restore(d["vol"])
        if "obi" in d:
            self._obi.restore(d["obi"])
        if "vpin" in d:
            self._vpin.restore(d["vpin"])


__all__ = ["AvellanedaStoikovStrategy"]
