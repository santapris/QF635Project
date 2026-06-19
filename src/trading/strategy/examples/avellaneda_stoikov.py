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
    gamma               : A-S risk aversion (default 0.2)
    k                   : order arrival intensity (default 1.5)
    tau_seconds         : quoting horizon in seconds (default 2.0)
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
                          caps amend rate: even though the OMS now does
                          cancel-replace via amend, re-quoting on every
                          sub-tick drift floods the venue rate limit and
                          widens the amend-vs-fill race window

Tuning the spread/skew (why the defaults are what they are)
-----------------------------------------------------------
EWMAVolatility reports *annualized relative* vol; on_tick converts it to
*absolute per-second* vol (× micro / √seconds_per_year) before the A-S
formula, so γσ²τ lands in price units. With sigma at the min_vol floor:

    half_spread ≈ (γ · min_vol² · P² · τ / T_yr) / 2      (+ a tiny additive
    skew_at_qmax = γ · min_vol² · P² · τ / T_yr · max_pos    k-term, ~0.3 bps)

Two design constraints fix the defaults (P = 100k reference, T_yr = 3.15e7):
  - skew ≈ half_spread at full inventory  ⟹  max_position ≈ 0.5
  - half_spread ≈ 1.5 bps of price        ⟹  γ · τ ≈ 0.378  (→ γ=0.2, τ=2.0)
The half_spread is ~price-independent in bps but grows mildly with P. If you
move to a very different price level or symbol, re-solve γ·τ for that P.

VPIN toxicity gate — what it is and when it fires
--------------------------------------------------
VPIN (Volume-Synchronized Probability of Informed Trading, Easley et al. 2012)
measures whether recent trade flow is one-sided (informed) or balanced (noise).

Algorithm:
  - Accumulate trade volume into equal-size buckets (vpin_bucket_volume).
  - Classify each trade as buy or sell via BVC (bulk-volume classification).
  - Bucket toxicity = |V_buy - V_sell| / bucket_volume  ∈ [0, 1].
  - VPIN = rolling mean of toxicity over the last 50 buckets.

Interpretation:
  VPIN ≈ 0.0  →  balanced flow (noise traders, normal quoting safe)
  VPIN ≈ 0.5  →  moderate imbalance (increasing adverse-selection risk)
  VPIN ≈ 1.0  →  strongly one-sided flow (informed trader likely present)

When VPIN > vpin_threshold the strategy widens half_spread by vpin_widen_factor
to compensate for expected adverse selection. Logs "vpin_widening_triggered" at
INFO so the event is visible without DEBUG logging.

Why VPIN_WIDE stays silent on testnet:
  Binance testnet has no real informed traders. Trade flow is synthetic and
  balanced → VPIN stays near 0 and never reaches the 0.7 threshold. To
  observe widening: lower vpin_threshold to ~0.3, or run on live market data
  during a news/trend event with genuine one-sided flow.
"""

from __future__ import annotations

import math
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

# Seconds in a 365-day year — matches EWMAVolatility's default annualization
# so we can invert it back to per-second vol below.
_SECONDS_PER_YEAR = 365 * 24 * 3600.0

class AvellanedaStoikovStrategy(AbstractStrategy):
    """Adaptive market-making via Avellaneda-Stoikov optimal quoting."""

    def __init__(
        self,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        gamma: float = 0.2,
        k: float = 1.5,
        tau_seconds: float = 2.0,
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
        # Last emitted quote prices — skip re-quoting until price moves
        # ≥ min_price_move_ticks, capping amend churn against the venue
        self._last_bid: Decimal | None = None
        self._last_ask: Decimal | None = None

        # Stateful analytics — one set per strategy (single instrument assumed)
        self._microprice = Microprice()
        self._vol = EWMAVolatility(half_life_seconds=half_life_seconds)
        self._as = AvellanedaStoikov(gamma=gamma, k=k, tau_seconds=tau_seconds)
        self._obi = OBI()
        self._vpin = VPIN(bucket_volume=vpin_bucket_volume)
        # Track previous widening state to log transitions only (not every tick)
        self._vpin_widened_prev: bool = False
        # OFI needs clock; injected on first on_tick via context
        self._ofi: OFI | None = None
        self._ofi_window_seconds = ofi_window_seconds
        # Latest diagnostics dict for the registry to publish
        self._latest_diagnostics: dict | None = None

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
            gamma=f("gamma", 0.2),
            k=f("k", 1.5),
            tau_seconds=f("tau_seconds", 2.0),
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

        # EWMAVolatility returns *annualized relative* vol (dimensionless,
        # e.g. 0.5 = 50%/yr). The A-S formula needs *absolute per-second*
        # price vol so that gamma * sigma^2 * tau_seconds lands in price
        # units. Convert: relative -> absolute by * micro; annual -> per-second
        # by / sqrt(seconds_per_year). min_vol stays an annualized floor so
        # its config meaning ("50% annual vol") is unchanged.
        sigma_ann = (
            max(self._min_vol, sigma_raw)
            if (sigma_raw := self._vol.update(micro, ts_ns)) is not None
            else self._min_vol
        )
        sigma = sigma_ann * micro / math.sqrt(_SECONDS_PER_YEAR)

        ofi_raw = self._ofi.update(bid, bid_size, ask, ask_size, ts_ns)
        ofi_val = ofi_raw or 0.0
        obi_val = self._obi.update(bid_size, ask_size)

        inventory = self._inventory(event, ctx)
        quotes = self._as.quotes(mid=micro, inventory=inventory, sigma=sigma)

        reservation_raw = quotes.reservation
        reservation = reservation_raw + self._ofi_alpha * ofi_val
        half_spread_raw = quotes.half_spread
        half_spread = half_spread_raw

        vpin_val = self._vpin.value
        vpin_widened = vpin_val is not None and vpin_val > self._vpin_threshold
        if vpin_widened:
            half_spread *= self._vpin_widen_factor
        if vpin_widened and not self._vpin_widened_prev:
            ctx.logger.info(
                "vpin_widening_triggered",
                vpin=round(vpin_val, 4),  # type: ignore[arg-type]
                threshold=self._vpin_threshold,
                widen_factor=self._vpin_widen_factor,
                half_spread_before=round(half_spread_raw, 4),
                half_spread_after=round(half_spread, 4),
            )
        elif not vpin_widened and self._vpin_widened_prev:
            ctx.logger.info(
                "vpin_widening_cleared",
                vpin=round(vpin_val, 4) if vpin_val is not None else None,
                threshold=self._vpin_threshold,
            )
        self._vpin_widened_prev = vpin_widened

        instrument = event.instrument
        tick = instrument.tick_size

        bid_price = round_to_tick(Decimal(str(reservation - half_spread)), tick)
        ask_price = round_to_tick(Decimal(str(reservation + half_spread)), tick)

        best_bid = event.bid_price
        best_ask = event.ask_price

        min_move = instrument.tick_size * self._min_price_move_ticks

        # min_price_move_ticks caps amend churn, but it must not *drop* the leg:
        # SignalEvent is a snapshot, so a missing side reads as "withdraw" and
        # the OMS cancels the resting order (then re-places it next tick, losing
        # queue position). Instead, when the new price hasn't moved far enough,
        # re-emit the leg at the *last* price — the OMS sees an exact match and
        # no-ops, preserving queue position. Hard guards (inventory cap,
        # post-only) are the only conditions that legitimately withdraw a side.
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

        # Re-run the post-only guard against the price actually being emitted:
        # a held (last-tick) price can cross once the opposite best moves onto
        # it, so guard the effective price, not the freshly computed one.
        buy_guard = post_only_guard(Side.BUY, bid_price, best_bid, best_ask)
        sell_guard = post_only_guard(Side.SELL, ask_price, best_bid, best_ask)

        legs: list[OrderLeg] = []
        bid_quote: float | None = None
        ask_quote: float | None = None

        if inventory < float(self._max_position) and buy_guard:
            legs.append(self._make_leg(Side.BUY, bid_price))
            self._last_bid = bid_price
            bid_quote = float(bid_price)
        else:
            # Side withdrawn by a hard guard: the OMS will cancel the resting
            # bid, so forget its price — next time the side resumes it must
            # place fresh (move-gate compares against None), not hold a dead one.
            self._last_bid = None

        if inventory > -float(self._max_position) and sell_guard:
            legs.append(self._make_leg(Side.SELL, ask_price))
            self._last_ask = ask_price
            ask_quote = float(ask_price)
        else:
            self._last_ask = None

        ctx.logger.debug(
            "as_tick",
            micro=round(micro, 4),
            arith_mid=round((bid + ask) / 2, 4),
            sigma_ann=round(sigma_ann, 4),
            sigma_abs=round(sigma, 8),
            ofi=round(ofi_val, 4),
            obi=round(obi_val or 0.0, 4),
            vpin=round(vpin_val, 4) if vpin_val is not None else None,
            vpin_widened=vpin_widened,
            reservation=round(reservation, 4),
            half_spread=round(half_spread, 4),
            bid_quote=bid_quote,
            ask_quote=ask_quote,
            inventory=round(inventory, 6),
            buy_guard=buy_guard,
            sell_guard=sell_guard,
            n_legs=len(legs),
        )

        self._latest_diagnostics = {
            "ts_event": event.ts_event,
            "ts_ingest": ctx.clock.now_ns(),
            "source": f"strategy:{ctx.strategy_id}",
            "strategy_id": ctx.strategy_id,
            "instrument": event.instrument,
            "inventory": inventory,
            "reservation_raw": reservation_raw,
            "reservation": reservation,
            "half_spread_raw": half_spread_raw,
            "half_spread": half_spread,
            "bid_quote": bid_quote,
            "ask_quote": ask_quote,
            "buy_guard": buy_guard,
            "sell_guard": sell_guard,
            "n_legs": len(legs),
            "vpin_widened": vpin_widened,
        }

        if not legs:
            return []

        return [SignalEvent(
            ts_event=event.ts_event,
            ts_ingest=ctx.clock.now_ns(),
            source=f"strategy:{ctx.strategy_id}",
            strategy_id=ctx.strategy_id,
            instrument=event.instrument,
            legs=tuple(legs),
            # TODO - the inventory always remains the same - not clear if it's an issue with the position engine or the inventory calculation
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

    def get_strategy_diagnostics(self) -> dict | None:
        return self._latest_diagnostics

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
