"""Momentum strategy — EMA crossover.

Long when fast EMA > slow EMA; flat or short otherwise. Acts only on
the *transition* (crossover), not on every tick where the condition
holds, so it doesn't spam orders.

Parameters (read from ``ctx.parameters`` so they hot-reload):

- ``fast_period`` (int, default 20)
- ``slow_period`` (int, default 50)
- ``target_quantity`` (Decimal as string, default "1")

This is intentionally simple — a teaching example. A production
momentum strategy would also size by volatility, respect a stop, and
reconcile its intended position against the actual portfolio before
emitting orders.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from ...core.events import SignalEvent, TickEvent
from ...core.instruments import Instrument
from ...core.types import OrderType, Side, StrategyId, TimeInForce
from ..base import AbstractStrategy
from ..context import StrategyContext
from ..indicator_lib import EMA


class MomentumStrategy(AbstractStrategy):
    """EMA crossover. Goes long on cross-up, short on cross-down."""

    def __init__(
        self,
        *,
        strategy_id: StrategyId,
        instruments: list[Instrument],
        fast_period: int = 20,
        slow_period: int = 50,
    ) -> None:
        super().__init__(strategy_id=strategy_id, instruments=instruments)
        if fast_period >= slow_period:
            raise ValueError("fast_period must be less than slow_period")

        # One pair of EMAs per instrument so we can run the same strategy
        # over many symbols simultaneously.
        self._fast: dict[str, EMA] = {
            i.instrument_id: EMA(fast_period) for i in instruments
        }
        self._slow: dict[str, EMA] = {
            i.instrument_id: EMA(slow_period) for i in instruments
        }
        self._regime: dict[str, Literal["above", "below"] | None] = {
            i.instrument_id: None for i in instruments
        }

    async def on_tick(
        self, event: TickEvent, ctx: StrategyContext
    ) -> list[SignalEvent]:
        iid = event.instrument.instrument_id
        fast_ema = self._fast[iid]
        slow_ema = self._slow[iid]

        mid = float(event.mid)
        fast_ema.update(mid)
        slow_ema.update(mid)

        if not (fast_ema.is_ready and slow_ema.is_ready):
            return []
        # mypy: both are ready, so values are not None.
        fast_v = fast_ema.value
        slow_v = slow_ema.value
        assert fast_v is not None and slow_v is not None

        new_regime: Literal["above", "below"] = "above" if fast_v > slow_v else "below"
        old_regime = self._regime[iid]
        self._regime[iid] = new_regime

        if old_regime is None or old_regime == new_regime:
            # First reading after warm-up, or no transition.
            return []

        # Crossover detected — emit a signal.
        side = Side.BUY if new_regime == "above" else Side.SELL
        target_quantity = Decimal(str(ctx.get_param("target_quantity", "1")))
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
                rationale=(
                    f"EMA crossover {old_regime}->{new_regime} "
                    f"(fast={fast_v:.4f}, slow={slow_v:.4f})"
                ),
            )
        ]


__all__ = ["MomentumStrategy"]
