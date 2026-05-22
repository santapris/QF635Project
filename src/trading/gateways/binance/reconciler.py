"""Balance reconciliation.

Periodically polls ``/api/v3/account`` and compares the reported
balances against what our :class:`PositionEngine` thinks we hold. Any
divergence above ``mismatch_threshold`` raises an alert.

When does this matter?

- A fill we never received (network blip) leaves us short of position
  but with the actual asset on the venue.
- A manual trade on the Binance UI changes balances without flowing
  through the OMS.
- A failed cancel that filled at the last second.
- A duplicate fill the OMS deduped — our position stops short but the
  venue's view is correct.

We do not auto-correct. Auto-reconciliation in trading systems is a
classic foot-cannon: papering over a bug doesn't fix it, and a wrong
auto-correction can make the divergence worse. Instead the reconciler
publishes a :class:`ReconciliationMismatchAlert` (subclass of
RiskAlertEvent) so an operator notices and decides.
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Final

from ...core.clock import Clock
from ...core.events import RiskAlertEvent
from ...core.instruments import Instrument
from ...core.types import Severity, StrategyId
from ...event_bus.base import AbstractEventBus, Topic
from ...position.engine import PositionEngine
from .config import BinanceConfig
from .rest_client import BinanceRESTClient

_log = logging.getLogger(__name__)

_W_ACCOUNT: Final[float] = 10.0  # Binance weight for /api/v3/account


class BalanceReconciler:
    """Background task: poll account every ``reconcile_interval_seconds``.

    Compares the venue's reported free+locked balance per asset against
    the *sum* of position-engine quantities for instruments where that
    asset is the base. Quote-currency balances (e.g. USDT) are reported
    but not compared, since strategies don't have a position in their
    quote — quote balance changes are reflected in realized PnL.
    """

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        clock: Clock,
        config: BinanceConfig,
        rest: BinanceRESTClient,
        position_engine: PositionEngine,
        tracked_instruments: list[Instrument],
        mismatch_threshold: Decimal = Decimal("0.0001"),
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._config = config
        self._rest = rest
        self._position_engine = position_engine
        self._tracked = tracked_instruments
        self._threshold = mismatch_threshold
        self._task: asyncio.Task[None] | None = None
        self._stop = False

    # --- Lifecycle -------------------------------------------------------

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(
            self._run_loop(), name="binance-balance-reconciler"
        )

    async def stop(self) -> None:
        self._stop = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # --- Main loop -------------------------------------------------------

    async def _run_loop(self) -> None:
        interval = self._config.reconcile_interval_seconds
        while not self._stop:
            try:
                await self.reconcile_once()
            except asyncio.CancelledError:
                return
            except Exception:
                _log.exception("balance reconcile failed; will retry next cycle")
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                return

    async def reconcile_once(self) -> dict[str, tuple[Decimal, Decimal]]:
        """One reconciliation pass. Returns ``{asset: (venue, ours)}``.

        Public so an operator script can call it on demand.
        """
        account = await self._rest.request(
            "GET", "/api/v3/account",
            signed=True, weight=_W_ACCOUNT,
        )
        balances_raw = account.get("balances", [])
        venue_balances: dict[str, Decimal] = {}
        for b in balances_raw:
            asset = b["asset"]
            free = Decimal(str(b.get("free", "0")))
            locked = Decimal(str(b.get("locked", "0")))
            if free + locked > 0:
                venue_balances[asset] = free + locked

        # Aggregate our positions by base-currency asset.
        # We sum across all strategies — the venue doesn't know about
        # the strategy attribution.
        ours_by_asset: dict[str, Decimal] = {}
        for inst in self._tracked:
            # Sum across strategies for this instrument.
            qty_sum = Decimal(0)
            book_keys = [
                k for k in self._position_engine._books.keys()  # type: ignore[attr-defined]
                if k[1] == inst.instrument_id
            ]
            for k in book_keys:
                qty_sum += self._position_engine._books[k].quantity  # type: ignore[attr-defined]
            asset = inst.base_currency
            ours_by_asset[asset] = ours_by_asset.get(asset, Decimal(0)) + qty_sum

        comparisons: dict[str, tuple[Decimal, Decimal]] = {}
        # Compare for each tracked asset. Ignore assets the venue holds
        # but we don't track — that's normal (USDT, BNB, etc.).
        for asset, ours in ours_by_asset.items():
            venue = venue_balances.get(asset, Decimal(0))
            comparisons[asset] = (venue, ours)
            diff = venue - ours
            if abs(diff) > self._threshold:
                await self._publish_mismatch_alert(asset, venue, ours, diff)

        return comparisons

    # --- Alert -----------------------------------------------------------

    async def _publish_mismatch_alert(
        self, asset: str, venue: Decimal, ours: Decimal, diff: Decimal
    ) -> None:
        await self._bus.publish(
            Topic.ALERTS,
            RiskAlertEvent(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source="binance-reconciler",
                rule_name="balance_reconcile",
                severity=Severity.WARN,
                message=(
                    f"balance mismatch on {asset}: "
                    f"venue={venue}, ours={ours}, diff={diff}"
                ),
                metadata={
                    "asset": asset,
                    "venue_balance": str(venue),
                    "internal_position": str(ours),
                    "diff": str(diff),
                },
            ),
        )


__all__ = ["BalanceReconciler"]
