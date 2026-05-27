"""Balance reconciliation.

Periodically polls ``{api_prefix}/account`` and compares the reported
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
import structlog
from decimal import Decimal
from typing import Final

from ...core.clock import Clock
from ...core.events import AccountBalance, AccountSnapshotEvent, RiskAlertEvent
from ...core.instruments import Instrument
from ...core.types import Severity, StrategyId
from ...event_bus.base import AbstractEventBus, Topic
from ...position.engine import PositionEngine
from .config import BinanceConfig
from .rest_client import BinanceRESTClient

_log = structlog.get_logger(__name__)

_W_ACCOUNT: Final[float] = 10.0  # Binance weight for account info endpoint. Not in official docs but observed empirically. We set it higher than the observed 5 to be safe; if we get throttled we want to back off more aggressively. If Binance changes this, we'll get a 429 and can adjust accordingly.


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
        # Subscriber-ordering invariant: the runner starts the dashboard
        # (and any other AccountSnapshotEvent subscriber) before this
        # reconciler, so the first reconcile_once() lands after they're
        # registered. See trading.runners.examples.binance_testnet.
        interval = self._config.reconcile_interval_seconds
        while not self._stop:
            try:
                await self.reconcile_once()
            except asyncio.CancelledError:
                return
            except Exception:
                _log.exception("balance_reconcile_failed_will_retry_next_cycle")
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                return

    async def reconcile_once(self) -> dict[str, tuple[Decimal, Decimal]]:
        """One reconciliation pass. Returns ``{asset: (venue, ours)}``.

        Also publishes an ``AccountSnapshotEvent`` so the dashboard can
        display balances without waiting for the user-data stream's first
        push (which only fires on balance changes). Public so an operator
        script can call it on demand.
        """
        account = await self._rest.request(
            "GET", self._config.account_path,
            signed=True, weight=_W_ACCOUNT,
        )
        # Spot returns "balances" with free/locked; Futures returns "assets"
        # with walletBalance. Pick the schema by which top-level key exists,
        # then key into each row by *membership* rather than truthiness — a
        # legitimate "0" string is truthy in Python but Decimal("0") is not,
        # so `b.get("free") or b.get("walletBalance")` would misbehave on
        # futures rows that happen to also carry a zero "free".
        if "balances" in account:
            balances_raw = account.get("balances", []) or []
            schema = "spot"
        else:
            balances_raw = account.get("assets", []) or []
            schema = "futures"

        venue_balances: dict[str, Decimal] = {}
        snapshot_balances: list[AccountBalance] = []
        for b in balances_raw:
            asset = b["asset"]
            if schema == "spot":
                free = Decimal(str(b.get("free", "0")))
                locked = Decimal(str(b.get("locked", "0")))
            else:
                free = Decimal(str(b.get("walletBalance", "0")))
                locked = Decimal("0")
            if free + locked > 0:
                venue_balances[asset] = free + locked
                snapshot_balances.append(
                    AccountBalance(asset=asset, free=free, locked=locked)
                )
        await self._publish_account_snapshot(snapshot_balances)

        # Aggregate our positions by base-currency asset.
        # We sum across all strategies — the venue doesn't know about
        # the strategy attribution.
        all_books = self._position_engine.get_all_books()
        ours_by_asset: dict[str, Decimal] = {}
        for inst in self._tracked:
            qty_sum = sum(
                (book.quantity for k, book in all_books.items() if k[1] == inst.instrument_id),
                Decimal(0),
            )
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

    async def _publish_account_snapshot(
        self, balances: list[AccountBalance]
    ) -> None:
        await self._bus.publish(
            Topic.ACCOUNT,
            AccountSnapshotEvent(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source="binance-reconciler",
                balances=tuple(balances),
            ),
        )


__all__ = ["BalanceReconciler"]
