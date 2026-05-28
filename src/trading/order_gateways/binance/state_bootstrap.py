"""Startup + periodic reconciliation of venue order/position state.

The system treats its own in-memory order/position state as a cache that
must be rebuilt from the venue, never as authoritative. On startup this
adopts everything the venue already has (orders left resting across a
restart, positions held, orders placed by a human) so the system can
recover mid-trade. A background task then periodically re-pulls and
reconciles to repair drift from missed user-data-stream events.

Attribution policy:
- Orders: parse the venue ``clientOrderId``. If it matches our minting
  scheme it is adopted to its original strategy; otherwise EXTERNAL.
- Positions: the venue reports net-per-symbol only, with no per-strategy
  breakdown, so adopted positions go to EXTERNAL. This is lossy and
  unavoidable; strategies rebuild their own view as they trade.

Reconciliation (periodic):
- Venue has an order we don't track  -> adopt it.
- We track an open order the venue no longer reports -> it filled or was
  cancelled during a gap; terminalize it locally (cancelled).
- Position drift beyond a threshold -> alert only (do not silently rewrite
  positions; a wrong position is a loud problem, not a papered-over one).
"""

from __future__ import annotations

import asyncio
import structlog
from decimal import Decimal
from typing import Final
from ...core.clock import Clock
from ...core.events import VenuePosition, VenuePositionSnapshotEvent
from ...core.instruments import Instrument
from ...core.types import (
    ClientOrderId,
    ExchangeOrderId,
    OrderStatus,
    OrderType,
    Quantity,
    Price,
    TimeInForce,
)
from ...event_bus.base import AbstractEventBus, Topic
from ...oms.engine import OMSEngine
from .config import BinanceConfig
from .order_translation import side_from_binance
from .rest_client import BinanceRESTClient
from .symbols import SymbolMapper

_log = structlog.get_logger(__name__)

_W_OPEN_ORDERS: Final[float] = 3.0
_W_POSITION_RISK: Final[float] = 5.0


def _order_type_from_binance(binance_type: str, tif: str) -> tuple[OrderType, TimeInForce]:
    """Best-effort inverse of order_type_to_binance for adoption/display.

    Adopted orders are not re-managed by a strategy, so exact fidelity is
    less critical than for placement; we map to the closest canonical pair.
    """
    t = binance_type.upper()
    tif_map = {
        "GTC": TimeInForce.GTC, "IOC": TimeInForce.IOC,
        "FOK": TimeInForce.FOK, "GTX": TimeInForce.GTX,
    }
    our_tif = tif_map.get(tif.upper(), TimeInForce.GTC)
    if t == "MARKET":
        return OrderType.MARKET, our_tif
    if t in ("LIMIT_MAKER",) or (t == "LIMIT" and tif.upper() == "GTX"):
        return OrderType.POST_ONLY, our_tif
    if t == "LIMIT":
        return OrderType.LIMIT, our_tif
    if t in ("STOP", "STOP_LOSS"):
        return OrderType.STOP, our_tif
    if t in ("STOP_LOSS_LIMIT", "STOP_LIMIT"):
        return OrderType.STOP_LIMIT, our_tif
    return OrderType.LIMIT, our_tif


class StateBootstrapper:
    """Adopts venue order/position state at startup and reconciles it
    periodically thereafter."""

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        clock: Clock,
        config: BinanceConfig,
        rest: BinanceRESTClient,
        oms: OMSEngine,
        symbols: SymbolMapper,
        tracked_instruments: list[Instrument],
        resync_interval_seconds: float = 30.0,
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._config = config
        self._rest = rest
        self._oms = oms
        self._symbols = symbols
        self._tracked = list(tracked_instruments)
        self._resync_interval = resync_interval_seconds
        self._task: asyncio.Task[None] | None = None
        self._stop = False

    # --- Lifecycle --------------------------------------------------------

    async def start(self) -> None:
        """Run the one-shot bootstrap, then launch the periodic resync."""
        await self.bootstrap()
        self._stop = False
        self._task = asyncio.create_task(self._resync_loop(), name="binance-state-resync")

    async def stop(self) -> None:
        self._stop = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # --- Bootstrap (one-shot, at startup) --------------------------------

    async def bootstrap(self) -> None:
        """Adopt venue orders and publish venue net positions. Idempotent."""
        await self._adopt_open_orders()
        await self._publish_venue_positions()

    async def _adopt_open_orders(self) -> set[str]:
        """Fetch venue open orders and adopt any we don't already track.

        Returns the set of venue client_order_ids seen, so the periodic
        resync can detect locally-open orders the venue no longer reports.
        """
        seen_coids: set[str] = set()
        for inst in self._tracked:
            wire = self._symbols.wire_symbol(inst)
            try:
                orders = await self._rest.request(
                    "GET", self._config.api_prefix + "/openOrders",
                    params={"symbol": wire}, signed=True, weight=_W_OPEN_ORDERS,
                )
            except Exception:
                _log.exception("bootstrap_failed_to_list_open_orders", wire_symbol=wire)
                continue
            for o in orders:
                coid = str(o.get("clientOrderId") or "")
                if not coid:
                    continue
                seen_coids.add(coid)
                try:
                    qty = Quantity(Decimal(str(o["origQty"])))
                    executed = Quantity(Decimal(str(o.get("executedQty", "0"))))
                    price_raw = o.get("price")
                    price = (
                        Price(Decimal(str(price_raw)))
                        if price_raw not in (None, "", "0", "0.0")
                        else None
                    )
                    order_type, tif = _order_type_from_binance(
                        str(o.get("type", "LIMIT")), str(o.get("timeInForce", "GTC")),
                    )
                    await self._oms.adopt_order(
                        instrument=inst,
                        client_order_id=ClientOrderId(coid),
                        side=side_from_binance(str(o["side"])),
                        order_type=order_type,
                        quantity=qty,
                        cumulative_filled=executed,
                        price=price,
                        time_in_force=tif,
                        exchange_order_id=ExchangeOrderId(str(o["orderId"])),
                        created_at_ns=int(o.get("time", 0)) * 1_000_000,
                    )
                except Exception:
                    _log.exception("bootstrap_failed_to_adopt_order", coid=coid, wire_symbol=wire)
        return seen_coids

    async def _publish_venue_positions(self) -> None:
        """Pull the venue's net positions and publish them as ground truth.

        We do NOT synthesize fills into the PositionEngine — that would
        corrupt per-strategy books and PnL with a position no strategy
        actually traded. Instead we publish the venue's net per instrument
        verbatim on its own topic; the dashboard shows it as the 'net' row
        alongside the per-strategy fill-derived rows. Futures only — spot
        has no positionRisk endpoint (net there comes from wallet balance).
        """
        if not self._config.futures:
            return
        try:
            positions = await self._rest.request(
                "GET", "/fapi/v2/positionRisk", signed=True, weight=_W_POSITION_RISK,
            )
        except Exception:
            _log.exception("failed_to_list_venue_positions")
            return
        venue: list[VenuePosition] = []
        for p in positions:
            inst = self._symbols.by_wire(str(p.get("symbol") or ""))
            if inst is None:
                continue
            amt = Decimal(str(p.get("positionAmt", "0")))
            if amt == 0:
                continue
            entry = Decimal(str(p.get("entryPrice", "0")))
            mark = Decimal(str(p.get("markPrice", "0"))) or entry
            upnl = Decimal(str(p.get("unRealizedProfit", "0")))
            venue.append(VenuePosition(
                instrument=inst,
                net_quantity=Quantity(amt),
                entry_price=Price(entry),
                mark_price=Price(mark),
                unrealized_pnl=Price(upnl),
            ))
        await self._bus.publish(
            Topic.VENUE_POSITIONS,
            VenuePositionSnapshotEvent(
                ts_event=self._clock.now_ns(), ts_ingest=self._clock.now_ns(),
                source="binance-state-bootstrap",
                positions=tuple(venue),
            ),
        )

    # --- Periodic resync --------------------------------------------------

    async def _resync_loop(self) -> None:
        while not self._stop:
            try:
                await asyncio.sleep(self._resync_interval)
            except asyncio.CancelledError:
                return
            try:
                await self._resync_once()
            except Exception:
                _log.exception("state_resync_failed_will_retry")

    async def _resync_once(self) -> None:
        """Reconcile local open orders to the venue and refresh venue positions."""
        seen_coids = await self._adopt_open_orders()
        # Terminalize any locally-open order the venue no longer reports.
        for order in list(self._oms.open_orders()):
            if str(order.client_order_id) not in seen_coids:
                _log.warning(
                    "resync_order_gone_from_venue_terminalizing",
                    order_id=str(order.order_id),
                    client_order_id=str(order.client_order_id),
                )
                await self._terminalize(order)
        # Refresh the venue net positions (ground-truth row on the dashboard).
        await self._publish_venue_positions()

    async def _terminalize(self, order) -> None:
        """Mark a locally-open order cancelled — the venue no longer has it."""
        try:
            order.transition_to(OrderStatus.CANCELLED, at_ns=self._clock.now_ns())
        except Exception:
            # Already terminal or illegal transition — nothing to do.
            return
        await self._oms._publish_open_orders()


__all__ = ["StateBootstrapper"]
