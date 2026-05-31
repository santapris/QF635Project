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
- We track an open order the venue no longer reports (for several passes)
  -> resolve its actual status via /allOrders and terminalize to that
  (FILLED / CANCELLED / EXPIRED). Never assume cancelled: an order absent
  from /openOrders may have filled. The user-data stream is the primary
  path for terminal transitions; this is a backstop for missed events.
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
from ...oms.engine import EXTERNAL_STRATEGY_ID, OMSEngine, strategy_id_from_client_order_id
from .config import BinanceConfig
from .order_translation import side_from_binance
from .rest_client import BinanceRESTClient
from .symbols import SymbolMapper

_log = structlog.get_logger(__name__)

_W_OPEN_ORDERS: Final[float] = 3.0
_W_POSITION_RISK: Final[float] = 5.0
# GET /allOrders is heavier than a single /order lookup (weight ~5 on futures)
# but resolves *every* gone order for a symbol in one call, so the cost is paid
# per-symbol rather than per-order — strictly cheaper in the case that matters
# (a stream outage drops many orders at once).
_W_ALL_ORDERS: Final[float] = 5.0

# How many consecutive resync passes an order must be absent from /openOrders
# before we query the venue to terminalize it. The user-data stream is the
# primary path for terminal transitions; a single missed snapshot (a paging
# gap, a brief stream hiccup that reconnect+replay heals) must not trigger a
# query. Only *persistent* absence does. With a 30s resync this is ~1 minute
# of confirmed absence before we spend a REST call.
_GONE_THRESHOLD: Final[int] = 2

# How far back to ask /allOrders to look. Must comfortably exceed
# _GONE_THRESHOLD * resync_interval so a just-terminalized order is still in
# the window when we finally query for it.
_ALL_ORDERS_LOOKBACK_MS: Final[int] = 10 * 60 * 1000

# Binance order statuses that mean "no longer working", mapped to our terminal
# states. Anything not listed (NEW, PARTIALLY_FILLED, PENDING_*) is still live
# and must NOT terminalize. FILLED is the critical one: an order gone from
# /openOrders because it filled must be recorded as FILLED, never CANCELLED.
_VENUE_TERMINAL_STATUS: Final[dict[str, OrderStatus]] = {
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELLED,
    "CANCELLED": OrderStatus.CANCELLED,
    "EXPIRED": OrderStatus.EXPIRED,
    "EXPIRED_IN_MATCH": OrderStatus.EXPIRED,
    "REJECTED": OrderStatus.REJECTED,
}


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
        # client_order_id -> number of consecutive resync passes the order has
        # been absent from /openOrders. An order must be gone for
        # ``_GONE_THRESHOLD`` passes before we spend a REST call to terminalize
        # it; seeing it (or it becoming terminal) resets the count.
        self._consecutive_gone: dict[str, int] = {}

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

    async def _adopt_open_orders(self) -> tuple[set[str], set[str]]:
        """Fetch venue open orders and adopt any we don't already track.

        Returns ``(seen_coids, fetched_iids)``:
        - ``seen_coids`` is every venue client_order_id observed this pass, so
          the periodic resync can detect locally-open orders the venue no
          longer reports.
        - ``fetched_iids`` is the set of instrument_ids whose ``/openOrders``
          GET *succeeded*. The resync must only judge "gone from venue" against
          instruments we actually fetched — a transient GET failure must not be
          read as "the venue has no orders for this symbol", or we would
          terminalize live orders on a network blip.
        """
        seen_coids: set[str] = set()
        fetched_iids: set[str] = set()
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
            fetched_iids.add(inst.instrument_id)
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
                    strategy_id = strategy_id_from_client_order_id(coid)
                    tracked_locally = ClientOrderId(coid) in self._oms._coid_to_order_id
                    _log.warning(
                        "venue_order_resync",
                        client_order_id=coid,
                        exchange_order_id=str(o.get("orderId")),
                        symbol=wire,
                        side=str(o.get("side")),
                        qty=str(qty),
                        price=str(price),
                        strategy_id=str(strategy_id),
                        external=strategy_id == EXTERNAL_STRATEGY_ID,
                        tracked_locally=tracked_locally,
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
        return seen_coids, fetched_iids

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
        """Reconcile local open orders to the venue and refresh venue positions.

        Adoption is verbatim: any venue order we don't track is adopted into the
        OMS in the state the venue reports, and the per-strategy reconciliation
        loop decides on its next tick whether to keep it (matches a desired leg)
        or cancel it (matches none). We deliberately do NOT classify orders as
        "orphans" to cancel here — the same desired-leg matching that governs
        self-placed orders is the single source of truth for what should rest,
        and adopted orders flow through it identically. Source bugs that used to
        create orphans (e.g. a futures GTX amend that cancelled the order yet
        was published as OrderAmended) are fixed at the source instead.

        The "gone from venue" handling lives in ``_terminalize_gone_orders``:
        only orders persistently absent from ``/openOrders`` are resolved against
        the venue's actual status (never assumed CANCELLED, since a gone order
        may have filled), and only for symbols we successfully fetched.
        """
        seen_coids, fetched_iids = await self._adopt_open_orders()
        await self._terminalize_gone_orders(seen_coids, fetched_iids)
        # Refresh the venue net positions (ground-truth row on the dashboard).
        await self._publish_venue_positions()

    async def _terminalize_gone_orders(
        self, seen_coids: set[str], fetched_iids: set[str]
    ) -> None:
        """Terminalize locally-open orders that have persistently left /openOrders.

        An order leaves /openOrders for two very different reasons: it was
        cancelled/expired, OR it *filled*. Assuming CANCELLED corrupts a filled
        order's terminal state and — because the OMS drops fills on terminal
        orders — silently swallows the fill when the user-data stream finally
        delivers it. So we resolve each gone order's *actual* status against the
        venue before terminalizing.

        Two guards keep this cheap and safe:

        - **Staleness.** The user-data stream is the primary path for terminal
          transitions; this resync is a backstop for missed events. We only act
          on orders absent for ``_GONE_THRESHOLD`` consecutive passes, so a
          single missing snapshot (a paging gap, a brief stream hiccup healed by
          reconnect+replay) costs nothing. Persistent absence is the signal.
        - **Scope.** Only instruments whose ``/openOrders`` GET succeeded this
          pass (``fetched_iids``); a failed GET tells us nothing about that
          symbol and must change nothing, or a network blip terminalizes live
          orders. PENDING_* orders are in-flight (REST round-trip not done) so
          their absence is expected — never "gone".

        Persistent-gone orders are resolved per *symbol* via one ``/allOrders``
        call, not one ``/order`` per order, so a stream outage that drops many
        orders at once costs one weighted call per symbol rather than N.
        """
        _in_flight = (
            OrderStatus.PENDING_NEW,
            OrderStatus.PENDING_AMEND,
            OrderStatus.PENDING_CANCEL,
        )
        # Bucket persistently-gone orders by instrument so we query each symbol
        # once. Update the consecutive-gone counter for every open order.
        gone_by_inst: dict[str, list] = {}
        live_coids: set[str] = set()
        for order in self._oms.open_orders():
            coid = str(order.client_order_id)
            if order.instrument.instrument_id not in fetched_iids:
                continue  # didn't fetch this symbol — say nothing about it
            if order.status in _in_flight or coid in seen_coids:
                live_coids.add(coid)
                continue
            n = self._consecutive_gone.get(coid, 0) + 1
            self._consecutive_gone[coid] = n
            if n >= _GONE_THRESHOLD:
                gone_by_inst.setdefault(order.instrument.instrument_id, []).append(order)

        # Reset the counter for anything we saw live this pass, and drop entries
        # for orders no longer open (terminalized, or vanished) so the map can't
        # grow unbounded.
        open_coids = {str(o.client_order_id) for o in self._oms.open_orders()}
        for coid in list(self._consecutive_gone):
            if coid in live_coids or coid not in open_coids:
                self._consecutive_gone.pop(coid, None)

        for orders in gone_by_inst.values():
            await self._resolve_gone_orders(orders)

    async def _resolve_gone_orders(self, orders: list) -> None:
        """Resolve a symbol's persistently-gone orders via one /allOrders call.

        Conservatism on uncertainty: if the query fails, or an order's id is not
        in the returned window, or its status is one we don't recognise as
        terminal, we change nothing and retry next pass. A live order must never
        be terminalized on a guess.
        """
        wire = self._symbols.wire_symbol(orders[0].instrument)
        rows = await self._fetch_recent_orders(wire)
        if rows is None:
            _log.warning(
                "resync_allorders_query_failed_leaving_open",
                wire_symbol=wire, count=len(orders),
            )
            return  # could not read venue state — leave every candidate open
        by_coid = {str(r.get("clientOrderId") or ""): r for r in rows}
        for order in orders:
            coid = str(order.client_order_id)
            row = by_coid.get(coid)
            if row is None:
                # Not in the lookback window — too old to confirm. Leave open;
                # this is the rare case where a per-order /order lookup would be
                # the fallback, but it should not happen within the window.
                _log.warning(
                    "resync_gone_order_not_in_allorders_window_leaving_open",
                    client_order_id=coid, wire_symbol=wire,
                )
                continue
            venue_status = str(row.get("status", "")).upper()
            terminal = _VENUE_TERMINAL_STATUS.get(venue_status)
            if terminal is None:
                # Venue still considers it live; it was just absent from the
                # /openOrders pages we read. Reset its gone counter.
                self._consecutive_gone.pop(coid, None)
                _log.info(
                    "resync_gone_order_venue_reports_live_leaving_open",
                    client_order_id=coid, venue_status=venue_status,
                )
                continue
            try:
                executed = Quantity(Decimal(str(row.get("executedQty", "0"))))
            except (ArithmeticError, ValueError):
                executed = None
            _log.warning(
                "resync_order_gone_from_venue_terminalizing",
                order_id=str(order.order_id),
                client_order_id=coid,
                venue_status=venue_status,
                terminal_status=terminal.value,
                executed_qty=str(executed),
            )
            await self._oms.terminalize_from_venue(
                order.order_id, status=terminal, cumulative_filled=executed,
            )
            self._consecutive_gone.pop(coid, None)

    async def _fetch_recent_orders(self, wire_symbol: str) -> list | None:
        """Fetch a symbol's recent orders (incl. terminal), or None on failure.

        None means "we could not determine venue state" — the caller treats it
        as 'change nothing', never as 'cancelled'.
        """
        start_ms = self._clock.now_ns() // 1_000_000 - _ALL_ORDERS_LOOKBACK_MS
        try:
            rows = await self._rest.request(
                "GET", self._config.api_prefix + "/allOrders",
                params={"symbol": wire_symbol, "startTime": start_ms},
                signed=True, weight=_W_ALL_ORDERS,
            )
        except Exception:
            _log.exception("resync_allorders_failed", wire_symbol=wire_symbol)
            return None
        return rows if isinstance(rows, list) else None


__all__ = ["StateBootstrapper"]
