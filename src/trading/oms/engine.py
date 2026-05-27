"""Order Management System engine.

Flow:

1. Subscribe to ``signals`` and cache each by ``event_id``.
2. Subscribe to ``risk-decisions``. On an approved decision, look up
   the cached signal, route it through :class:`OrderRouter` to get an
   :class:`ExecutionAlgo`, and start the algo.
3. On every tick (and periodically), drive each active algo's
   ``next_slice``. When it returns a spec, build an
   :class:`OrderRequest`, publish it on the ``orders`` topic for the
   order_gateway.
4. Subscribe to ``orders`` for order_gateway responses (ack/reject/cancel)
   and to ``fills`` for fill events. Drive the per-order state machine.

Topic discipline:

- **OMS publishes**: ``OrderRequest`` / ``CancelRequest`` / ``AmendRequest``
  on ``orders``.
- **OrderGateway publishes**: ``OrderAcknowledged`` / ``OrderRejected`` /
  ``OrderCancelled`` on ``orders``; ``FillEvent`` on ``fills``.
- OMS subscribes to both, filters by isinstance. Self-publications are
  ignored naturally.

Signal cache eviction: signals older than ``signal_ttl_seconds`` are
dropped on each new signal arrival. Stops the cache growing unbounded.
"""

from __future__ import annotations

import structlog
from collections.abc import Iterable
from uuid import uuid4

from ..core.clock import Clock
from ..core.events import (
    BaseEvent,
    CancelRequest,
    EventId,
    FillEvent,
    OrderAcknowledged,
    OrderCancelled,
    OrderLeg,
    OrderRejected,
    OrderRequest,
    RiskDecision,
    SignalEvent,
)
from ..core.exceptions import BackpressureError, InvalidStateTransitionError, OrderNotFoundError
from ..core.types import (
    ClientOrderId,
    OrderId,
    OrderStatus,
    StrategyId,
)
from ..event_bus.base import AbstractEventBus, Topic
from .order import Order

_log = structlog.get_logger(__name__)

_NS_PER_SECOND = 1_000_000_000


class OMSEngine:
    """Order Management System."""

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        clock: Clock,
        source: str = "oms",
        signal_ttl_seconds: float = 300.0,
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._source = source
        self._signal_ttl_ns = int(signal_ttl_seconds * _NS_PER_SECOND)

        # Signal cache: event_id -> (signal, received_ns).
        # Insertion-ordered dict gives us cheap LRU-style eviction.
        self._signal_cache: dict[EventId, tuple[SignalEvent, int]] = {}

        # Active orders. Keyed by OrderId.
        self._orders: dict[OrderId, Order] = {}
        # Reverse lookup: client_order_id -> order_id. Lets the user-data
        # stream (and any other caller) resolve strategy attribution by
        # client order id without scanning all orders.
        self._coid_to_order_id: dict[ClientOrderId, OrderId] = {}

        self._started = False
        self._dropped_events: int = 0

    # --- Lifecycle --------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self._bus.subscribe(Topic.SIGNALS, self._on_signal)
        await self._bus.subscribe(Topic.RISK_DECISIONS, self._on_risk_decision)
        await self._bus.subscribe(Topic.ORDERS, self._on_order_event)
        await self._bus.subscribe(Topic.FILLS, self._on_fill)

    async def stop(self) -> None:
        self._started = False

    # --- Inbound handlers -------------------------------------------------

    async def _on_signal(self, event: BaseEvent) -> None:
        if not isinstance(event, SignalEvent):
            return
        self._evict_stale_signals()
        self._signal_cache[event.event_id] = (event, self._clock.now_ns())

    async def _on_risk_decision(self, event: BaseEvent) -> None:
        if not isinstance(event, RiskDecision):
            return
        # Pop from cache regardless — approved or not, we're done with it.
        entry = self._signal_cache.pop(event.signal_event_id, None)
        if entry is None:
            _log.warning(
                "risk_decision_references_stale_or_unknown_signal",
                signal_event_id=event.signal_event_id,
            )
            return
        signal, _ = entry
        if not event.approved:
            return

        # Rebuild the signal with only the risk-approved legs at approved sizes.
        # Keyed by leg_id so legs that share a side (e.g. price ladders) stay
        # distinct.
        approved_qty_by_leg = {
            al.leg_id: al.approved_quantity for al in event.approved_legs
        }
        approved_signal = SignalEvent(
            event_id=signal.event_id,
            ts_event=signal.ts_event,
            ts_ingest=signal.ts_ingest,
            source=signal.source,
            strategy_id=signal.strategy_id,
            instrument=signal.instrument,
            legs=tuple(
                OrderLeg(
                    leg_id=leg.leg_id,
                    side=leg.side,
                    price=leg.price,
                    quantity=approved_qty_by_leg[leg.leg_id],
                    order_type=leg.order_type,
                    time_in_force=leg.time_in_force,
                )
                for leg in signal.legs
                if leg.leg_id in approved_qty_by_leg
            ),
            atomic=signal.atomic,
            rationale=signal.rationale,
            metadata=signal.metadata,
        )
        await self._reconcile_quotes(approved_signal)

    async def _on_order_event(self, event: BaseEvent) -> None:
        """Handle order_gateway responses on the orders topic."""
        if isinstance(event, OrderAcknowledged):
            await self._handle_ack(event)
        elif isinstance(event, OrderRejected):
            await self._handle_reject(event)
        elif isinstance(event, OrderCancelled):
            await self._handle_cancel(event)
        # OrderRequest / CancelRequest / AmendRequest: our own messages;
        # ignore.

    async def _on_fill(self, event: BaseEvent) -> None:
        if not isinstance(event, FillEvent):
            return
        order = self._orders.get(event.order_id)
        if order is None:
            _log.warning("fill_for_unknown_order_id_ignoring", order_id=event.order_id)
            return
        applied = order.apply_fill(event)
        if not applied:
            _log.info("duplicate_fill_ignored", fill_id=event.fill_id)

    # --- OrderGateway-response branches ----------------------------------------

    async def _handle_ack(self, event: OrderAcknowledged) -> None:
        order = self._orders.get(event.order_id)
        if order is None:
            return
        try:
            order.transition_to(OrderStatus.ACKNOWLEDGED, at_ns=event.ts_event)
        except InvalidStateTransitionError:
            # Already moved past ACKNOWLEDGED (e.g. instant fill). Ignore.
            return
        order.exchange_order_id = event.exchange_order_id

    async def _handle_reject(self, event: OrderRejected) -> None:
        order = self._orders.get(event.order_id)
        if order is None:
            return
        try:
            order.transition_to(OrderStatus.REJECTED, at_ns=event.ts_event)
        except InvalidStateTransitionError:
            return
        order.reject_reason = event.reason

    async def _handle_cancel(self, event: OrderCancelled) -> None:
        order = self._orders.get(event.order_id)
        if order is None:
            return
        try:
            order.transition_to(OrderStatus.CANCELLED, at_ns=event.ts_event)
        except InvalidStateTransitionError:
            return

    # --- Quote reconciliation and placement ------------------------------

    async def _reconcile_quotes(self, signal: SignalEvent) -> None:
        """Reconcile open orders against the strategy's desired leg state.

        ``signal.legs`` is the complete set of orders the strategy wants
        resting for this instrument right now. The diff:

        - Open orders that match a desired leg's ``(side, price, leaves)``
          stay put (preserves queue position).
        - Desired legs with no match cause a fresh place.
        - Open orders with no match get cancelled (withdrawn or stale).

        Matching is per-leg rather than per-side so a price ladder with
        multiple legs on the same side reconciles correctly.
        """
        sid = signal.strategy_id
        iid = signal.instrument.instrument_id
        active = (OrderStatus.PENDING_NEW, OrderStatus.ACKNOWLEDGED, OrderStatus.PARTIALLY_FILLED)

        open_orders: list[Order] = [
            o for o in self._orders.values()
            if o.strategy_id == sid
            and o.instrument.instrument_id == iid
            and o.status in active
        ]

        # Greedily pair each desired leg with one open order at the same
        # (side, price, leaves). Pairing is one-to-one — a given open order
        # can only satisfy one leg.
        unmatched_open: set[OrderId] = {o.order_id for o in open_orders}
        legs_to_place: list[OrderLeg] = []

        for leg in signal.legs:
            match = next(
                (o for o in open_orders
                 if o.order_id in unmatched_open
                 and o.side == leg.side
                 and o.price == leg.price
                 and o.leaves_quantity == leg.quantity),
                None,
            )
            if match is not None:
                unmatched_open.discard(match.order_id)
            else:
                legs_to_place.append(leg)

        # Anything still unmatched is stale or withdrawn — cancel it.
        for order in open_orders:
            if order.order_id in unmatched_open:
                await self._safe_cancel(order.order_id, why="reconcile_stale_or_withdrawn")

        # Place legs that had no matching open order.
        for leg in legs_to_place:
            await self._place_quote(signal, leg)

    async def _safe_cancel(self, order_id: OrderId, *, why: str) -> None:
        """Cancel an order, tolerating the race where it terminalized first."""
        try:
            await self.cancel_order(order_id)
        except OrderNotFoundError:
            # Order finished between our snapshot and the cancel call. Benign.
            return
        except BackpressureError:
            # Bus is full; the cancel didn't ship. Loud — we may keep
            # trying to place fresh orders against a stale book.
            _log.warning("reconcile_cancel_backpressure", order_id=order_id, reason=why)
        except Exception:
            _log.exception("reconcile_cancel_failed", order_id=order_id, reason=why)

    async def _place_quote(self, signal: SignalEvent, leg: OrderLeg) -> None:
        """Submit a single leg from a reconciled SignalEvent."""
        order_id = OrderId(uuid4())
        client_order_id = ClientOrderId(
            f"{signal.strategy_id}-{order_id.hex[:12]}"
        )
        order = Order(
            order_id=order_id,
            client_order_id=client_order_id,
            strategy_id=signal.strategy_id,
            instrument=signal.instrument,
            side=leg.side,
            order_type=leg.order_type,
            quantity=leg.quantity,
            price=leg.price,
            time_in_force=leg.time_in_force,
            created_at_ns=self._clock.now_ns(),
            parent_order_id=None,
        )
        self._orders[order_id] = order
        self._coid_to_order_id[client_order_id] = order_id

        req = OrderRequest(
            ts_event=self._clock.now_ns(),
            ts_ingest=self._clock.now_ns(),
            source=self._source,
            order_id=order_id,
            client_order_id=client_order_id,
            strategy_id=signal.strategy_id,
            instrument=signal.instrument,
            side=leg.side,
            order_type=leg.order_type,
            quantity=leg.quantity,
            price=leg.price,
            time_in_force=leg.time_in_force,
        )
        if not await self._safe_publish(Topic.ORDERS, req):
            order.transition_to(OrderStatus.REJECTED, at_ns=self._clock.now_ns())
            order.reject_reason = "dropped: bus backpressure on orders topic"

    async def cancel_order(self, order_id: OrderId) -> None:
        """Send a CancelRequest for a single order."""
        order = self._orders.get(order_id)
        if order is None:
            raise OrderNotFoundError("unknown order_id", order_id=str(order_id))
        if order.is_terminal:
            return
        try:
            order.transition_to(OrderStatus.PENDING_CANCEL, at_ns=self._clock.now_ns())
        except InvalidStateTransitionError:
            return
        await self._safe_publish(
            Topic.ORDERS,
            CancelRequest(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self._source,
                order_id=order_id,
                client_order_id=order.client_order_id,
                instrument=order.instrument,
            ),
        )

    # --- Read API --------------------------------------------------------

    def get_order(self, order_id: OrderId) -> Order | None:
        return self._orders.get(order_id)

    def open_orders(self) -> Iterable[Order]:
        return (o for o in self._orders.values() if not o.is_terminal)

    def strategy_id_for_client_order(
        self, coid: ClientOrderId
    ) -> StrategyId | None:
        """Return the strategy that owns ``coid``, or None if unknown.

        Used by BinanceUserDataStream to stamp fills with the correct
        strategy id without coupling the stream directly to OMS internals.
        """
        order_id = self._coid_to_order_id.get(coid)
        if order_id is None:
            return None
        order = self._orders.get(order_id)
        return order.strategy_id if order is not None else None

    # --- Metrics ---------------------------------------------------------

    def snapshot(self) -> dict:
        """Return a point-in-time dict of operational counters."""
        open_orders = sum(1 for o in self._orders.values() if not o.is_terminal)
        return {
            "open_orders": open_orders,
            "total_orders": len(self._orders),
            "dropped_events": self._dropped_events,
        }

    # --- Helpers ---------------------------------------------------------

    async def _safe_publish(self, topic: str, event: BaseEvent) -> bool:
        """Publish to the bus; absorb BackpressureError and return False if dropped."""
        try:
            await self._bus.publish(topic, event)
            return True
        except BackpressureError as exc:
            self._dropped_events += 1
            _log.critical(
                "bus_backpressure_oms_event_dropped",
                total_drops=self._dropped_events, topic=topic,
                event_type=type(event).__name__,
            )
            return False

    def _evict_stale_signals(self) -> None:
        cutoff = self._clock.now_ns() - self._signal_ttl_ns
        # Insertion-ordered. Drop from the front while older than cutoff.
        stale_keys: list[EventId] = []
        for eid, (_, received_ns) in self._signal_cache.items():
            if received_ns >= cutoff:
                break
            stale_keys.append(eid)
        for eid in stale_keys:
            self._signal_cache.pop(eid, None)


__all__ = ["OMSEngine"]
