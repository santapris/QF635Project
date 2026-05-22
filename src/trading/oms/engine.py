"""Order Management System engine.

Flow:

1. Subscribe to ``signals`` and cache each by ``event_id``.
2. Subscribe to ``risk-decisions``. On an approved decision, look up
   the cached signal, route it through :class:`OrderRouter` to get an
   :class:`ExecutionAlgo`, and start the algo.
3. On every tick (and periodically), drive each active algo's
   ``next_slice``. When it returns a spec, build an
   :class:`OrderRequest`, publish it on the ``orders`` topic for the
   gateway.
4. Subscribe to ``orders`` for gateway responses (ack/reject/cancel)
   and to ``fills`` for fill events. Drive the per-order state machine.

Topic discipline:

- **OMS publishes**: ``OrderRequest`` / ``CancelRequest`` / ``AmendRequest``
  on ``orders``.
- **Gateway publishes**: ``OrderAcknowledged`` / ``OrderRejected`` /
  ``OrderCancelled`` on ``orders``; ``FillEvent`` on ``fills``.
- OMS subscribes to both, filters by isinstance. Self-publications are
  ignored naturally.

Signal cache eviction: signals older than ``signal_ttl_seconds`` are
dropped on each new signal arrival. Stops the cache growing unbounded.
"""

from __future__ import annotations

import logging
from collections import deque
from collections.abc import Iterable
from decimal import Decimal
from uuid import uuid4

from ..core.clock import Clock
from ..core.events import (
    BaseEvent,
    CancelRequest,
    EventId,
    FillEvent,
    OrderAcknowledged,
    OrderCancelled,
    OrderRejected,
    OrderRequest,
    RiskDecision,
    SignalEvent,
    TickEvent,
)
from ..core.exceptions import InvalidStateTransitionError, OrderNotFoundError
from ..core.types import (
    ClientOrderId,
    OrderId,
    OrderStatus,
    Side,
    StrategyId,
)
from ..event_bus.base import AbstractEventBus, Topic
from .execution_algos import ChildOrderSpec, ExecutionAlgo
from .order import Order
from .router import OrderRouter

_log = logging.getLogger(__name__)

_NS_PER_SECOND = 1_000_000_000


class OMSEngine:
    """Order Management System."""

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        clock: Clock, # dont need to change this as when the engine file is deployed for production, it will signal LiveClock for the production code
        router: OrderRouter | None = None,
        source: str = "oms",
        signal_ttl_seconds: float = 300.0,
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._router = router or OrderRouter()
        self._source = source
        self._signal_ttl_ns = int(signal_ttl_seconds * _NS_PER_SECOND)

        # Signal cache: event_id -> (signal, received_ns).
        # Insertion-ordered dict gives us cheap LRU-style eviction.
        self._signal_cache: dict[EventId, tuple[SignalEvent, int]] = {}

        # Active orders. Keyed by OrderId.
        self._orders: dict[OrderId, Order] = {}
        # Algos keyed by parent order id.
        self._algos: dict[OrderId, ExecutionAlgo] = {}
        # Parent metadata for child orders (signal, strategy, instrument).
        # Each algo's children share the parent's identity.
        self._parents: dict[OrderId, SignalEvent] = {}

        self._started = False

    # --- Lifecycle --------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self._bus.subscribe(Topic.SIGNALS, self._on_signal)
        await self._bus.subscribe(Topic.RISK_DECISIONS, self._on_risk_decision)
        await self._bus.subscribe(Topic.ORDERS, self._on_order_event)
        await self._bus.subscribe(Topic.FILLS, self._on_fill)
        await self._bus.subscribe(Topic.MARKET_DATA, self._on_market_data)

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
                "risk decision references signal we never saw or it expired: %s",
                event.signal_event_id,
            )
            return
        signal, _ = entry
        if not event.approved:
            # Risk said no. Nothing for us to do.
            return

        # Approved size may have been clamped by the risk engine.
        approved_qty = event.approved_quantity or signal.target_quantity
        try:
            algo = self._router.route(
                signal, approved_quantity=approved_qty, now_ns=self._clock.now_ns()
            )
        except Exception:
            _log.exception(
                "router failed to construct algo; dropping signal %s",
                signal.event_id,
            )
            return

        # Mint a parent order id solely as an identity for the algo
        # session. Parent orders themselves are never sent to the
        # gateway; only their children are. The OMS uses the parent id
        # to group children and cancel the algo wholesale if needed.
        parent_id = OrderId(uuid4())
        self._algos[parent_id] = algo
        self._parents[parent_id] = signal

        # Kick the algo immediately — many algos (Immediate, the first
        # TWAP slice) emit on their first call.
        await self._drive_algo(parent_id)

    async def _on_order_event(self, event: BaseEvent) -> None:
        """Handle gateway responses on the orders topic."""
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
            _log.warning("fill for unknown order_id %s; ignoring", event.order_id)
            return
        applied = order.apply_fill(event)
        if not applied:
            _log.info("duplicate fill %s ignored", event.fill_id)
            return
        if order.parent_order_id is not None:
            algo = self._algos.get(order.parent_order_id)
            if algo is not None:
                try:
                    algo.on_fill(event)
                except Exception:
                    _log.exception("algo.on_fill raised; ignoring")
                # If parent is finished (algo done + no leaves), tidy up.
                if algo.is_done() and self._parent_has_leaves(order.parent_order_id) == Decimal(0):
                    self._algos.pop(order.parent_order_id, None)
                    self._parents.pop(order.parent_order_id, None)

    async def _on_market_data(self, event: BaseEvent) -> None:
        if not isinstance(event, TickEvent):
            return
        # Drive each algo. We notify them of the tick and then ask for
        # the next slice.
        for parent_id, algo in list(self._algos.items()):
            try:
                algo.on_tick(event)
            except Exception:
                _log.exception("algo.on_tick raised; ignoring")
                continue
            await self._drive_algo(parent_id)

    # --- Gateway-response branches ----------------------------------------

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
        # If this was a child order in an algo, terminate the algo —
        # a rejected slice almost always means the rest will be rejected
        # too (insufficient margin, bad symbol, etc.).
        if order.parent_order_id is not None:
            algo = self._algos.pop(order.parent_order_id, None)
            self._parents.pop(order.parent_order_id, None)
            if algo is not None:
                algo.cancel()

    async def _handle_cancel(self, event: OrderCancelled) -> None:
        order = self._orders.get(event.order_id)
        if order is None:
            return
        try:
            order.transition_to(OrderStatus.CANCELLED, at_ns=event.ts_event)
        except InvalidStateTransitionError:
            return

    # --- Algo driving ----------------------------------------------------

    async def _drive_algo(self, parent_id: OrderId) -> None:
        """Ask the algo for the next slice; if there is one, submit it."""
        algo = self._algos.get(parent_id)
        if algo is None:
            return
        spec = algo.next_slice(self._clock.now_ns())
        if spec is None:
            return
        await self._submit_child(parent_id, spec)

    async def _submit_child(
        self, parent_id: OrderId, spec: ChildOrderSpec
    ) -> None:
        """Build an OrderRequest from the spec and publish it."""
        signal = self._parents[parent_id]
        order_id = OrderId(uuid4())
        # Client order id: short, stable, deterministic per order.
        client_order_id = ClientOrderId(f"{signal.strategy_id}-{order_id.hex[:12]}")

        order = Order(
            order_id=order_id,
            client_order_id=client_order_id,
            strategy_id=signal.strategy_id,
            instrument=signal.instrument,
            side=signal.side,
            order_type=spec.order_type,
            quantity=spec.quantity,
            price=spec.price,
            time_in_force=spec.time_in_force,
            created_at_ns=self._clock.now_ns(),
            parent_order_id=parent_id,
        )
        self._orders[order_id] = order

        req = OrderRequest(
            ts_event=self._clock.now_ns(),
            ts_ingest=self._clock.now_ns(),
            source=self._source,
            order_id=order_id,
            client_order_id=client_order_id,
            strategy_id=signal.strategy_id,
            instrument=signal.instrument,
            side=signal.side,
            order_type=spec.order_type,
            quantity=spec.quantity,
            price=spec.price,
            time_in_force=spec.time_in_force,
        )
        await self._bus.publish(Topic.ORDERS, req)

    # --- Cancel API (caller-driven) --------------------------------------

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
        await self._bus.publish(
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

    async def cancel_parent(self, parent_id: OrderId) -> None:
        """Cancel the algo and all in-flight children for one parent."""
        algo = self._algos.get(parent_id)
        if algo is not None:
            algo.cancel()
        # Send cancels for any non-terminal children.
        for order_id, order in list(self._orders.items()):
            if order.parent_order_id == parent_id and not order.is_terminal:
                await self.cancel_order(order_id)

    # --- Read API --------------------------------------------------------

    def get_order(self, order_id: OrderId) -> Order | None:
        return self._orders.get(order_id)

    def open_orders(self) -> Iterable[Order]:
        return (o for o in self._orders.values() if not o.is_terminal)

    # --- Helpers ---------------------------------------------------------

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

    def _parent_has_leaves(self, parent_id: OrderId) -> Decimal:
        """Sum of open leaves_quantity across children. Used to decide algo cleanup."""
        total = Decimal(0)
        for order in self._orders.values():
            if order.parent_order_id == parent_id and not order.is_terminal:
                total += order.leaves_quantity
        return total


__all__ = ["OMSEngine"]
