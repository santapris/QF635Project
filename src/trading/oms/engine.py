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

import asyncio
import structlog
from collections.abc import Iterable
from uuid import uuid4

from ..core.clock import Clock
from ..core.events import (
    BaseEvent,
    CancelRequest,
    EventId,
    ExecutionRoutedEvent,
    FillEvent,
    OrderAcknowledged,
    OrderCancelled,
    OrderLeg,
    OrderRejected,
    OrderRequest,
    RiskDecision,
    SignalEvent,
    TickEvent,
)
from ..core.exceptions import BackpressureError, InvalidStateTransitionError, OrderNotFoundError
from ..core.types import (
    ClientOrderId,
    OrderId,
    OrderStatus,
    Price,
    StrategyId,
)
from ..event_bus.base import AbstractEventBus, Topic
from .execution_algos import ChildOrderSpec, ExecutionAlgo
from .order import Order
from .router import DefaultExecutionRouter, ExecutionRouter, RoutingContext

_log = structlog.get_logger(__name__)

_NS_PER_SECOND = 1_000_000_000


class OMSEngine:
    """Order Management System."""

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        clock: Clock,
        router: ExecutionRouter | None = None,
        source: str = "oms",
        signal_ttl_seconds: float = 300.0,
        algo_driver_interval_seconds: float = 0.1,
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._router = router or DefaultExecutionRouter()
        self._source = source
        self._signal_ttl_ns = int(signal_ttl_seconds * _NS_PER_SECOND)
        # How often the driver loop wakes sliced algos. Configurable because
        # the right cadence depends on the strategy's execution horizon — a
        # fast scalper TWAP wants tighter ticks than a slow VWAP unwind.
        self._algo_driver_interval = algo_driver_interval_seconds

        # Signal cache: event_id -> (signal, received_ns).
        # Insertion-ordered dict gives us cheap LRU-style eviction.
        self._signal_cache: dict[EventId, tuple[SignalEvent, int]] = {}

        # Active orders. Keyed by OrderId.
        self._orders: dict[OrderId, Order] = {}
        # Reverse lookup: client_order_id -> order_id. Lets the user-data
        # stream (and any other caller) resolve strategy attribution by
        # client order id without scanning all orders.
        self._coid_to_order_id: dict[ClientOrderId, OrderId] = {}

        # Live execution algos for sliced legs, keyed by OrderLeg.leg_id.
        # An algo lives until it is done (all slices emitted, children
        # terminal) or the strategy withdraws its leg.
        self._algos: dict[str, ExecutionAlgo] = {}
        # The (signal, leg) context each algo needs to stamp its children.
        self._algo_ctx: dict[str, tuple[SignalEvent, OrderLeg]] = {}
        # Latest mark per instrument_id, fed from MARKET_DATA purely so the
        # router can size orders. NOT used to drive algos — that's the timer.
        self._latest_mark: dict[str, Price] = {}
        self._driver_task: asyncio.Task[None] | None = None

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
        # Cache marks for router sizing. Handler is cheap (one dict write).
        await self._bus.subscribe(Topic.MARKET_DATA, self._on_market_data)
        self._driver_task = asyncio.create_task(
            self._algo_driver(), name="oms-algo-driver"
        )

    async def stop(self) -> None:
        self._started = False
        if self._driver_task is not None:
            self._driver_task.cancel()
            try:
                await self._driver_task
            except asyncio.CancelledError:
                pass
            self._driver_task = None

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
                    intent=leg.intent,
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

    async def _on_market_data(self, event: BaseEvent) -> None:
        # Cache the latest mark so the router can size orders. Deliberately
        # does NOT drive algos — slicing cadence is the timer's job so it
        # can't be starved or flooded by tick rate.
        if isinstance(event, TickEvent):
            self._latest_mark[event.instrument.instrument_id] = event.mid

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
            return
        # Notify the owning algo, if this was a slice child.
        leg_id = order.parent_leg_id
        if leg_id is not None:
            algo = self._algos.get(leg_id)
            if algo is not None:
                try:
                    algo.on_fill(event)
                except Exception:
                    _log.exception("algo_on_fill_raised", leg_id=leg_id)
                if algo.is_done() and not self._leg_has_live_children(leg_id):
                    self._retire_algo(leg_id)

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

        Each leg is first routed: PASSIVE / small clips become *immediate*
        legs placed directly; NORMAL/URGENT legs over threshold become
        *sliced* legs owned by an execution algo. The two are reconciled by
        different rules because they have different identities:

        - Immediate legs match resting orders by ``(side, price, leaves)`` so
          unchanged quotes keep their queue position. Slice children (which
          carry ``parent_leg_id``) are excluded from this matching.
        - Sliced legs are keyed by ``leg_id``. A re-signalled leg_id resumes
          the running algo; a withdrawn leg_id cancels the algo and its
          in-flight children.
        """
        immediate_legs: list[OrderLeg] = []
        sliced_legs: list[tuple[OrderLeg, ExecutionAlgo]] = []

        ctx = RoutingContext(
            now_ns=self._clock.now_ns(),
            instrument=signal.instrument,
            last_mark=self._latest_mark.get(signal.instrument.instrument_id),
        )
        for leg in signal.legs:
            decision = self._router.route(leg, ctx)
            await self._emit_routed(signal, leg, decision.algo_name, decision.reason)
            if decision.algo is None:
                immediate_legs.append(leg)
            else:
                sliced_legs.append((leg, decision.algo))

        await self._reconcile_immediate(signal, immediate_legs)
        await self._reconcile_sliced(signal, sliced_legs)

    async def _reconcile_immediate(
        self, signal: SignalEvent, legs: list[OrderLeg]
    ) -> None:
        """Match-or-replace reconciliation for non-sliced legs.

        Considers only plain resting orders (``parent_leg_id is None``) so it
        never cancels a slice child as 'stale'.
        """
        sid = signal.strategy_id
        iid = signal.instrument.instrument_id
        active = (OrderStatus.PENDING_NEW, OrderStatus.ACKNOWLEDGED, OrderStatus.PARTIALLY_FILLED)

        open_orders: list[Order] = [
            o for o in self._orders.values()
            if o.strategy_id == sid
            and o.instrument.instrument_id == iid
            and o.status in active
            and o.parent_leg_id is None
        ]

        # Greedily pair each desired leg with one open order at the same
        # (side, price, leaves). Pairing is one-to-one — a given open order
        # can only satisfy one leg.
        unmatched_open: set[OrderId] = {o.order_id for o in open_orders}
        legs_to_place: list[OrderLeg] = []

        for leg in legs:
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

    async def _reconcile_sliced(
        self, signal: SignalEvent, sliced_legs: list[tuple[OrderLeg, ExecutionAlgo]]
    ) -> None:
        """Start/resume/withdraw execution algos for sliced legs."""
        sid = signal.strategy_id
        iid = signal.instrument.instrument_id
        desired_leg_ids = {leg.leg_id for leg, _ in sliced_legs}

        # Withdraw: algos for this (strategy, instrument) whose leg_id is no
        # longer desired — cancel the algo and its in-flight children.
        for leg_id in list(self._algos):
            ctx = self._algo_ctx.get(leg_id)
            if ctx is None:
                continue
            algo_signal, _ = ctx
            same_scope = (
                algo_signal.strategy_id == sid
                and algo_signal.instrument.instrument_id == iid
            )
            if same_scope and leg_id not in desired_leg_ids:
                self._retire_algo(leg_id)
                await self._cancel_children(leg_id)

        # Start or resume.
        for leg, algo in sliced_legs:
            if leg.leg_id in self._algos:
                # Resume: leave the running algo's slice schedule intact.
                # Refresh the context so children use the latest approved leg.
                self._algo_ctx[leg.leg_id] = (signal, leg)
                continue
            self._algos[leg.leg_id] = algo
            self._algo_ctx[leg.leg_id] = (signal, leg)
            # Kick once now so algos that emit on their first call (e.g. the
            # first TWAP slice at t=start) don't wait a full driver tick.
            await self._drive_algo(leg.leg_id)

    # --- Algo driving ----------------------------------------------------

    async def _algo_driver(self) -> None:
        """Wake every live algo on a fixed cadence and submit any slices."""
        try:
            while self._started:
                await asyncio.sleep(self._algo_driver_interval)
                for leg_id in list(self._algos):
                    await self._drive_algo(leg_id)
        except asyncio.CancelledError:
            return

    async def _drive_algo(self, leg_id: str) -> None:
        algo = self._algos.get(leg_id)
        if algo is None:
            return
        try:
            spec = algo.next_slice(self._clock.now_ns())
        except Exception:
            _log.exception("algo_next_slice_raised", leg_id=leg_id)
            self._retire_algo(leg_id)
            return
        if spec is not None:
            await self._submit_child(leg_id, spec)
        if algo.is_done() and not self._leg_has_live_children(leg_id):
            self._retire_algo(leg_id)

    async def _submit_child(self, leg_id: str, spec: ChildOrderSpec) -> None:
        """Build and publish one slice as a child order stamped with leg_id."""
        ctx = self._algo_ctx.get(leg_id)
        if ctx is None:
            return
        signal, leg = ctx
        order_id = OrderId(uuid4())
        client_order_id = ClientOrderId(f"{signal.strategy_id}-{order_id.hex[:12]}")
        order = Order(
            order_id=order_id,
            client_order_id=client_order_id,
            strategy_id=signal.strategy_id,
            instrument=signal.instrument,
            side=leg.side,
            order_type=spec.order_type,
            quantity=spec.quantity,
            price=spec.price,
            time_in_force=spec.time_in_force,
            created_at_ns=self._clock.now_ns(),
            parent_leg_id=leg_id,
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
            order_type=spec.order_type,
            quantity=spec.quantity,
            price=spec.price,
            time_in_force=spec.time_in_force,
        )
        if not await self._safe_publish(Topic.ORDERS, req):
            order.transition_to(OrderStatus.REJECTED, at_ns=self._clock.now_ns())
            order.reject_reason = "dropped: bus backpressure on orders topic"

    def _leg_has_live_children(self, leg_id: str) -> bool:
        return any(
            o.parent_leg_id == leg_id and not o.is_terminal
            for o in self._orders.values()
        )

    def _retire_algo(self, leg_id: str) -> None:
        algo = self._algos.pop(leg_id, None)
        self._algo_ctx.pop(leg_id, None)
        if algo is not None:
            try:
                algo.cancel()
            except Exception:
                _log.exception("algo_cancel_raised", leg_id=leg_id)

    async def _cancel_children(self, leg_id: str) -> None:
        for order in list(self._orders.values()):
            if order.parent_leg_id == leg_id and not order.is_terminal:
                await self._safe_cancel(order.order_id, why="algo_leg_withdrawn")

    async def _emit_routed(
        self, signal: SignalEvent, leg: OrderLeg, algo_name: str, reason: str
    ) -> None:
        await self._safe_publish(
            Topic.ORDERS,
            ExecutionRoutedEvent(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self._source,
                strategy_id=signal.strategy_id,
                instrument=signal.instrument,
                leg_id=leg.leg_id,
                side=leg.side,
                intent=leg.intent,
                quantity=leg.quantity,
                algo=algo_name,
                reason=reason,
            ),
        )

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
            parent_leg_id=None,
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
            "active_algos": len(self._algos),
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
