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
    AmendRequest,
    BaseEvent,
    CancelRequest,
    EventId,
    ExecutionRoutedEvent,
    FillEvent,
    OpenOrderDetail,
    OpenOrdersSnapshotEvent,
    OrderAcknowledged,
    OrderAmended,
    OrderCancelled,
    OrderLeg,
    OrderRejected,
    OrderRequest,
    RiskDecision,
    SignalEvent,
    TickEvent,
    WorkingExposure,
)
from ..core.exceptions import BackpressureError, InvalidStateTransitionError, OrderNotFoundError
from ..core.instruments import Instrument
from ..core.types import (
    ClientOrderId,
    ExchangeOrderId,
    OrderId,
    OrderStatus,
    OrderType,
    Price,
    Quantity,
    Side,
    StrategyId,
    TimeInForce,
)
from decimal import Decimal
from ..event_bus.base import AbstractEventBus, Topic
from .execution_algos import ChildOrderSpec, ExecutionAlgo
from .order import Order
from .router import DefaultExecutionRouter, ExecutionRouter, RoutingContext

_log = structlog.get_logger(__name__)

_NS_PER_SECOND = 1_000_000_000

# Reserved strategy id for orders/positions adopted from the venue that we
# cannot attribute to a known strategy (placed by a human, another system, or
# whose client_order_id doesn't match our minting scheme). External orders are
# tracked for risk/display but never owned by a strategy's reconciliation.
EXTERNAL_STRATEGY_ID = StrategyId("external")


def strategy_id_from_client_order_id(coid: str) -> StrategyId:
    """Recover the owning strategy from a client_order_id, or EXTERNAL.

    We mint client order ids as ``f"{strategy_id}-{order_id.hex[:12]}"``. The
    trailing segment is a 12-char hex token, so the strategy id is everything
    before the final ``-``. A coid that doesn't fit this shape was not minted
    by us → EXTERNAL.
    """
    head, sep, tail = coid.rpartition("-")
    if sep and head and len(tail) == 12 and all(c in "0123456789abcdef" for c in tail):
        return StrategyId(head)
    return EXTERNAL_STRATEGY_ID


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
            await self._publish_open_orders()  # reject removes working exposure
        elif isinstance(event, OrderCancelled):
            await self._handle_cancel(event)
            await self._publish_open_orders()  # cancel removes working exposure
        elif isinstance(event, OrderAmended):
            await self._handle_amend(event)
            await self._publish_open_orders()  # price/qty change = exposure change
        # An ack doesn't change leaves (PENDING_NEW already counted), so no
        # republish there. OrderRequest / CancelRequest / AmendRequest are our
        # own echoes — ignored.

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
        if order.is_terminal:
            # Fill arrived after the order reached a terminal state (e.g. cancel-fill
            # race). The exchange processed the fill before our cancel landed; log and
            # skip — the strategy already received the fill callback via the bus.
            #
            # TODO: this skips updating order.cumulative_filled / average_fill_price,
            # so the OMS's fill accounting for this order is incomplete. If anything
            # downstream reads those fields after cancellation (reconciliation, PnL,
            # algo slicing) it will see stale data. Fix: split apply_fill into a
            # data-update step and a state-transition step so a terminal order can
            # still absorb the fill accounting without attempting an illegal transition.
            _log.warning(
                "fill_on_terminal_order_ignored",
                order_id=event.order_id,
                order_status=order.status,
                fill_id=event.fill_id,
            )
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
        # A fill reduces leaves (and may terminalize the order) — exposure changed.
        await self._publish_open_orders()

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

    async def _handle_amend(self, event: OrderAmended) -> None:
        order = self._orders.get(event.order_id)
        if order is None:
            return
        try:
            order.transition_to(OrderStatus.ACKNOWLEDGED, at_ns=event.ts_event)
        except InvalidStateTransitionError:
            # Raced a fill or cancel that already moved the order past PENDING_AMEND.
            order.pending_amend = None
            return
        if order.pending_amend is not None:
            order.price, order.quantity = order.pending_amend
            order.pending_amend = None
        if event.new_exchange_order_id is not None:
            order.exchange_order_id = event.new_exchange_order_id

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
        # One snapshot after the whole reconcile — covers every place/cancel
        # above without emitting a burst of intermediate snapshots.
        await self._publish_open_orders()

    async def _reconcile_immediate(
        self, signal: SignalEvent, legs: list[OrderLeg]
    ) -> None:
        """Match-or-amend-or-replace reconciliation for non-sliced legs.

        Considers only plain resting orders (``parent_leg_id is None``) so it
        never cancels a slice child as 'stale'. Scoped to ``signal.strategy_id``.

        For each desired leg we look for a resting order on the same side:
        - Exact match (side, price, leaves): no-op — preserve queue position.
        - Price/qty differs but order is live (ACKNOWLEDGED/PARTIALLY_FILLED):
          send an AmendRequest rather than cancel+place, saving one round-trip
          and preserving queue position at the new price.
        - Order is in PENDING_AMEND: skip this tick — wait for the confirm.
        - No resting order on this side: place a fresh order.

        Sides no longer in the desired set are cancelled.
        """
        sid = signal.strategy_id
        iid = signal.instrument.instrument_id
        amendable = (OrderStatus.ACKNOWLEDGED, OrderStatus.PARTIALLY_FILLED)
        active = (OrderStatus.PENDING_NEW, *amendable, OrderStatus.PENDING_AMEND)

        # One resting plain order per side (market-making emits at most one per side).
        open_by_side: dict[Side, Order] = {}
        for o in self._orders.values():
            if (
                o.strategy_id == sid
                and o.instrument.instrument_id == iid
                and o.parent_leg_id is None
                and o.status in active
            ):
                open_by_side.setdefault(o.side, o)

        desired_sides: set[Side] = set()
        for leg in legs:
            desired_sides.add(leg.side)
            existing = open_by_side.get(leg.side)

            if existing is None:
                await self._place_quote(signal, leg)
            elif (
                existing.price == leg.price
                and existing.leaves_quantity == leg.quantity
            ):
                pass  # already correct — no-op, keep queue position
            elif existing.status == OrderStatus.PENDING_AMEND:
                pass  # amend already in flight — wait for confirm, retry next tick
            elif existing.status in amendable:
                await self._amend_quote(existing, leg)
            # else PENDING_NEW: too early to amend — leave it alone this tick

        # Withdraw: sides no longer desired → cancel whatever is resting there.
        for side, order in open_by_side.items():
            if side not in desired_sides:
                await self._safe_cancel(order.order_id, why="reconcile_withdrawn")

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
            # A new slice was placed from the timer path (outside reconcile),
            # so emit a snapshot here; reconcile-time kicks are covered by
            # _reconcile_quotes' own trailing snapshot.
            await self._publish_open_orders()
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

    async def _amend_quote(self, order: Order, leg: OrderLeg) -> None:
        """Send an AmendRequest for a resting quote whose price or qty changed."""
        try:
            order.transition_to(OrderStatus.PENDING_AMEND, at_ns=self._clock.now_ns())
        except InvalidStateTransitionError:
            return  # raced to a terminal state — skip
        order.pending_amend = (leg.price, leg.quantity)
        await self._safe_publish(
            Topic.ORDERS,
            AmendRequest(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self._source,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                instrument=order.instrument,
                new_price=leg.price,
                new_quantity=leg.quantity,
            ),
        )

    # --- Adoption of pre-existing venue state ----------------------------

    async def adopt_order(
        self,
        *,
        instrument: Instrument,
        client_order_id: ClientOrderId,
        side: Side,
        order_type: OrderType,
        quantity: Quantity,
        cumulative_filled: Quantity,
        price: Price | None,
        time_in_force: TimeInForce,
        exchange_order_id: ExchangeOrderId | None = None,
        created_at_ns: int | None = None,
    ) -> OrderId:
        """Adopt an order that already exists on the venue.

        Used at startup (and during periodic resync) to recover orders the
        venue reports that this process did not place — e.g. orders left
        resting across a restart, or placed by a human. The order is seeded
        in the ACKNOWLEDGED/PARTIALLY_FILLED state (it is already live) and
        attributed to its owning strategy via the client_order_id, falling
        back to EXTERNAL. It carries no parent_leg_id — adopted orders are
        plain resting orders; we cannot reconstruct algo ownership.

        Idempotent on client_order_id: re-adopting an order we already track
        returns the existing OrderId without creating a duplicate.
        """
        existing = self._coid_to_order_id.get(client_order_id)
        if existing is not None:
            return existing

        strategy_id = strategy_id_from_client_order_id(str(client_order_id))
        order_id = OrderId(uuid4())
        status = (
            OrderStatus.PARTIALLY_FILLED
            if cumulative_filled > 0
            else OrderStatus.ACKNOWLEDGED
        )
        order = Order(
            order_id=order_id,
            client_order_id=client_order_id,
            strategy_id=strategy_id,
            instrument=instrument,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            time_in_force=time_in_force,
            created_at_ns=created_at_ns if created_at_ns is not None else self._clock.now_ns(),
            parent_leg_id=None,
        )
        # Seed lifecycle state directly — the order is already live on the
        # venue, so we bypass the PENDING_NEW → ACK transition path.
        order.status = status
        order.cumulative_filled = cumulative_filled
        order.exchange_order_id = exchange_order_id
        self._orders[order_id] = order
        self._coid_to_order_id[client_order_id] = order_id
        _log.info(
            "adopted_order",
            order_id=str(order_id), client_order_id=str(client_order_id),
            strategy_id=strategy_id, side=side.value, status=status.value,
            instrument=instrument.symbol,
        )
        await self._publish_open_orders()
        return order_id

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

    def working_exposures(self) -> tuple[WorkingExposure, ...]:
        """Aggregate leaves_quantity of open orders per (strategy, instrument).

        Buy and sell are kept separate so consumers can reason about the
        worst case on each side. PENDING_NEW (placed, not yet acked) orders
        count — a fill can arrive before the ack, so they are real exposure.
        """
        agg: dict[tuple[StrategyId, str], dict] = {}
        for o in self._orders.values():
            if o.is_terminal:
                continue
            key = (o.strategy_id, o.instrument.instrument_id)
            slot = agg.get(key)
            if slot is None:
                slot = {
                    "buy": Decimal(0), "sell": Decimal(0),
                    "count": 0, "instrument": o.instrument,
                }
                agg[key] = slot
            if o.side is Side.BUY:
                slot["buy"] += o.leaves_quantity
            else:
                slot["sell"] += o.leaves_quantity
            slot["count"] += 1
        return tuple(
            WorkingExposure(
                strategy_id=sid,
                instrument=slot["instrument"],
                working_buy=Quantity(slot["buy"]),
                working_sell=Quantity(slot["sell"]),
                open_order_count=slot["count"],
            )
            for (sid, _iid), slot in agg.items()
        )

    def open_order_details(self) -> tuple[OpenOrderDetail, ...]:
        """Per-order detail for every currently-resting order. For display."""
        return tuple(
            OpenOrderDetail(
                order_id=str(o.order_id),
                client_order_id=str(o.client_order_id),
                strategy_id=o.strategy_id,
                instrument=o.instrument,
                side=o.side,
                order_type=o.order_type,
                quantity=o.quantity,
                leaves_quantity=o.leaves_quantity,
                price=o.price,
                status=o.status,
                created_at_ns=o.created_at_ns,
            )
            for o in self._orders.values()
            if not o.is_terminal
        )

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

    async def _publish_open_orders(self) -> None:
        """Publish a working-order snapshot. Called after any change to the
        open-order set so risk and the dashboard track effective exposure.

        Snapshot semantics: a dropped publish self-heals on the next change.
        """
        await self._safe_publish(
            Topic.OPEN_ORDERS,
            OpenOrdersSnapshotEvent(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self._source,
                exposures=self.working_exposures(),
                orders=self.open_order_details(),
            ),
        )

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


__all__ = ["OMSEngine", "EXTERNAL_STRATEGY_ID", "strategy_id_from_client_order_id"]
