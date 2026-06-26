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

Signal-as-snapshot (see :class:`SignalEvent` for the full contract): a signal
is a strategy's *complete desired resting state* for one instrument, not an
increment. ``_reconcile_immediate`` enforces this — it matches the signal's
legs against all resting orders for the (strategy, instrument), including
in-flight ones, and cancels any resting order with no matching leg. Including
in-flight orders in that match is what makes a signal a true snapshot rather
than an increment, and is what lets the risk layer cap exposure without
counting working orders.
"""

from __future__ import annotations

import asyncio
import structlog
from collections.abc import Iterable
from uuid import uuid4

from ..core.clock import Clock
from ..core.events import (
    AmendRejected,
    AmendRequest,
    BaseEvent,
    CancelRejected,
    CancelRequest,
    EventId,
    ExecutionRoutedEvent,
    FillEvent,
    KillSwitchEvent,
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

# Statuses for which a sibling order is (or is about to be) resting on the
# venue and could therefore be self-traded. PENDING_CANCEL is excluded — that
# order is on its way off the book — and terminal states obviously can't fill.
# A PENDING_AMEND order is still live at its current price until the amend
# confirms, so it counts.
_STP_LIVE_STATUSES = (
    OrderStatus.PENDING_NEW,
    OrderStatus.ACKNOWLEDGED,
    OrderStatus.PARTIALLY_FILLED,
    OrderStatus.PENDING_AMEND,
)

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
        pending_amend_timeout_seconds: float = 5.0,
        self_trade_prevention: bool = True,
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._router = router or DefaultExecutionRouter()
        self._source = source
        self._signal_ttl_ns = int(signal_ttl_seconds * _NS_PER_SECOND)
        # Internal netting / self-trade prevention. The OMS holds every
        # strategy's orders in one map, so it is the single place that can see a
        # would-be cross between two strategies on the same instrument. When on,
        # a leg that would lift/hit a *sibling* strategy's resting order is held
        # back this tick rather than routed to the venue — the firm never trades
        # with itself (no wasted fees/spread, no wash trade). Per-strategy P&L
        # attribution is untouched: we suppress the cross, we don't re-attribute
        # a fill. See _crosses_other_strategy and _reconcile_immediate.
        self._stp_enabled = self_trade_prevention
        self._self_trade_prevented: int = 0
        # How often the driver loop wakes sliced algos. Configurable because
        # the right cadence depends on the strategy's execution horizon — a
        # fast scalper TWAP wants tighter ticks than a slow VWAP unwind.
        self._algo_driver_interval = algo_driver_interval_seconds
        # An in-flight amend/cancel normally confirms in <100ms. If the venue's
        # response is lost (observed: an amend racing a fill, where the
        # OrderAmended never lands), the order would sit in PENDING_AMEND
        # forever — the reconciler waits for a confirm that never comes and
        # refuses to cancel a mid-amend order. The sweep below treats any
        # in-flight transition older than this as lost and rolls it back so the
        # next reconcile re-amends or cancels it.
        self._pending_amend_timeout_ns = int(
            pending_amend_timeout_seconds * _NS_PER_SECOND
        )
        # Throttle the sweep: the driver wakes every ~0.1s, but checking order
        # ages every second is ample given a multi-second timeout.
        self._last_amend_sweep_ns: int = 0

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
        # The kill switch latches system-wide. When it fires we must actively
        # pull every resting order — the risk engine stops *new* orders, but
        # quotes already on the book are abandoned risk until cancelled.
        await self._bus.subscribe(Topic.ALERTS, self._on_alert)
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

    async def _on_alert(self, event: BaseEvent) -> None:
        # The kill switch is the only alert the OMS acts on. Everything else on
        # this topic (risk warnings, info) is for operators/dashboard, not us.
        if not isinstance(event, KillSwitchEvent):
            return
        _log.error(
            "kill_switch_engaged_cancelling_all_resting",
            triggered_by=event.triggered_by,
            reason=event.reason,
        )
        await self._cancel_all_resting(why="kill_switch")

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
            # Risk fully blocked this signal — cancel all resting orders for
            # this strategy+instrument so stale quotes don't accumulate.
            await self._cancel_resting(signal.strategy_id, signal.instrument.instrument_id)
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
        await self._reconcile_quotes(approved_signal, decision_ts_ingest=event.ts_ingest)

    async def _on_order_event(self, event: BaseEvent) -> None:
        """Handle order_gateway responses on the orders topic."""
        if isinstance(event, OrderAcknowledged):
            await self._handle_ack(event)
        elif isinstance(event, OrderRejected):
            await self._handle_reject(event)
            await self._publish_open_orders()  # reject removes working exposure
        elif isinstance(event, CancelRejected):
            await self._handle_cancel_rejected(event)
        elif isinstance(event, AmendRejected):
            await self._handle_amend_rejected(event)
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
            # Fill arrived after the order reached a terminal state. This is a
            # race we can't avoid: the venue processed the fill, but our amend or
            # cancel hit "order gone" (-2013) / a cancel ack first and we already
            # terminalized the order (see the amend-gone path in the Binance
            # gateway). The fill is authoritative — absorb its *accounting* so
            # cumulative_filled / average_fill_price / leaves stay correct, but
            # don't attempt an (illegal) state transition out of the terminal
            # state. Position/PnL are driven off the bus FillEvent directly, so
            # this only repairs the OMS's per-order view.
            recorded = order.record_fill(event)
            if not recorded:
                _log.info("duplicate_fill_ignored", fill_id=event.fill_id)
                return
            _log.warning(
                "fill_on_terminal_order_recorded",
                order_id=event.order_id,
                order_status=order.status,
                fill_id=event.fill_id,
                cumulative_filled=str(order.cumulative_filled),
            )
            await self._publish_open_orders()
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

    async def _handle_cancel_rejected(self, event: CancelRejected) -> None:
        """Roll a PENDING_CANCEL order back to ACKNOWLEDGED.

        The cancel was rejected by the venue, meaning the order is still live.
        Rolling back to ACKNOWLEDGED lets the reconciler see it as a resting
        order on the next tick and retry the cancel rather than placing a
        duplicate alongside it.
        """
        order = self._orders.get(event.order_id)
        if order is None:
            return
        _log.warning(
            "cancel_rejected_rolling_back_to_acknowledged",
            order_id=str(event.order_id),
            client_order_id=str(event.client_order_id),
            reason=event.reason,
        )
        try:
            order.transition_to(OrderStatus.ACKNOWLEDGED, at_ns=event.ts_event)
        except InvalidStateTransitionError:
            return

    async def _handle_amend_rejected(self, event: AmendRejected) -> None:
        """Roll a PENDING_AMEND order back to ACKNOWLEDGED.

        The amend was rejected by the venue, meaning the order is still live at
        its current price/qty. Rolling back to ACKNOWLEDGED lets the reconciler
        see it as a resting order on the next tick and retry or cancel it.
        """
        order = self._orders.get(event.order_id)
        if order is None:
            return
        order.pending_amend = None
        # If fills already consumed all leaves while the amend was in-flight,
        # the order is effectively done — terminalize it rather than reviving it
        # as ACKNOWLEDGED with leaves=0, which would cause the reconciler to
        # loop amending it forever.
        if order.leaves_quantity == 0:
            try:
                order.transition_to(OrderStatus.FILLED, at_ns=self._clock.now_ns())
            except InvalidStateTransitionError:
                pass
            return
        # Note: rejects that are *permanent for the order* (e.g. Binance -5026
        # "modify limit exhausted") never reach here as an AmendRejected — the
        # Binance gateway translates them to OrderCancelled so the OMS stays
        # venue-neutral. Everything that arrives here is a transient reject: the
        # order is still live at its old price, so roll back and retry next tick.
        _log.warning(
            "amend_rejected_rolling_back_to_acknowledged",
            order_id=str(event.order_id),
            client_order_id=str(event.client_order_id),
            reason=event.reason,
        )
        try:
            order.transition_to(OrderStatus.ACKNOWLEDGED, at_ns=self._clock.now_ns())
        except InvalidStateTransitionError:
            return

    async def _handle_amend(self, event: OrderAmended) -> None:
        order = self._orders.get(event.order_id)
        if order is None:
            return
        if order.status is OrderStatus.PENDING_CANCEL:
            # Amend response arrived after we already queued a cancel for this
            # order — the order is dying and must not be revived.  Drop the
            # stale amend on the floor and let the cancel response terminalize
            # the order normally.  See also _reconcile_immediate which guards
            # against cancelling PENDING_AMEND orders during un-matched sweep.
            _log.info(
                "amend_response_stale_order_pending_cancel",
                order_id=str(order.order_id),
                client_order_id=str(order.client_order_id),
            )
            order.pending_amend = None
            return
        try:
            order.transition_to(OrderStatus.ACKNOWLEDGED, at_ns=event.ts_event)
        except InvalidStateTransitionError:
            # Raced a fill or cancel that already moved the order past PENDING_AMEND.
            order.pending_amend = None
            return
        # Trust the gateway's reported resulting price/qty over our requested
        # values: the venue can clamp or partially apply an amend, and applying
        # the *requested* pending_amend here is how local state silently drifts
        # from the book (orphaned resting orders, ladder accumulation). Fall
        # back to pending_amend only if the event omits a field.
        if event.new_price is not None:
            order.price = event.new_price
        elif order.pending_amend is not None:
            order.price = order.pending_amend[0]
        if event.new_quantity is not None:
            order.quantity = event.new_quantity
        elif order.pending_amend is not None:
            order.quantity = order.pending_amend[1]
        order.pending_amend = None
        if event.new_exchange_order_id is not None:
            order.exchange_order_id = event.new_exchange_order_id

    async def _sweep_stale_pending_amends(self) -> None:
        """Recover orders wedged in PENDING_AMEND by a lost venue response.

        An amend normally confirms in <100ms. When the OrderAmended /
        AmendRejected / OrderCancelled response is lost (e.g. an amend that
        raced a fill), the order is stranded in PENDING_AMEND: the reconciler
        skips it waiting for a confirm that never arrives, and deliberately
        won't cancel a mid-amend order. The order stays alive on the venue but
        frozen in the OMS — a permanent desync.

        After ``pending_amend_timeout`` we treat the amend as lost and roll the
        order back to ACKNOWLEDGED (or FILLED if fills consumed all leaves
        meanwhile), exactly as :meth:`_handle_amend_rejected` does for an
        explicit reject. The next reconcile tick then re-amends or cancels it.
        Rolling back is the conservative choice: it makes no assumption about
        whether the amend applied, only that the OMS must stop waiting.

        A genuinely-late amend response arriving after rollback is safe —
        :meth:`_handle_amend` catches the now-illegal ACKNOWLEDGED->ACKNOWLEDGED
        transition and clears ``pending_amend``.
        """
        now = self._clock.now_ns()
        # Throttle: at most once per second (timeout is multi-second).
        if now - self._last_amend_sweep_ns < _NS_PER_SECOND:
            return
        self._last_amend_sweep_ns = now

        for order in self._orders.values():
            if order.status is not OrderStatus.PENDING_AMEND:
                continue
            if now - order.last_update_ns < self._pending_amend_timeout_ns:
                continue

            age_s = (now - order.last_update_ns) / _NS_PER_SECOND
            order.pending_amend = None
            # Leaves fully consumed while the amend was in-flight: terminalize
            # rather than reviving as ACKNOWLEDGED with leaves=0, which would
            # make the reconciler loop amending it forever.
            target = (
                OrderStatus.FILLED
                if order.leaves_quantity == 0
                else OrderStatus.ACKNOWLEDGED
            )
            try:
                order.transition_to(target, at_ns=now)
            except InvalidStateTransitionError:
                # Raced a real response between the age check and here.
                continue
            _log.warning(
                "pending_amend_timed_out_rolling_back",
                order_id=str(order.order_id),
                client_order_id=str(order.client_order_id),
                rolled_back_to=target.value,
                age_seconds=round(age_s, 2),
            )

    # --- Quote reconciliation and placement ------------------------------

    async def _reconcile_quotes(self, signal: SignalEvent, *, decision_ts_ingest: int = 0) -> None:
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

        await self._reconcile_immediate(signal, immediate_legs, decision_ts_ingest=decision_ts_ingest)
        await self._reconcile_sliced(signal, sliced_legs)
        # One snapshot after the whole reconcile — covers every place/cancel
        # above without emitting a burst of intermediate snapshots.
        await self._publish_open_orders()

    async def _reconcile_immediate(
        self, signal: SignalEvent, legs: list[OrderLeg], *, decision_ts_ingest: int = 0
    ) -> None:
        """Match-or-amend-or-replace reconciliation for non-sliced legs.

        Considers only plain resting orders (``parent_leg_id is None``) so it
        never cancels a slice child as 'stale'. Scoped to ``signal.strategy_id``.

        Each desired leg is matched to the best available resting order:
        - Exact match (side, price, leaves): no-op — preserve queue position.
        - Same side, price/qty differs, order is amendable: send AmendRequest.
        - Same side but PENDING_AMEND: skip this tick — wait for the confirm.
        - No resting order available: place a fresh order.

        Any resting order not matched to a desired leg is cancelled, regardless
        of how many orders exist per side. This handles both ladder strategies
        (multiple legs per side, each matched individually) and accumulation
        bugs (stale extras with no matching desired leg).

        Snapshot enforcement (see :class:`SignalEvent`): the ``resting`` set
        below includes in-flight ``PENDING_NEW`` orders, not just acknowledged
        ones. That is deliberate and load-bearing — matching against in-flight
        orders is what makes a signal a true snapshot rather than an increment,
        so a strategy re-quoting faster than the ack round-trip matches its own
        in-flight order instead of stacking duplicates. Risk relies on this and
        so does not count working orders (see MaxPosition).
        """
        sid = signal.strategy_id
        iid = signal.instrument.instrument_id
        amendable = (OrderStatus.ACKNOWLEDGED, OrderStatus.PARTIALLY_FILLED)
        active = (OrderStatus.PENDING_NEW, *amendable, OrderStatus.PENDING_AMEND, OrderStatus.PENDING_CANCEL)

        # Collect all resting plain orders for this (strategy, instrument).
        resting: list[Order] = [
            o for o in self._orders.values()
            if (
                o.strategy_id == sid
                and o.instrument.instrument_id == iid
                and o.parent_leg_id is None
                and o.status in active
            )
        ]

        # Match each desired leg to the best resting order: prefer exact
        # (price, leaves_qty) match to preserve queue position, then fall back
        # to any amendable order on the same side.
        matched: set[OrderId] = set()
        for leg in legs:
            # Prefer exact match first (preserves queue position).
            exact = next(
                (
                    o for o in resting
                    if o.order_id not in matched
                    and o.side == leg.side
                    and o.price == leg.price
                    and o.leaves_quantity == leg.quantity
                ),
                None,
            )
            if exact is not None:
                matched.add(exact.order_id)
                continue  # already correct — no-op

            # No exact match: find the best candidate on this side to amend.
            candidate = next(
                (
                    o for o in resting
                    if o.order_id not in matched
                    and o.side == leg.side
                ),
                None,
            )
            matched.add(candidate.order_id) if candidate is not None else None

            if candidate is None:
                # Self-trade prevention: don't place a fresh order that would
                # cross a sibling strategy's resting order.
                if self._stp_blocks(signal, leg):
                    continue
                await self._place_quote(signal, leg, decision_ts_ingest=decision_ts_ingest)
            elif candidate.status in (OrderStatus.PENDING_AMEND, OrderStatus.PENDING_CANCEL):
                pass  # in-flight op — wait for confirm, retry next tick
            elif candidate.status in amendable:
                # Self-trade prevention: don't amend a resting order *into* a
                # cross with a sibling. The candidate is already in ``matched``,
                # so skipping leaves it resting at its current (non-crossing)
                # price rather than cancelling it.
                if self._stp_blocks(signal, leg):
                    continue
                await self._amend_quote(candidate, leg)
            # else PENDING_NEW: too early to amend — leave it alone this tick

        # Cancel every resting order that was not matched to a desired leg.
        # PENDING_AMEND orders are mid-amend from a recent reconcile tick;
        # cancelling them here would let the amend response revive the order
        # after the cancel (a transition from PENDING_CANCEL back to
        # ACKNOWLEDGED — see _handle_amend guard for the other half of this
        # race).  Skip them and let the next tick handle the conflict.
        for order in resting:
            if order.order_id not in matched and order.status is not OrderStatus.PENDING_AMEND:
                await self._safe_cancel(order.order_id, why="reconcile_withdrawn")

    def _crosses_other_strategy(
        self, sid: StrategyId, iid: str, side: Side, price: Price | None
    ) -> Order | None:
        """Return a sibling strategy's resting order this leg would cross, if any.

        A leg self-trades when, on the same instrument but a *different*
        strategy, a live resting order sits on the opposite side at a price the
        leg would match: a BUY at/above a resting SELL, or a SELL at/below a
        resting BUY. Market (priceless) legs are not checked — they are not used
        by the quoting strategies and crossing the spread is their intent.
        """
        if price is None:
            return None
        for o in self._orders.values():
            if (
                o.strategy_id == sid
                or o.instrument.instrument_id != iid
                or o.status not in _STP_LIVE_STATUSES
                or o.price is None
            ):
                continue
            if side is Side.BUY and o.side is Side.SELL and price >= o.price:
                return o
            if side is Side.SELL and o.side is Side.BUY and price <= o.price:
                return o
        return None

    def _stp_blocks(self, signal: SignalEvent, leg: OrderLeg) -> bool:
        """True if self-trade prevention should hold this leg back this tick."""
        if not self._stp_enabled:
            return False
        blocker = self._crosses_other_strategy(
            signal.strategy_id, signal.instrument.instrument_id, leg.side, leg.price
        )
        if blocker is None:
            return False
        self._self_trade_prevented += 1
        _log.info(
            "self_trade_prevented",
            strategy_id=signal.strategy_id,
            instrument=signal.instrument.symbol,
            side=leg.side.value,
            price=str(leg.price),
            blocked_by_strategy=blocker.strategy_id,
            blocker_side=blocker.side.value,
            blocker_price=str(blocker.price),
        )
        return True

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
                await self._sweep_stale_pending_amends()
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

    async def _cancel_resting(self, strategy_id: StrategyId, instrument_id: str) -> None:
        """Cancel all non-terminal plain orders for a (strategy, instrument)."""
        for order in list(self._orders.values()):
            if (
                order.strategy_id == strategy_id
                and order.instrument.instrument_id == instrument_id
                and order.parent_leg_id is None
                and not order.is_terminal
            ):
                await self._safe_cancel(order.order_id, why="risk_blocked")

    async def _cancel_all_resting(self, *, why: str) -> None:
        """Cancel every non-terminal order across all strategies/instruments.

        The kill switch's cleanup arm. Unlike :meth:`_cancel_resting` (scoped to
        one strategy+instrument and to plain orders), this pulls *everything*,
        including execution-algo children, because a system-wide halt means no
        order should remain live. Active algos are torn down first so the driver
        loop does not re-emit fresh slices against orders we are cancelling.
        """
        for leg_id in list(self._algos):
            self._retire_algo(leg_id)
        cancelled = 0
        for order in list(self._orders.values()):
            if not order.is_terminal:
                await self._safe_cancel(order.order_id, why=why)
                cancelled += 1
        _log.warning("cancel_all_resting_complete", reason=why, count=cancelled)

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

    async def _place_quote(self, signal: SignalEvent, leg: OrderLeg, *, decision_ts_ingest: int = 0) -> None:
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
            upstream_ts_ns=decision_ts_ingest or None,
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
                side=order.side,
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
            self._refresh_adopted_order(
                existing,
                quantity=quantity,
                cumulative_filled=cumulative_filled,
                price=price,
            )
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
        # Publish OrderAcknowledged so bus subscribers (e.g. user-data stream)
        # register the client_order_id -> order_id mapping and can route
        # subsequent venue events (fills, cancels) for this order correctly.
        if exchange_order_id is not None:
            await self._safe_publish(
                Topic.ORDERS,
                OrderAcknowledged(
                    ts_event=self._clock.now_ns(),
                    ts_ingest=self._clock.now_ns(),
                    source=self._source,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    exchange_order_id=exchange_order_id,
                ),
            )
        await self._publish_open_orders()
        return order_id

    def _refresh_adopted_order(
        self,
        order_id: OrderId,
        *,
        quantity: Quantity,
        cumulative_filled: Quantity,
        price: Price | None,
    ) -> None:
        """Reconcile a re-adopted order's fields to a fresh venue snapshot.

        ``adopt_order`` is idempotent on client_order_id, so a periodic resync
        re-adopts orders we already track. Returning early would freeze the
        order at the state it was first adopted in — a partial fill that grew
        between resyncs would never update ``cumulative_filled``/``leaves``,
        and the resync would silently fail to repair partial-fill drift on
        already-tracked orders (half its stated job).

        We only ratchet ``cumulative_filled`` *forward* (the venue is
        authoritative, but the user-data stream may have already advanced it
        past this snapshot; never rewind). ``price``/``quantity`` are taken
        verbatim — an external party may have amended on the venue. We do not
        synthesize a FillEvent: that would risk double-counting against the
        user-data stream and corrupt per-strategy books, exactly what
        adoption deliberately avoids. We only correct exposure accounting.
        """
        order = self._orders.get(order_id)
        if order is None or order.is_terminal:
            return
        if cumulative_filled > order.cumulative_filled:
            order.cumulative_filled = cumulative_filled
        if quantity != order.quantity:
            order.quantity = quantity
        if price is not None and price != order.price:
            order.price = price

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

    async def terminalize_from_venue(
        self,
        order_id: OrderId,
        *,
        status: OrderStatus,
        cumulative_filled: Quantity | None = None,
    ) -> None:
        """Drive a locally-open order to a venue-reported terminal state.

        Used by the resync when an order has dropped off the venue's open-order
        list: rather than assuming CANCELLED, the caller queries the venue for
        the order's *actual* terminal status and passes it here. This avoids
        recording a filled order as cancelled (and then swallowing the fill on
        the terminal-order path in ``_on_fill``).

        ``cumulative_filled``, when given, ratchets the order's fill total
        forward to the venue's figure before terminalizing, so a fully- or
        partially-filled order's leaves/exposure are correct on the way out.
        We do not emit a FillEvent (no double-count against the user-data
        stream); we only correct the order's own accounting.
        """
        order = self._orders.get(order_id)
        if order is None or order.is_terminal:
            return
        if not status.is_terminal:
            _log.warning(
                "terminalize_from_venue_non_terminal_status_ignored",
                order_id=str(order_id), status=status.value,
            )
            return
        if cumulative_filled is not None and cumulative_filled > order.cumulative_filled:
            order.cumulative_filled = cumulative_filled
        try:
            order.transition_to(status, at_ns=self._clock.now_ns())
        except InvalidStateTransitionError:
            # Raced the user-data stream to a terminal state. Benign.
            return
        await self._publish_open_orders()

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
            "self_trade_prevented": self._self_trade_prevented,
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
