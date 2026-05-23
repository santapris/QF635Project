"""Backtest gateway.

Mirrors :class:`SimulationGateway` (fill semantics, fees, partial
fills, slippage) but uses a *scheduled* approach instead of
``asyncio.sleep`` for latency. Events that would otherwise be emitted
"after a latency delay" are placed on a min-heap keyed by simulated
ns; the replay engine drains them at the right simulated time.

This is what lets a 24-hour historical replay finish in seconds: no
wall-clock waits.

The gateway exposes :meth:`due_events` to the replay engine. The
engine calls it after each clock advance to drain whatever the
gateway has ready, then continues to the next data event.
"""

from __future__ import annotations

import heapq
import itertools
import structlog
import random
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Final
from uuid import uuid4

from ..core.clock import SimulatedClock
from ..core.events import (
    AmendRequest,
    BaseEvent,
    CancelRequest,
    FillEvent,
    OrderAcknowledged,
    OrderCancelled,
    OrderRejected,
    OrderRequest,
    TickEvent,
    TradeEvent,
)
from ..core.instruments import Instrument
from ..core.types import (
    ClientOrderId,
    ExchangeOrderId,
    FillId,
    OrderId,
    OrderType,
    Price,
    Quantity,
    Side,
    StrategyId,
    Timestamp,
)
from ..event_bus.base import AbstractEventBus, Topic
from ..gateways.base import AbstractGateway
from ..gateways.sim_config import SimulationGatewayConfig

_log = structlog.get_logger(__name__)

_NS_PER_MS = 1_000_000


@dataclass(slots=True)
class _RestingOrder:
    order_id: OrderId
    client_order_id: ClientOrderId
    exchange_order_id: ExchangeOrderId
    strategy_id: StrategyId
    instrument: Instrument
    side: Side
    quantity: Quantity
    price: Price
    leaves: Quantity


@dataclass(slots=True)
class _TopOfBook:
    bid: Price | None = None
    ask: Price | None = None


@dataclass(order=True)
class _Scheduled:
    """One scheduled event in the latency heap.

    The ``seq`` field is a tiebreaker so events scheduled at the same
    ns are dispatched in submission order (heapq compares the full
    tuple). ``topic`` and ``event`` are not part of the order key.
    """
    due_ns: int
    seq: int
    topic: str = field(compare=False)
    event: BaseEvent = field(compare=False)


class BacktestGateway(AbstractGateway):
    """Time-jumping venue simulator for backtests."""

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        clock: SimulatedClock,
        config: SimulationGatewayConfig,
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._cfg = config
        self._venue = config.venue
        self._rng = random.Random(config.seed)

        self._tops: dict[str, _TopOfBook] = {}
        self._resting: dict[OrderId, _RestingOrder] = {}

        # Scheduling
        self._schedule: list[_Scheduled] = []
        self._seq = itertools.count()

        self._started = False

    @property
    def venue(self) -> str:
        return self._venue

    # --- Lifecycle --------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self._bus.subscribe(Topic.ORDERS, self._on_order_event)
        await self._bus.subscribe(Topic.MARKET_DATA, self._on_market_data)

    async def stop(self) -> None:
        self._started = False

    # --- Scheduling -------------------------------------------------------

    def _schedule_event(self, delay_ms: float, topic: str, event: BaseEvent) -> None:
        due_ns = self._clock.now_ns() + max(0, int(delay_ms * _NS_PER_MS))
        heapq.heappush(
            self._schedule,
            _Scheduled(due_ns=due_ns, seq=next(self._seq), topic=topic, event=event),
        )

    async def drain_due(self) -> int:
        """Publish all events whose ``due_ns <= clock.now_ns()``. Returns count.

        The replay engine calls this after each clock advance. Returning
        the count lets the engine know whether to loop again (a fill may
        have triggered another schedulable event, e.g. a matched resting
        order).
        """
        count = 0
        now = self._clock.now_ns()
        while self._schedule and self._schedule[0].due_ns <= now:
            scheduled = heapq.heappop(self._schedule)
            await self._bus.publish(scheduled.topic, scheduled.event)
            count += 1
        return count

    def next_due_ns(self) -> int | None:
        """Earliest scheduled event time, or None if nothing pending."""
        return self._schedule[0].due_ns if self._schedule else None

    # --- Inbound handlers -------------------------------------------------

    async def _on_order_event(self, event: BaseEvent) -> None:
        if isinstance(event, OrderRequest):
            self._handle_order_request(event)
        elif isinstance(event, CancelRequest):
            self._handle_cancel_request(event)
        elif isinstance(event, AmendRequest):
            self._handle_amend_request(event)

    async def _on_market_data(self, event: BaseEvent) -> None:
        if isinstance(event, TickEvent):
            self._tops[event.instrument.instrument_id] = _TopOfBook(
                bid=event.bid_price, ask=event.ask_price
            )
            # A new tick might let a resting limit fill (if the spread
            # crosses through it). Most exchanges drive this off trades
            # rather than book updates; we keep the trade-driven path
            # as the primary, but a tick could surface that the book
            # has moved through our level — let it pass; the trade
            # event for the same move (if data has it) will trigger
            # the fill more accurately.
        elif isinstance(event, TradeEvent):
            self._match_resting_against_trade(event)

    # --- Order submission -------------------------------------------------

    def _handle_order_request(self, req: OrderRequest) -> None:
        if req.instrument.exchange != self._venue:
            return

        # Latency to ack.
        ack_delay = self._cfg.latency.submit_ack_ms

        # Stochastic reject?
        if self._cfg.rejects.reject_probability > 0 and (
            self._rng.random() < self._cfg.rejects.reject_probability
        ):
            self._schedule_reject(req, self._cfg.rejects.reject_reason, ack_delay)
            return

        # POST_ONLY: would it cross?
        if req.order_type is OrderType.POST_ONLY:
            top = self._tops.get(req.instrument.instrument_id)
            if top is not None and self._would_cross(req, top):
                self._schedule_reject(
                    req, "POST_ONLY would cross at submission", ack_delay
                )
                return

        # Acknowledge.
        exchange_order_id = ExchangeOrderId(f"bt-{uuid4().hex[:12]}")
        self._schedule_event(
            ack_delay,
            Topic.ORDERS,
            OrderAcknowledged(
                ts_event=self._due_ns(ack_delay),
                ts_ingest=self._due_ns(ack_delay),
                source=self._venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                exchange_order_id=exchange_order_id,
            ),
        )

        # Decide fill behaviour.
        if req.order_type in (OrderType.MARKET, OrderType.IOC, OrderType.FOK):
            self._fill_market(req, exchange_order_id, ack_delay)
            return

        top = self._tops.get(req.instrument.instrument_id)
        if top is not None and self._would_cross(req, top):
            self._fill_market(req, exchange_order_id, ack_delay)
            return

        # Rest on the book.
        self._resting[req.order_id] = _RestingOrder(
            order_id=req.order_id,
            client_order_id=req.client_order_id,
            exchange_order_id=exchange_order_id,
            strategy_id=req.strategy_id,
            instrument=req.instrument,
            side=req.side,
            quantity=req.quantity,
            price=req.price or Price(Decimal(0)),
            leaves=req.quantity,
        )

    def _handle_cancel_request(self, req: CancelRequest) -> None:
        delay = self._cfg.latency.cancel_ack_ms
        order = self._resting.pop(req.order_id, None)
        if order is None:
            self._schedule_event(
                delay,
                Topic.ORDERS,
                OrderRejected(
                    ts_event=self._due_ns(delay),
                    ts_ingest=self._due_ns(delay),
                    source=self._venue,
                    order_id=req.order_id,
                    client_order_id=req.client_order_id,
                    reason="order not found or already done",
                ),
            )
            return
        self._schedule_event(
            delay,
            Topic.ORDERS,
            OrderCancelled(
                ts_event=self._due_ns(delay),
                ts_ingest=self._due_ns(delay),
                source=self._venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                reason="user requested",
            ),
        )

    def _handle_amend_request(self, req: AmendRequest) -> None:
        order = self._resting.get(req.order_id)
        if order is None:
            return
        if req.new_quantity is not None:
            order.quantity = req.new_quantity
            order.leaves = req.new_quantity
        if req.new_price is not None:
            order.price = req.new_price

    # --- Fill paths -------------------------------------------------------

    def _fill_market(
        self, req: OrderRequest, exchange_order_id: ExchangeOrderId, ack_delay: float
    ) -> None:
        top = self._tops.get(req.instrument.instrument_id)
        if top is None or top.bid is None or top.ask is None:
            if req.price is not None:
                fill_price = req.price
            else:
                self._schedule_reject(
                    req, "no market data available for fill price", ack_delay
                )
                return
        else:
            fill_price = top.ask if req.side is Side.BUY else top.bid
            if self._cfg.fills.slippage_ticks:
                slippage = req.instrument.tick_size * Decimal(
                    self._cfg.fills.slippage_ticks
                )
                fill_price = (
                    fill_price + slippage
                    if req.side is Side.BUY
                    else fill_price - slippage
                )

        fill_price = req.instrument.round_price(fill_price)
        fill_delay = ack_delay + self._cfg.latency.fill_ms
        self._emit_fills(req, exchange_order_id, fill_price, is_maker=False,
                         delay=fill_delay)

    def _match_resting_against_trade(self, trade: TradeEvent) -> None:
        for order_id in list(self._resting.keys()):
            order = self._resting.get(order_id)
            if order is None or order.instrument.instrument_id != trade.instrument.instrument_id:
                continue
            crosses = (
                (order.side is Side.BUY and trade.price <= order.price)
                or (order.side is Side.SELL and trade.price >= order.price)
            )
            if not crosses:
                continue
            self._emit_fills_for_resting(
                order, order.price, is_maker=True, delay=self._cfg.latency.fill_ms
            )
            if order.leaves == 0:
                self._resting.pop(order.order_id, None)

    def _emit_fills(
        self,
        req: OrderRequest,
        exchange_order_id: ExchangeOrderId,
        fill_price: Price,
        *,
        is_maker: bool,
        delay: float,
    ) -> None:
        first_qty, second_qty = self._split_fill(req.quantity)
        if first_qty > 0:
            self._schedule_fill(
                delay,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                exchange_order_id=exchange_order_id,
                strategy_id=req.strategy_id,
                instrument=req.instrument,
                side=req.side,
                fill_price=fill_price,
                fill_quantity=first_qty,
                cumulative=first_qty,
                leaves=Quantity(req.quantity - first_qty),
                is_maker=is_maker,
            )
        if second_qty > 0:
            # Second fill arrives one fill_ms later.
            self._schedule_fill(
                delay + self._cfg.latency.fill_ms,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                exchange_order_id=exchange_order_id,
                strategy_id=req.strategy_id,
                instrument=req.instrument,
                side=req.side,
                fill_price=fill_price,
                fill_quantity=second_qty,
                cumulative=req.quantity,
                leaves=Quantity(Decimal(0)),
                is_maker=is_maker,
            )

    def _emit_fills_for_resting(
        self,
        order: _RestingOrder,
        fill_price: Price,
        *,
        is_maker: bool,
        delay: float,
    ) -> None:
        first_qty, second_qty = self._split_fill(order.leaves)
        prior_filled = order.quantity - order.leaves

        if first_qty > 0:
            order.leaves = Quantity(order.leaves - first_qty)
            self._schedule_fill(
                delay,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
                strategy_id=order.strategy_id,
                instrument=order.instrument,
                side=order.side,
                fill_price=fill_price,
                fill_quantity=first_qty,
                cumulative=Quantity(prior_filled + first_qty),
                leaves=order.leaves,
                is_maker=is_maker,
            )
        if second_qty > 0:
            order.leaves = Quantity(order.leaves - second_qty)
            self._schedule_fill(
                delay + self._cfg.latency.fill_ms,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
                strategy_id=order.strategy_id,
                instrument=order.instrument,
                side=order.side,
                fill_price=fill_price,
                fill_quantity=second_qty,
                cumulative=Quantity(prior_filled + first_qty + second_qty),
                leaves=order.leaves,
                is_maker=is_maker,
            )

    def _split_fill(self, qty: Quantity) -> tuple[Quantity, Quantity]:
        prob = self._cfg.fills.partial_fill_probability
        if prob <= 0 or self._rng.random() >= prob:
            return Quantity(qty), Quantity(Decimal(0))
        min_frac = self._cfg.fills.partial_fill_min_fraction
        max_frac = 1.0 - min_frac
        if max_frac <= min_frac:
            return Quantity(qty), Quantity(Decimal(0))
        frac = self._rng.uniform(min_frac, max_frac)
        first = Quantity(qty * Decimal(str(frac)))
        return first, Quantity(qty - first)

    def _schedule_fill(
        self,
        delay_ms: float,
        *,
        order_id: OrderId,
        client_order_id: ClientOrderId,
        exchange_order_id: ExchangeOrderId,
        strategy_id: StrategyId,
        instrument: Instrument,
        side: Side,
        fill_price: Price,
        fill_quantity: Quantity,
        cumulative: Quantity,
        leaves: Quantity,
        is_maker: bool,
    ) -> None:
        notional = fill_price * fill_quantity
        fee = self._cfg.fees.fee_for(notional=notional, is_maker=is_maker)
        fee_currency = self._cfg.fees.fee_currency or instrument.quote_currency
        self._schedule_event(
            delay_ms,
            Topic.FILLS,
            FillEvent(
                fill_id=FillId(uuid4()),
                ts_event=self._due_ns(delay_ms),
                ts_ingest=self._due_ns(delay_ms),
                source=self._venue,
                order_id=order_id,
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                strategy_id=strategy_id,
                instrument=instrument,
                side=side,
                fill_price=fill_price,
                fill_quantity=fill_quantity,
                cumulative_quantity=cumulative,
                leaves_quantity=leaves,
                fee=Price(fee),
                fee_currency=fee_currency,
                is_maker=is_maker,
            ),
        )

    def _schedule_reject(self, req: OrderRequest, reason: str, delay_ms: float) -> None:
        self._schedule_event(
            delay_ms,
            Topic.ORDERS,
            OrderRejected(
                ts_event=self._due_ns(delay_ms),
                ts_ingest=self._due_ns(delay_ms),
                source=self._venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                reason=reason,
            ),
        )

    # --- Helpers ---------------------------------------------------------

    @staticmethod
    def _would_cross(req: OrderRequest, top: _TopOfBook) -> bool:
        if req.price is None:
            return False
        if req.side is Side.BUY and top.ask is not None:
            return req.price >= top.ask
        if req.side is Side.SELL and top.bid is not None:
            return req.price <= top.bid
        return False

    def _due_ns(self, delay_ms: float) -> Timestamp:
        return Timestamp(self._clock.now_ns() + int(delay_ms * _NS_PER_MS))


__all__ = ["BacktestGateway"]
