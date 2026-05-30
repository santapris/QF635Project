"""Simulation order_gateway.

A complete in-process venue simulator. Owns:

- Per-instrument top-of-book snapshots (updated from :class:`TickEvent`
  on the market-data topic).
- A book of resting limit orders, keyed by ``order_id``.
- Latency and fee models from config.
- A deterministic RNG for tests that toggle partial fills or rejects.

Fill rules:

- **Market / IOC / FOK / Aggressive limits**: fill immediately against
  the opposite side of the book at submission time. Apply slippage in
  ticks if configured.
- **Resting limits**: enter the book. Fill when a :class:`TradeEvent`
  prints at a price that would have crossed the resting limit.
- **POST_ONLY**: reject if it would cross at submission time; otherwise
  enter the book as a maker.

Latency is applied via ``asyncio.sleep`` in microsecond-scaled units.
On a :class:`SimulatedClock`, swap this for a clock-driven scheduler
(out of scope for this batch — backtest engine in batch 9 does it).
"""

from __future__ import annotations

import asyncio
import structlog
import random
from dataclasses import dataclass
from decimal import Decimal
from typing import Final
from uuid import uuid4

from ..core.clock import Clock
from ..core.events import (
    AmendRequest,
    BaseEvent,
    CancelRequest,
    FillEvent,
    OrderAcknowledged,
    OrderAmended,
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
)
from ..event_bus.base import AbstractEventBus, Topic
from .base import AbstractOrderGateway
from .sim_config import SimulationOrderGatewayConfig

_log = structlog.get_logger(__name__)

_NS_PER_MS = 1_000_000


@dataclass(slots=True)
class _RestingOrder:
    """A simulator-side record of a resting limit order."""

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


class SimulationOrderGateway(AbstractOrderGateway):
    """In-process venue simulator with realistic order semantics."""

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        clock: Clock,
        config: SimulationOrderGatewayConfig,
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._cfg = config
        self._venue = config.venue
        self._rng = random.Random(config.seed)

        self._tops: dict[str, _TopOfBook] = {}
        self._resting: dict[OrderId, _RestingOrder] = {}
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

    # --- Inbound handlers -------------------------------------------------

    async def _on_order_event(self, event: BaseEvent) -> None:
        if isinstance(event, OrderRequest):
            await self._handle_order_request(event)
        elif isinstance(event, CancelRequest):
            await self._handle_cancel_request(event)
        elif isinstance(event, AmendRequest):
            await self._handle_amend_request(event)
        # Acks/rejects/fills published by us; ignore.

    async def _on_market_data(self, event: BaseEvent) -> None:
        if isinstance(event, TickEvent):
            self._tops[event.instrument.instrument_id] = _TopOfBook(
                bid=event.bid_price, ask=event.ask_price
            )
        elif isinstance(event, TradeEvent):
            await self._match_resting_against_trade(event)

    # --- Order submission -------------------------------------------------

    async def _handle_order_request(self, req: OrderRequest) -> None:
        # Filter venue. In a single-order_gateway deployment this is always
        # true; in a multi-venue setup the registry would route, but we
        # also defend here.
        if req.instrument.exchange != self._venue:
            return

        # Apply submit -> ack latency.
        await self._sleep_ms(self._cfg.latency.submit_ack_ms)

        # Stochastic reject?
        if self._cfg.rejects.reject_probability > 0 and (
            self._rng.random() < self._cfg.rejects.reject_probability
        ):
            await self._publish_reject(req, self._cfg.rejects.reject_reason)
            return

        # POST_ONLY check: would it cross? If so, reject.
        if req.order_type is OrderType.POST_ONLY:
            top = self._tops.get(req.instrument.instrument_id)
            if top is not None and self._would_cross(req, top):
                await self._publish_reject(
                    req, "POST_ONLY would cross at submission"
                )
                return

        # Acknowledge: ack always precedes fill.
        exchange_order_id = ExchangeOrderId(f"sim-{uuid4().hex[:12]}")
        await self._bus.publish(
            Topic.ORDERS,
            OrderAcknowledged(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self._venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                exchange_order_id=exchange_order_id,
            ),
        )

        # Decide fill behaviour by order type.
        if req.order_type in (OrderType.MARKET, OrderType.IOC, OrderType.FOK):
            await self._fill_market(req, exchange_order_id)
            return

        # LIMIT / STOP_LIMIT / POST_ONLY: see if it can fill at submission.
        top = self._tops.get(req.instrument.instrument_id)
        if top is not None and self._would_cross(req, top):
            # Aggressive limit — fill at the order's price (or worse if slippage).
            await self._fill_market(req, exchange_order_id)
            return

        # Otherwise: enter the book as a resting order.
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

    async def _handle_cancel_request(self, req: CancelRequest) -> None:
        await self._sleep_ms(self._cfg.latency.cancel_ack_ms)
        order = self._resting.pop(req.order_id, None)
        if order is None:
            # Nothing to cancel — either already filled/cancelled or unknown.
            # Real venues return a specific error here; for the sim we emit
            # a benign rejection rather than acknowledging a cancel of nothing.
            await self._publish_reject_cancel(req, "order not found or already done")
            return
        await self._bus.publish(
            Topic.ORDERS,
            OrderCancelled(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self._venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                reason="user requested",
            ),
        )

    async def _handle_amend_request(self, req: AmendRequest) -> None:
        await self._sleep_ms(self._cfg.latency.cancel_ack_ms)
        order = self._resting.get(req.order_id)
        if order is None:
            await self._publish_reject_cancel(req, "order not found for amend")
            return
        if req.new_price is not None:
            order.price = req.new_price
        if req.new_quantity is not None:
            already_filled = order.quantity - order.leaves
            order.quantity = req.new_quantity
            order.leaves = Quantity(max(Decimal(0), req.new_quantity - already_filled))
        await self._bus.publish(
            Topic.ORDERS,
            OrderAmended(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self._venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                new_price=req.new_price,
                new_quantity=req.new_quantity,
            ),
        )

    # --- Fill paths -------------------------------------------------------

    async def _fill_market(
        self, req: OrderRequest, exchange_order_id: ExchangeOrderId
    ) -> None:
        """Immediate fill against the opposite side of the book (or last trade)."""
        await self._sleep_ms(self._cfg.latency.fill_ms)
        top = self._tops.get(req.instrument.instrument_id)
        if top is None or top.bid is None or top.ask is None:
            # No book seen yet. Fill at the request's price if provided
            # (aggressive limit), else reject.
            if req.price is not None:
                fill_price = req.price
            else:
                await self._publish_reject(
                    req, "no market data available for fill price"
                )
                return
        else:
            # Buys take the ask; sells hit the bid.
            fill_price = top.ask if req.side is Side.BUY else top.bid
            # Apply slippage (positive ticks worsen the price).
            if self._cfg.fills.slippage_ticks:
                slippage = req.instrument.tick_size * Decimal(
                    self._cfg.fills.slippage_ticks
                )
                fill_price = (
                    fill_price + slippage
                    if req.side is Side.BUY
                    else fill_price - slippage
                )

        # Snap to the instrument's tick grid.
        fill_price = req.instrument.round_price(fill_price)
        await self._emit_fills(req, exchange_order_id, fill_price, is_maker=False)

    async def _match_resting_against_trade(self, trade: TradeEvent) -> None:
        """A public trade can fill any resting limit on the opposite side it crossed."""
        # Snapshot keys: a fill may pop entries during iteration.
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
            # Fill at the resting order's price (price improvement is rare
            # in real markets; this is the conservative model).
            await self._sleep_ms(self._cfg.latency.fill_ms)
            await self._emit_fills_for_resting(order, order.price, is_maker=True)
            if order.leaves == 0:
                self._resting.pop(order.order_id, None)

    async def _emit_fills(
        self,
        req: OrderRequest,
        exchange_order_id: ExchangeOrderId,
        fill_price: Price,
        *,
        is_maker: bool,
    ) -> None:
        """Emit one or two fills for ``req``, depending on partial-fill config."""
        first_qty, second_qty = self._split_fill(req.quantity)
        if first_qty > 0:
            await self._publish_fill(
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                exchange_order_id=exchange_order_id,
                strategy_id=req.strategy_id,
                instrument=req.instrument,
                side=req.side,
                fill_price=fill_price,
                fill_quantity=first_qty,
                cumulative=first_qty,
                leaves=req.quantity - first_qty,
                is_maker=is_maker,
            )
        if second_qty > 0:
            await self._sleep_ms(self._cfg.latency.fill_ms)
            await self._publish_fill(
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

    async def _emit_fills_for_resting(
        self, order: _RestingOrder, fill_price: Price, *, is_maker: bool
    ) -> None:
        """Fill (some of) a resting order. Always-full unless partial config set."""
        first_qty, second_qty = self._split_fill(order.leaves)
        prior_leaves = order.leaves
        prior_filled = order.quantity - prior_leaves

        if first_qty > 0:
            order.leaves = Quantity(order.leaves - first_qty)
            await self._publish_fill(
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
            await self._sleep_ms(self._cfg.latency.fill_ms)
            await self._publish_fill(
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
        """Optionally split a full fill into two for realism. Returns (first, second)."""
        prob = self._cfg.fills.partial_fill_probability
        if prob <= 0 or self._rng.random() >= prob:
            return Quantity(qty), Quantity(Decimal(0))
        # Pick a first-fill fraction in [min_fraction, 1.0 - min_fraction]
        min_frac = self._cfg.fills.partial_fill_min_fraction
        max_frac = 1.0 - min_frac
        if max_frac <= min_frac:
            return Quantity(qty), Quantity(Decimal(0))
        frac = self._rng.uniform(min_frac, max_frac)
        first = Quantity(qty * Decimal(str(frac)))
        # Round to lot — but tests use sub-lot quantities; keep raw if too small.
        return first, Quantity(qty - first)

    async def _publish_fill(
        self,
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
        await self._bus.publish(
            Topic.FILLS,
            FillEvent(
                fill_id=FillId(uuid4()),
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
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

    # --- Reject helpers ---------------------------------------------------

    async def _publish_reject(self, req: OrderRequest, reason: str) -> None:
        await self._bus.publish(
            Topic.ORDERS,
            OrderRejected(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self._venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                reason=reason,
            ),
        )

    async def _publish_reject_cancel(
        self, req: CancelRequest | AmendRequest, reason: str
    ) -> None:
        """Reject a cancel/amend. We reuse OrderRejected for visibility.

        Real venues differentiate (CancelRejected, etc.) but the OMS only
        cares that something has failed; the symmetry simplifies routing.
        """
        await self._bus.publish(
            Topic.ORDERS,
            OrderRejected(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self._venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                reason=reason,
            ),
        )

    # --- Helpers ----------------------------------------------------------

    @staticmethod
    def _would_cross(req: OrderRequest, top: _TopOfBook) -> bool:
        if req.price is None:
            return False
        if req.side is Side.BUY and top.ask is not None:
            return req.price >= top.ask
        if req.side is Side.SELL and top.bid is not None:
            return req.price <= top.bid
        return False

    async def _sleep_ms(self, ms: float) -> None:
        if ms <= 0:
            return
        await asyncio.sleep(ms / 1000.0)


__all__ = ["SimulationOrderGateway"]
