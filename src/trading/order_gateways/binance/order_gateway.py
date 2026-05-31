"""Binance order gateway (Spot and Futures).

Implements :class:`AbstractOrderGateway`. Whether it targets Spot or Futures
is determined entirely by :attr:`BinanceConfig.futures` — no code outside
this module (OMS, strategies) needs to know. Subscribes to the ``orders``
topic for outbound :class:`OrderRequest` / :class:`CancelRequest` /
:class:`AmendRequest` events, translates them to Binance REST calls, and
publishes the canonical response events on the same topic.

Important behaviour notes:

- **Idempotency via client order id.** Every order we send includes the
  caller's ``client_order_id`` as ``newClientOrderId``. If we time out
  and retry, Binance deduplicates by this id. Binance restricts
  ``newClientOrderId`` characters; the OMS's id format (``{strat}-{hex}``)
  is already safe.

- **Fills do NOT come from REST responses.** The canonical source of fills
  is the user-data WebSocket. We ignore the fills array in REST responses.

- **Amend semantics are venue-capability-driven.**
  Futures supports a native PUT amend (``PUT /fapi/v1/order``) — one
  round-trip, emits ``OrderAmended``.
  Spot has no modify endpoint — we cancel the old order and emit
  ``OrderCancelled``; the OMS then re-places a fresh order on the next
  reconcile tick. The OMS sees only canonical events; it does not know or
  care which path ran.
"""

from __future__ import annotations

import asyncio
import structlog
from decimal import Decimal
from typing import Any

from ...core.clock import Clock
from ...core.events import (
    AmendRequest,
    BaseEvent,
    CancelRejected,
    CancelRequest,
    OrderAcknowledged,
    OrderAmended,
    OrderCancelled,
    OrderRejected,
    OrderRequest,
)
from ...core.exceptions import BackpressureError, OrderGatewayError, OrderError
from ...core.types import (
    ClientOrderId,
    ExchangeOrderId,
    OrderId,
    OrderType,
    Price,
    Quantity,
    TimeInForce,
)
from ...event_bus.base import AbstractEventBus, Topic
from ..base import AbstractOrderGateway
from .config import BinanceConfig, BinanceCredentials
from .order_translation import (
    order_type_to_binance,
    side_to_binance,
    tif_to_binance,
)
from .rest_client import BinanceRESTClient
from .symbols import SymbolMapper

_log = structlog.get_logger(__name__)


# Binance endpoint weights from the docs. Worth keeping these accurate
# because they drive the rate limiter — a wrong weight means we'll either
# get throttled prematurely or overrun the venue's limit.
_W_NEW_ORDER = 1
_W_CANCEL_ORDER = 1
_W_OPEN_ORDERS = 6 


class BinanceOrderGateway(AbstractOrderGateway):
    """Order order_gateway for Binance Spot."""

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        clock: Clock,
        config: BinanceConfig,
        credentials: BinanceCredentials,
        symbols: SymbolMapper,
        rest_client: BinanceRESTClient | None = None,
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._config = config
        self._creds = credentials
        self._symbols = symbols
        self._rest = rest_client or BinanceRESTClient(
            config=config, credentials=credentials, clock=clock,
        )
        # Track our own OrderId -> Binance exchange_order_id, for cancels
        # that arrive after the ack. We need this because CancelRequest
        # carries our OrderId, not the exchange's.
        self._exchange_ids: dict[OrderId, ExchangeOrderId] = {}
        self._client_to_internal: dict[ClientOrderId, OrderId] = {}
        # client_order_id -> wire side string ("BUY"/"SELL"). Required by the
        # Futures PUT amend endpoint, which demands side even though the order
        # is already resting. Populated at ack time from the original OrderRequest.
        self._order_sides: dict[ClientOrderId, str] = {}
        self._started = False
        self._dropped_events: int = 0

    @property
    def venue(self) -> str:
        return "BINANCE"

    # --- Lifecycle --------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self._rest.connect()
        await self._bus.subscribe(Topic.ORDERS, self._on_order_event)

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        await self._rest.close()

    # --- Topic handler ----------------------------------------------------

    async def _on_order_event(self, event: BaseEvent) -> None:
        if isinstance(event, OrderRequest):
            await self._handle_new(event)
        elif isinstance(event, CancelRequest):
            await self._handle_cancel(event)
        elif isinstance(event, AmendRequest):
            await self._handle_amend(event)
        # Ack/reject/cancel/fill: published by us or the WS stream; ignore.

    # --- New order --------------------------------------------------------

    async def _handle_new(self, req: OrderRequest) -> None:
        if req.instrument.exchange != self.venue:
            return  # different venue's order_gateway will handle it
        try:
            params = self._build_order_params(req)
        except OrderError as exc:
            await self._publish_reject(req, f"order translation failed: {exc}")
            return

        try:
            resp = await self._rest.request(
                "POST", self._config.api_prefix + "/order",
                params=params, signed=True, weight=_W_NEW_ORDER,
            )
        except OrderError as exc:
            # Logical reject (insufficient balance, etc.) — publish rejection.
            if exc.context.get("logical_reject"):
                await self._publish_reject(
                    req, exc.message,
                    venue_code=str(exc.context.get("code", "")),
                )
                return
            # Other order errors (bad symbol, precision, etc.) — also reject.
            await self._publish_reject(
                req, exc.message,
                venue_code=str(exc.context.get("code", "")),
            )
            return
        except OrderGatewayError as exc:
            # Transport / auth / rate-limit. These are *not* order rejections —
            # the order's status is unknown. Publish a rejection with a
            # distinguishing reason so the OMS can surface to a human, but
            # ideally the OMS would retry by client_order_id on rate-limit.
            # For now: reject and log loudly.
            _log.exception("binance_new_order_gateway_error")
            await self._publish_reject(req, f"order_gateway error: {exc}")
            return

        # Map exchange ids and publish ack.
        exchange_order_id = ExchangeOrderId(str(resp["orderId"]))
        self._exchange_ids[req.order_id] = exchange_order_id
        self._client_to_internal[req.client_order_id] = req.order_id
        self._order_sides[req.client_order_id] = side_to_binance(req.side)
        await self._publish_ack(req, exchange_order_id)

        # If Binance reports the order as already DONE in this response
        # (e.g. an immediate-fill MARKET), the user-data stream will also
        # publish the fill — we just emit the ack here and let WS handle
        # the rest. The risk of duplicate fills is handled by Order's
        # _applied_fills set in the OMS.

    def _build_order_params(self, req: OrderRequest) -> dict[str, Any]:
        wire_symbol = self._symbols.wire_symbol(req.instrument)
        futures = self._config.futures
        binance_type, effective_tif = order_type_to_binance(
            req.order_type, req.time_in_force, futures=futures,
        )
        params: dict[str, Any] = {
            "symbol": wire_symbol,
            "side": side_to_binance(req.side),
            "type": binance_type,
            "quantity": self._format_decimal(req.quantity),
            "newClientOrderId": req.client_order_id,
            "newOrderRespType": "ACK",  # fills come via user-data WS
        }
        # Spot LIMIT_MAKER and MARKET must not have timeInForce set.
        # On Futures, POST_ONLY maps to LIMIT+GTX so TIF is always included.
        omit_tif = binance_type in ("MARKET", "LIMIT_MAKER")
        if not omit_tif:
            params["timeInForce"] = tif_to_binance(effective_tif)
        if req.price is not None and binance_type != "MARKET":
            params["price"] = self._format_decimal(req.price)
        if req.stop_price is not None:
            params["stopPrice"] = self._format_decimal(req.stop_price)
        return params

    # --- Cancel ----------------------------------------------------------

    async def _handle_cancel(self, req: CancelRequest) -> None:
        if req.instrument.exchange != self.venue:
            return
        wire_symbol = self._symbols.wire_symbol(req.instrument)
        # Prefer cancel-by-client-order-id; it's idempotent if the client
        # id is unique, which the OMS guarantees.
        params: dict[str, Any] = {
            "symbol": wire_symbol,
            "origClientOrderId": req.client_order_id,
        }
        try:
            await self._rest.request(
                "DELETE", self._config.api_prefix + "/order",
                params=params, signed=True, weight=_W_CANCEL_ORDER,
            )
        except OrderError as exc:
            # Cancel can fail because order doesn't exist (already done).
            # We translate that to a rejection of the cancel itself — the
            # OMS state for the order will catch up via the fill stream.
            _log.info(
                "binance_cancel_rejected",
                order_id=req.order_id,
                reason=exc.message,
            )
            await self._publish_cancel_rejected(req, exc.message)
            return
        except OrderGatewayError as exc:
            _log.exception("binance_cancel_transport_error")
            await self._publish_cancel_rejected(req, f"transport error: {exc}")
            return

        # OrderCancelled event will be published by the user-data WS
        # stream as the canonical source. We don't double-publish here —
        # the REST response confirms acceptance, not that the cancel
        # is complete.

    # --- Amend -----------------------------------------------------------

    async def _handle_amend(self, req: AmendRequest) -> None:
        """Amend a resting order using the best method available for this venue mode.

        Futures: native PUT amend — single round-trip, emits ``OrderAmended``.
        Spot: no modify endpoint — cancel the old order (emits ``OrderCancelled``),
              OMS re-places a fresh order on the next reconcile tick.

        The OMS sees only the canonical event; it is unaware of which path ran.
        """
        if req.instrument.exchange != self.venue:
            return
        if self._config.futures:
            await self._handle_amend_futures(req)
        else:
            await self._handle_amend_spot(req)

    async def _handle_amend_futures(self, req: AmendRequest) -> None:
        """Futures native amend via PUT /fapi/v1/order."""
        wire_symbol = self._symbols.wire_symbol(req.instrument)
        # Seed side from the request so adopted orders can use the native PUT path.
        self._order_sides.setdefault(req.client_order_id, side_to_binance(req.side))
        side = self._order_sides[req.client_order_id]
        params: dict[str, Any] = {
            "symbol": wire_symbol,
            "side": side,
            "origClientOrderId": req.client_order_id,
        }
        if req.new_price is not None:
            params["price"] = self._format_decimal(req.new_price)
        if req.new_quantity is not None:
            params["quantity"] = self._format_decimal(req.new_quantity)
        try:
            resp = await self._rest.request(
                "PUT", self._config.api_prefix + "/order",
                params=params, signed=True, weight=_W_NEW_ORDER,
            )
        except OrderError as exc:
            _log.info(
                "binance_futures_amend_rejected",
                order_id=req.order_id, reason=exc.message,
            )
            await self._publish_amend_reject(req, exc.message)
            return
        except OrderGatewayError as exc:
            _log.exception("binance_futures_amend_transport_error", order_id=req.order_id)
            await self._publish_amend_reject(req, f"transport error: {exc}")
            return

        new_exchange_id = ExchangeOrderId(str(resp["orderId"])) if "orderId" in resp else None

        # Publish the venue's *actual* resulting price/qty, not what we asked
        # for. The PUT can clamp or partially apply (e.g. a quantity change
        # against an already-partially-filled order), and a GTX amend that
        # would cross is silently adjusted rather than rejected on some paths.
        # Trusting req.new_* here is exactly how local state drifts from the
        # venue and orphans accumulate. Fall back to the requested value only
        # if the response omits the field.
        venue_price = resp.get("price")
        venue_qty = resp.get("origQty")
        applied_price = (
            Price(Decimal(str(venue_price)))
            if venue_price not in (None, "", "0", "0.0")
            else req.new_price
        )
        applied_qty = (
            Quantity(Decimal(str(venue_qty)))
            if venue_qty not in (None, "", "0", "0.0")
            else req.new_quantity
        )
        await self._safe_publish(
            Topic.ORDERS,
            OrderAmended(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self.venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                new_price=applied_price,
                new_quantity=applied_qty,
                new_exchange_order_id=new_exchange_id,
            ),
        )

    async def _handle_amend_spot(self, req: AmendRequest) -> None:
        """Spot cancel-replace: cancel the old order, OMS re-places on next tick."""
        wire_symbol = self._symbols.wire_symbol(req.instrument)
        try:
            await self._rest.request(
                "DELETE", self._config.api_prefix + "/order",
                params={"symbol": wire_symbol, "origClientOrderId": req.client_order_id},
                signed=True, weight=_W_CANCEL_ORDER,
            )
        except OrderError as exc:
            # Already gone (filled or cancelled) — user-data stream delivers the
            # real event. Reject the amend so the OMS exits PENDING_AMEND cleanly.
            _log.info(
                "binance_spot_amend_cancel_order_already_done",
                order_id=req.order_id, reason=exc.message,
            )
            await self._publish_amend_reject(req, f"cancel step failed: {exc.message}")
            return
        except OrderGatewayError as exc:
            _log.exception("binance_spot_amend_cancel_transport_error", order_id=req.order_id)
            await self._publish_amend_reject(req, f"transport error: {exc}")
            return

        # Cancel accepted by venue. Emit OrderCancelled immediately so the OMS
        # exits PENDING_AMEND without waiting for the user-data WS event.
        # A duplicate OrderCancelled from the WS later is harmless (CANCELLED is terminal).
        await self._safe_publish(
            Topic.ORDERS,
            OrderCancelled(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self.venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
            ),
        )

    # --- Publish helpers --------------------------------------------------

    async def _publish_ack(
        self, req: OrderRequest, exchange_order_id: ExchangeOrderId
    ) -> None:
        await self._safe_publish(
            Topic.ORDERS,
            OrderAcknowledged(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self.venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                exchange_order_id=exchange_order_id,
            ),
        )

    async def _publish_reject(
        self, req: OrderRequest, reason: str, venue_code: str = ""
    ) -> None:
        await self._safe_publish(
            Topic.ORDERS,
            OrderRejected(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self.venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                reason=reason,
                venue_error_code=venue_code or None,
            ),
        )

    async def _publish_cancel_rejected(
        self, req: CancelRequest, reason: str
    ) -> None:
        await self._safe_publish(
            Topic.ORDERS,
            CancelRejected(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self.venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                reason=reason,
            ),
        )

    async def _publish_amend_reject(self, req: AmendRequest, reason: str) -> None:
        """Signal that the cancel step of a cancel-replace failed."""
        await self._safe_publish(
            Topic.ORDERS,
            OrderRejected(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self.venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                reason=f"amend failed: {reason}",
            ),
        )

    # --- Startup sync ----------------------------------------------------

    async def cancel_stale_orders(self) -> int:
        """Cancel all open orders for every tracked instrument.

        Should be called once immediately after :meth:`start` so that any
        orders left over from a previous session do not interfere with the
        new one. Returns the total number of orders cancelled.
        """
        total = 0
        for wire_sym in self._symbols.all_wire_symbols():
            try:
                orders = await self._rest.request(
                    "GET", self._config.api_prefix + "/openOrders",
                    params={"symbol": wire_sym},
                    signed=True, weight=_W_OPEN_ORDERS,
                )
            except Exception:
                _log.exception(
                    "startup_sync_failed_to_list_open_orders", wire_symbol=wire_sym,
                )
                continue
            for order in orders:
                order_id = order.get("orderId")
                try:
                    await self._rest.request(
                        "DELETE", self._config.api_prefix + "/order",
                        params={"symbol": wire_sym, "orderId": order_id},
                        signed=True, weight=_W_CANCEL_ORDER,
                    )
                    _log.warning(
                        "startup_sync_cancelled_stale_order",
                        order_id=order_id, symbol=wire_sym,
                    )
                    total += 1
                except Exception:
                    _log.exception(
                        "startup_sync_failed_to_cancel_stale_order",
                        order_id=order_id, symbol=wire_sym,
                    )
        return total

    # --- Metrics ---------------------------------------------------------

    def snapshot(self) -> dict:
        """Return a point-in-time dict of operational counters."""
        return {
            "venue": self.venue,
            "dropped_events": self._dropped_events,
            "tracked_symbols": self._symbols.all_wire_symbols(),
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
                "bus_backpressure_order_gateway_event_dropped",
                total_drops=self._dropped_events, topic=topic,
                event_type=type(event).__name__,
            )
            return False

    @staticmethod
    def _format_decimal(d: Decimal) -> str:
        """Format Decimal for Binance. Strip trailing zeros and exponent notation.

        Binance is strict about scientific notation in numeric fields —
        ``1E-4`` will be rejected. We force fixed-point.
        """
        # ``normalize`` removes trailing zeros but may produce 1E+1 etc.
        # Convert to a plain string via format spec.
        return format(d.normalize(), "f")


__all__ = ["BinanceOrderGateway"]
