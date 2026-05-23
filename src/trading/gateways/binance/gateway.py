"""Binance Spot order gateway.

Implements :class:`AbstractGateway`. Subscribes to the ``orders`` topic
for our outbound :class:`OrderRequest` / :class:`CancelRequest` /
:class:`AmendRequest` events, translates them to Binance REST calls,
and publishes the canonical :class:`OrderAcknowledged` / :class:`OrderRejected`
/ :class:`OrderCancelled` events on the same topic in response.

Important behaviour notes:

- **Idempotency via client order id.** Every order we send includes the
  caller's ``client_order_id`` as ``newClientOrderId``. If we time out
  and retry, Binance dedupes by this id — that's the whole point of
  having one. Binance restricts ``newClientOrderId`` characters; we
  do not encode or transform — the OMS's id (already ``{strat}-{hex}``
  in batch 7) is safe.

- **Fills do NOT come from REST responses.** The order endpoint
  response contains fills in some cases (notably ``MARKET`` with
  ``newOrderRespType=FULL``), but the canonical source of fills is the
  user-data WebSocket. We rely on that and ignore the fills array in
  the REST response, except for one case: an immediate full fill on
  ``newOrderRespType=ACK`` returns ``status=FILLED`` with no fill detail,
  which the user-data stream must then deliver.

- **Order-amend semantics.** Binance Spot does NOT support modify; cancel-
  replace is the idiom. We implement :meth:`_handle_amend` as
  cancel-then-place. If the cancel succeeds and the place fails, we
  publish a rejection — the OMS knows how to handle it.
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
    CancelRequest,
    OrderAcknowledged,
    OrderCancelled,
    OrderRejected,
    OrderRequest,
)
from ...core.exceptions import BackpressureError, GatewayError, OrderError
from ...core.types import (
    ClientOrderId,
    ExchangeOrderId,
    OrderId,
    OrderType,
    TimeInForce,
)
from ...event_bus.base import AbstractEventBus, Topic
from ..base import AbstractGateway
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


class BinanceGateway(AbstractGateway):
    """Order gateway for Binance Spot."""

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
            return  # different venue's gateway will handle it
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
        except GatewayError as exc:
            # Transport / auth / rate-limit. These are *not* order rejections —
            # the order's status is unknown. Publish a rejection with a
            # distinguishing reason so the OMS can surface to a human, but
            # ideally the OMS would retry by client_order_id on rate-limit.
            # For now: reject and log loudly.
            _log.exception("binance_new_order_gateway_error")
            await self._publish_reject(req, f"gateway error: {exc}")
            return

        # Map exchange ids and publish ack.
        exchange_order_id = ExchangeOrderId(str(resp["orderId"]))
        self._exchange_ids[req.order_id] = exchange_order_id
        self._client_to_internal[req.client_order_id] = req.order_id
        await self._publish_ack(req, exchange_order_id)

        # If Binance reports the order as already DONE in this response
        # (e.g. an immediate-fill MARKET), the user-data stream will also
        # publish the fill — we just emit the ack here and let WS handle
        # the rest. The risk of duplicate fills is handled by Order's
        # _applied_fills set in the OMS.

    def _build_order_params(self, req: OrderRequest) -> dict[str, Any]:
        wire_symbol = self._symbols.wire_symbol(req.instrument)
        binance_type, effective_tif = order_type_to_binance(
            req.order_type, req.time_in_force,
        )
        params: dict[str, Any] = {
            "symbol": wire_symbol,
            "side": side_to_binance(req.side),
            "type": binance_type,
            "quantity": self._format_decimal(req.quantity),
            "newClientOrderId": req.client_order_id,
            "newOrderRespType": "ACK",  # fills come via user-data WS
        }
        # MARKET orders cannot have TIF set; everything else can.
        if req.order_type is not OrderType.MARKET:
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
        except GatewayError as exc:
            _log.exception("binance_cancel_transport_error")
            await self._publish_cancel_rejected(req, f"transport error: {exc}")
            return

        # OrderCancelled event will be published by the user-data WS
        # stream as the canonical source. We don't double-publish here —
        # the REST response confirms acceptance, not that the cancel
        # is complete.

    # --- Amend (cancel-replace) ------------------------------------------

    async def _handle_amend(self, req: AmendRequest) -> None:
        if req.instrument.exchange != self.venue:
            return
        # Binance Spot doesn't support modify. Cancel, then the strategy
        # (or its agent) must place a fresh order — we don't do the
        # replace automatically because the new size/price comes from
        # AmendRequest and the strategy would need to also see the
        # cancel result before deciding whether to proceed.
        #
        # For the MVP: surface this as unsupported. A real implementation
        # would either implement cancel-replace here or push it back to
        # the OMS, which would have to take care of cross-event ordering.
        await self._publish_cancel_rejected(
            CancelRequest(
                ts_event=req.ts_event, ts_ingest=req.ts_ingest, source=req.source,
                order_id=req.order_id, client_order_id=req.client_order_id,
                instrument=req.instrument,
            ),
            reason="binance amend not implemented; cancel and resubmit",
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
        # We reuse OrderRejected to signal a cancel failure — same shape,
        # same OMS handler. The OMS sees this and knows the cancel didn't
        # take.
        await self._safe_publish(
            Topic.ORDERS,
            OrderRejected(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self.venue,
                order_id=req.order_id,
                client_order_id=req.client_order_id,
                reason=f"cancel failed: {reason}",
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
                "bus_backpressure_gateway_event_dropped",
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


__all__ = ["BinanceGateway"]
