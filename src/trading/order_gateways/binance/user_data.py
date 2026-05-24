"""Binance user data stream (private WebSocket).

Subscribes to the user data stream at
``wss://stream.binance.com:9443/ws/<listenKey>`` and converts the
``executionReport`` events Binance emits into our canonical events:

- ``executionReport`` with ``x=NEW`` → :class:`OrderAcknowledged`
  (already published by the order_gateway on REST ack; we suppress this one)
- ``executionReport`` with ``x=TRADE`` → :class:`FillEvent`
- ``executionReport`` with ``x=CANCELED`` → :class:`OrderCancelled`
- ``executionReport`` with ``x=EXPIRED`` → :class:`OrderCancelled`
  (reason: expired)
- ``executionReport`` with ``x=REJECTED`` → :class:`OrderRejected`
- ``outboundAccountPosition`` → forwarded to the balance reconciler

This module owns the run loop: connect via listen key, receive frames,
normalize, publish, reconnect on disconnect or listen-key recreation.
"""

from __future__ import annotations

import asyncio
import json
import structlog
from collections.abc import Callable
from decimal import Decimal
from typing import Final
from uuid import uuid4

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
except ImportError:  # pragma: no cover
    websockets = None  # type: ignore[assignment]
    ConnectionClosed = Exception  # type: ignore[misc,assignment]

from ...core.clock import Clock
from ...core.events import (
    AccountBalance,
    AccountSnapshotEvent,
    BaseEvent,
    FillEvent,
    OrderCancelled,
    OrderRejected,
)
from ...core.exceptions import FeedDisconnectedError
from ...core.types import (
    ClientOrderId,
    ExchangeOrderId,
    FillId,
    OrderId,
    Side,
    StrategyId,
    Timestamp,
)
from ...event_bus.base import AbstractEventBus, Topic
from .config import BinanceConfig
from .listen_key import ListenKeyManager
from .symbols import SymbolMapper

_log = structlog.get_logger(__name__)

_BACKOFF_INITIAL_SEC: Final[float] = 1.0
_BACKOFF_MAX_SEC: Final[float] = 60.0


class BinanceUserDataStream:
    """Connects to the user data WS and publishes canonical fill/order events.

    The stream needs to know how to map Binance fills back to OMS-internal
    OrderId values. The OMS sends orders via REST using the
    ``newClientOrderId`` field; Binance echoes the same ``c`` field on
    every executionReport. So we map by ``client_order_id`` (which the
    OMS already generates uniquely per order).

    Strategy attribution: the user-data stream only knows the client order
    id, not the strategy that sent it. Pass ``oms.strategy_id_for_client_order``
    as ``strategy_id_lookup`` to bridge OMS attribution to fill events.
    """

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        clock: Clock,
        config: BinanceConfig,
        listen_key_manager: ListenKeyManager,
        symbols: SymbolMapper,
        strategy_id_lookup: Callable[[ClientOrderId], StrategyId | None],
    ) -> None:
        if websockets is None:
            raise ImportError(
                "websockets is required for the Binance user data stream. "
                "Install with: pip install 'websockets>=12'"
            )
        self._bus = bus
        self._clock = clock
        self._config = config
        self._listen_keys = listen_key_manager
        self._symbols = symbols
        self._strategy_id_lookup = strategy_id_lookup
        # Track which order_ids we've seen acks for so we can dedupe the
        # ack between the REST response and the WS executionReport. The
        # order_gateway publishes the ack on REST; the WS executionReport with
        # x=NEW would publish a second one, so we suppress those.
        self._task: asyncio.Task[None] | None = None
        self._stop = False
        # Map client_order_id -> our OrderId. Populated as we see acks
        # come back from the REST order_gateway via the bus.
        self._client_to_order_id: dict[ClientOrderId, OrderId] = {}

    # --- Lifecycle -------------------------------------------------------

    async def start(self) -> None:
        if self._task is not None:
            return
        # We need to know our OrderId for each client_order_id Binance reports
        # — so subscribe to the orders topic and snoop on OrderAcknowledged
        # events the order_gateway publishes.
        await self._bus.subscribe(Topic.ORDERS, self._on_order_event)
        self._task = asyncio.create_task(
            self._run_loop(), name="binance-user-data-stream"
        )

    async def stop(self) -> None:
        self._stop = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # --- Bus snoop -------------------------------------------------------

    async def _on_order_event(self, event: BaseEvent) -> None:
        """Snoop on OrderAcknowledged to capture client_order_id -> order_id.

        We don't republish — that's the order_gateway's job. We just learn the
        mapping so we can stamp incoming WS fills with the correct OrderId.
        """
        from ...core.events import OrderAcknowledged
        if isinstance(event, OrderAcknowledged):
            self._client_to_order_id[event.client_order_id] = event.order_id

    # --- Run loop --------------------------------------------------------

    async def _run_loop(self) -> None:
        backoff = _BACKOFF_INITIAL_SEC
        while not self._stop:
            try:
                key = await self._listen_keys.wait_for_recreation()
                await self._stream_one_session(key)
                backoff = _BACKOFF_INITIAL_SEC  # successful run resets backoff
            except asyncio.CancelledError:
                return
            except FeedDisconnectedError as exc:
                if self._stop:
                    return
                _log.warning("binance_user_data_ws_disconnected_reconnecting", error=str(exc), backoff_seconds=backoff)
                await asyncio.sleep(backoff)
                backoff = min(_BACKOFF_MAX_SEC, backoff * 2)
            except Exception:
                if self._stop:
                    return
                _log.exception("binance_user_data_stream_error_reconnecting", backoff_seconds=backoff)
                await asyncio.sleep(backoff)
                backoff = min(_BACKOFF_MAX_SEC, backoff * 2)

    async def _stream_one_session(self, listen_key: str) -> None:
        """Connect with ``listen_key`` and drain until disconnect or key change."""
        url = f"{self._config.ws_base_url}/ws/{listen_key}"
        _log.info("connecting_to_binance_user_data_ws")
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                max_size=2**20,
                close_timeout=5,
            ) as ws:
                _log.info("binance_user_data_ws_connected", url=url)
                # Race the WS message loop against the listen-key recreation
                # event. If the key gets recreated (expiry recovery), we
                # tear down this session and the outer loop reconnects.
                recreate_task = asyncio.create_task(
                    self._listen_keys.wait_for_recreation()
                )
                try:
                    while not self._stop:
                        recv_task = asyncio.create_task(ws.recv())
                        done, _ = await asyncio.wait(
                            {recv_task, recreate_task},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if recreate_task in done:
                            recv_task.cancel()
                            _log.info("listen_key_recreated_reconnecting_user_data_ws")
                            return
                        # recv_task completed
                        try:
                            raw = recv_task.result()
                        except ConnectionClosed as exc:
                            raise FeedDisconnectedError(
                                f"user data WS closed: {exc}",
                                source="binance-user-data",
                            ) from exc
                        await self._handle_frame(raw)
                finally:
                    recreate_task.cancel()
        except FeedDisconnectedError:
            raise
        except ConnectionClosed as exc:
            raise FeedDisconnectedError(
                f"user data WS closed: {exc}",
                source="binance-user-data",
            ) from exc

    # --- Frame handling --------------------------------------------------

    async def _handle_frame(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            text = raw.decode("utf-8")
        else:
            text = raw
        try:
            msg = json.loads(text)
        except json.JSONDecodeError:
            _log.warning("user_data_ws_non_json", text=text[:200])
            return

        event_type = msg.get("e")
        if event_type == "executionReport":
            await self._handle_execution_report(msg)
        elif event_type == "outboundAccountPosition":
            await self._handle_account_position(msg)
        elif event_type == "ORDER_TRADE_UPDATE":
            # Futures: order fields are nested under "o". Hoist event time
            # ("E") and transaction time ("T") so downstream parsing matches
            # the spot layout.
            order = dict(msg.get("o", {}))
            order.setdefault("E", msg.get("E"))
            order.setdefault("T", order.get("T", msg.get("T", msg.get("E"))))
            await self._handle_execution_report(order)
        elif event_type == "ACCOUNT_UPDATE":
            await self._handle_futures_account_update(msg)
        elif event_type == "TRADE_LITE":
            # Lightweight duplicate of the order's TRADE — full data still
            # arrives on ORDER_TRADE_UPDATE. Skip silently.
            pass
        elif event_type == "listKeyExpired":
            # Some Binance docs mention this; not always sent. The
            # listen-key keepalive normally prevents it. If it arrives,
            # treat as a disconnect — the manager will issue a new key.
            _log.warning("binance_reported_listen_key_expired")
            raise FeedDisconnectedError(
                "listenKey expired", source="binance-user-data",
            )
        else:
            # Unknown event types: log and continue. Binance occasionally
            # adds new ones; we shouldn't crash on the future.
            _log.debug("ignoring_binance_user_data_event_type", event_type=event_type)

    async def _handle_execution_report(self, msg: dict) -> None:
        """Parse and publish from an executionReport message.

        Binance field names (selected; full list in their docs):
            e  : event type ("executionReport")
            E  : event time (ms)
            s  : symbol
            c  : clientOrderId (current; new on amend)
            S  : side
            o  : order type
            f  : timeInForce
            q  : original quantity
            p  : price
            x  : execution type (NEW, TRADE, CANCELED, REJECTED, EXPIRED, REPLACED)
            X  : status
            r  : reject reason
            i  : orderId (exchange)
            l  : last filled qty
            z  : cumulative filled qty
            L  : last filled price
            n  : commission
            N  : commission asset
            T  : transaction time (ms)
            t  : trade id
            m  : isMaker
        """
        exec_type = msg.get("x")
        wire_symbol = msg.get("s", "")
        instrument = self._symbols.by_wire(wire_symbol)
        if instrument is None:
            # Fill on a symbol we don't have an instrument for. Ignore;
            # this typically means a manually-placed order on Binance UI.
            _log.debug("user_data_ws_event_for_unknown_symbol_ignoring", wire_symbol=wire_symbol)
            return

        client_order_id = ClientOrderId(str(msg.get("c", "")))
        exchange_order_id = ExchangeOrderId(str(msg.get("i", "")))
        order_id = self._client_to_order_id.get(client_order_id)
        if order_id is None:
            # We saw an executionReport for an order we never sent (or
            # whose ack we missed). Skip rather than fabricate an OrderId
            # — the OMS would reject the fill anyway.
            _log.warning(
                "user_data_ws_event_for_unknown_client_order_id_skipping",
                client_order_id=client_order_id,
            )
            return

        ts_event = Timestamp(int(msg.get("T", msg.get("E", 0))) * 1_000_000)
        ts_ingest = self._clock.now_ns()
        strategy_id = self._strategy_id_lookup(client_order_id) or StrategyId("unknown")

        if exec_type == "TRADE":
            await self._publish_fill(
                msg, instrument, order_id, client_order_id, exchange_order_id,
                strategy_id, ts_event, ts_ingest,
            )
        elif exec_type == "CANCELED":
            await self._bus.publish(
                Topic.ORDERS,
                OrderCancelled(
                    ts_event=ts_event, ts_ingest=ts_ingest, source="binance",
                    order_id=order_id,
                    client_order_id=client_order_id,
                    reason="binance reported CANCELED",
                ),
            )
        elif exec_type == "EXPIRED":
            await self._bus.publish(
                Topic.ORDERS,
                OrderCancelled(
                    ts_event=ts_event, ts_ingest=ts_ingest, source="binance",
                    order_id=order_id,
                    client_order_id=client_order_id,
                    reason=f"binance EXPIRED: {msg.get('r', '')}",
                ),
            )
        elif exec_type == "REJECTED":
            await self._bus.publish(
                Topic.ORDERS,
                OrderRejected(
                    ts_event=ts_event, ts_ingest=ts_ingest, source="binance",
                    order_id=order_id,
                    client_order_id=client_order_id,
                    reason=f"binance REJECTED: {msg.get('r', '')}",
                ),
            )
        elif exec_type == "NEW":
            # Binance sends NEW on every order acceptance. The order_gateway
            # already published OrderAcknowledged on REST. Suppress to
            # avoid duplicate events.
            pass
        else:
            _log.debug("unhandled_execution_report", exec_type=exec_type)

    async def _publish_fill(
        self,
        msg: dict,
        instrument,
        order_id: OrderId,
        client_order_id: ClientOrderId,
        exchange_order_id: ExchangeOrderId,
        strategy_id: StrategyId,
        ts_event: Timestamp,
        ts_ingest: Timestamp,
    ) -> None:
        side = Side.BUY if msg.get("S", "").upper() == "BUY" else Side.SELL
        last_qty = Decimal(str(msg["l"]))
        last_price = Decimal(str(msg["L"]))
        cum_qty = Decimal(str(msg["z"]))
        original_qty = Decimal(str(msg["q"]))
        leaves = original_qty - cum_qty
        fee = Decimal(str(msg.get("n", "0")))
        fee_currency = str(msg.get("N", "") or "")
        is_maker = bool(msg.get("m", False))
        venue_trade_id = str(msg.get("t", "") or "")

        await self._bus.publish(
            Topic.FILLS,
            FillEvent(
                fill_id=FillId(uuid4()),
                ts_event=ts_event, ts_ingest=ts_ingest, source="binance",
                order_id=order_id,
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                strategy_id=strategy_id,
                instrument=instrument,
                side=side,
                fill_price=last_price,
                fill_quantity=last_qty,
                cumulative_quantity=cum_qty,
                leaves_quantity=leaves,
                fee=fee,
                fee_currency=fee_currency,
                is_maker=is_maker,
                venue_trade_id=venue_trade_id,
            ),
        )

    async def _handle_account_position(self, msg: dict) -> None:
        """Publish an AccountSnapshotEvent from outboundAccountPosition.

        Binance fields: ``B`` is the balances array; each entry has
        ``a`` (asset), ``f`` (free), ``l`` (locked). ``E`` is event time (ms).
        """
        raw_balances = msg.get("B") or []
        balances = tuple(
            AccountBalance(
                asset=str(b.get("a", "")),
                free=Decimal(str(b.get("f", "0"))),
                locked=Decimal(str(b.get("l", "0"))),
            )
            for b in raw_balances
        )
        event_time_ms = msg.get("E")
        ts_event = (
            Timestamp(int(event_time_ms) * 1_000_000)
            if event_time_ms is not None
            else self._clock.now_ns()
        )
        await self._bus.publish(
            Topic.ACCOUNT,
            AccountSnapshotEvent(
                ts_event=ts_event,
                ts_ingest=self._clock.now_ns(),
                source="binance",
                balances=balances,
            ),
        )

    async def _handle_futures_account_update(self, msg: dict) -> None:
        """Publish an AccountSnapshotEvent from an ACCOUNT_UPDATE frame.

        Futures layout: ``a.B`` is the balances array; each entry has
        ``a`` (asset), ``wb`` (wallet balance), ``cw`` (cross wallet).
        Futures has no separate "free/locked" split — use ``wb`` as free
        and 0 as locked so the dashboard shape stays consistent.
        """
        account = msg.get("a") or {}
        raw_balances = account.get("B") or []
        balances = tuple(
            AccountBalance(
                asset=str(b.get("a", "")),
                free=Decimal(str(b.get("wb", "0"))),
                locked=Decimal(0),
            )
            for b in raw_balances
        )
        event_time_ms = msg.get("E")
        ts_event = (
            Timestamp(int(event_time_ms) * 1_000_000)
            if event_time_ms is not None
            else self._clock.now_ns()
        )
        await self._bus.publish(
            Topic.ACCOUNT,
            AccountSnapshotEvent(
                ts_event=ts_event,
                ts_ingest=self._clock.now_ns(),
                source="binance",
                balances=balances,
            ),
        )


__all__ = ["BinanceUserDataStream"]
