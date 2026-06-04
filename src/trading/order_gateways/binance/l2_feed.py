"""BinanceL2Feed — REST-polled L2 order book publisher.

Polls the Binance REST depth endpoint every 500ms per instrument and
publishes :class:`~trading.core.events.OrderBookEvent` to
``Topic.MARKET_DATA``.  Each REST response is a full snapshot of the top
20 price levels, so there is no WS sequence management, no gap handling,
and no bootstrap step.  The trade-off vs a diff stream is one extra REST
call per 500ms per instrument (~2 weight, well within the 2400/min limit).

This is deliberately simpler than a diff-based approach.  For an academic
prototype on Binance futures testnet the snapshot approach is reliable and
the latency (500ms refresh) is acceptable for multi-level OBI display.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Final

import structlog

from ...core.clock import Clock
from ...core.events import OrderBookEvent, OrderBookLevel
from ...core.instruments import Instrument
from ...core.types import Timestamp
from ...event_bus.base import AbstractEventBus, Topic
from .config import BinanceConfig
from .rest_client import BinanceRESTClient
from .symbols import SymbolMapper

_log = structlog.get_logger(__name__)

# Top-N levels per side fetched from REST.  20 gives enough depth for
# multi-level OBI while keeping the REST weight low (~2 on Binance futures).
_DEPTH_LEVELS: Final[int] = 20
# Weight is 2 for limit ≤ 20 on Binance Futures.
_DEPTH_WEIGHT: Final[float] = 2.0
# How often to poll each instrument.
_POLL_INTERVAL_S: Final[float] = 0.5


class BinanceL2Feed:
    """Polls Binance REST depth every 500ms, publishes OrderBookEvent to bus.

    Each response is a complete top-20 snapshot — no diff management, no
    gap detection.  All instruments are polled concurrently in a single loop.
    """

    def __init__(
        self,
        *,
        bus: AbstractEventBus,
        config: BinanceConfig,
        rest: BinanceRESTClient,
        symbols: SymbolMapper,
        instruments: list[Instrument],
        clock: Clock,
    ) -> None:
        self._bus = bus
        self._config = config
        self._rest = rest
        self._clock = clock
        self._instruments = instruments
        self._wire_to_instrument: dict[str, Instrument] = {
            symbols.wire_symbol(i): i for i in instruments
        }
        self._poll_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------
    # Public lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        _log.info("l2_feed.started", instruments=list(self._wire_to_instrument))
        self._poll_task = asyncio.create_task(self._poll_loop(), name="binance-l2-poll")

    async def stop(self) -> None:
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        _log.info("l2_feed.stopped")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        while True:
            ts_start = self._clock.now_ns()
            await asyncio.gather(
                *[self._poll_instrument(wire, inst)
                  for wire, inst in self._wire_to_instrument.items()]
            )
            elapsed_s = (self._clock.now_ns() - ts_start) / 1_000_000_000
            wait = max(0.0, _POLL_INTERVAL_S - elapsed_s)
            await asyncio.sleep(wait)

    async def _poll_instrument(self, wire: str, instrument: Instrument) -> None:
        try:
            data = await self._rest.request(
                "GET",
                self._config.api_prefix + "/depth",
                params={"symbol": wire, "limit": _DEPTH_LEVELS},
                weight=_DEPTH_WEIGHT,
            )
        except Exception:
            _log.warning("l2_feed.poll_failed", wire=wire, exc_info=True)
            return

        now_ns = Timestamp(self._clock.now_ns())
        # "T" is transaction time (ms) on futures; "E" is event time.
        ts_event_ms = data.get("T") or data.get("E")
        ts_event = Timestamp(int(ts_event_ms) * 1_000_000) if ts_event_ms else now_ns
        sequence = int(data.get("lastUpdateId", 0))

        bids = tuple(
            OrderBookLevel(price=Decimal(p), quantity=Decimal(q))
            for p, q in data.get("bids", [])
        )
        asks = tuple(
            OrderBookLevel(price=Decimal(p), quantity=Decimal(q))
            for p, q in data.get("asks", [])
        )

        if not bids and not asks:
            return

        event = OrderBookEvent(
            ts_event=ts_event,
            ts_ingest=now_ns,
            source="binance-l2",
            instrument=instrument,
            bids=bids,
            asks=asks,
            sequence=sequence,
            is_snapshot=True,
        )
        try:
            await self._bus.publish(Topic.MARKET_DATA, event)
        except Exception:
            _log.warning("l2_feed.publish_failed", wire=wire, exc_info=True)


__all__ = ["BinanceL2Feed"]
