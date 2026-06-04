"""AnalyticsService — strategy-agnostic microstructure analytics publisher.

Subscribes to Topic.MARKET_DATA and publishes MicrostructureSnapshotEvent to
Topic.ANALYTICS after every TickEvent, regardless of which strategy is running.
VPIN is updated on TradeEvents. All indicator instances are per-instrument and
lazily created on the first event for that instrument.
"""

from __future__ import annotations

import structlog

from ..core.clock import LiveClock
from ..core.events import MicrostructureSnapshotEvent, OrderBookEvent, TickEvent, TradeEvent
from ..core.types import Timestamp
from ..event_bus.base import AbstractEventBus, Topic
from .imbalance import OBI, OFI
from .microprice import Microprice
from .volatility import EWMAVolatility
from .vpin import VPIN

_log = structlog.get_logger(__name__)


class _InstrumentState:
    """Indicator set for a single instrument."""

    __slots__ = ("obi", "ofi", "microprice", "vol", "vpin",
                 "obi_l2", "depth_bid_total", "depth_ask_total")

    def __init__(
        self,
        ofi_window_seconds: float,
        vol_half_life_seconds: float,
        vpin_bucket_volume: float,
    ) -> None:
        _clock = LiveClock()
        self.obi = OBI()
        self.ofi = OFI(window_seconds=ofi_window_seconds, clock=_clock)
        self.microprice = Microprice()
        self.vol = EWMAVolatility(half_life_seconds=vol_half_life_seconds)
        self.vpin = VPIN(bucket_volume=vpin_bucket_volume)
        # L2 state — updated by _on_book(), included in every tick snapshot.
        self.obi_l2: float | None = None
        self.depth_bid_total: float | None = None
        self.depth_ask_total: float | None = None


class AnalyticsService:
    """Publishes MicrostructureSnapshotEvent from raw market data.

    Parameters mirror the defaults used by AvellanedaStoikovStrategy so the
    dashboard shows consistent values when both run together. Override via
    constructor kwargs if needed.
    """

    def __init__(
        self,
        bus: AbstractEventBus,
        *,
        ofi_window_seconds: float = 10.0,
        vol_half_life_seconds: float = 60.0,
        vpin_bucket_volume: float = 1.0,
    ) -> None:
        self._bus = bus
        self._ofi_window_seconds = ofi_window_seconds
        self._vol_half_life_seconds = vol_half_life_seconds
        self._vpin_bucket_volume = vpin_bucket_volume
        self._instruments: dict[str, _InstrumentState] = {}

    async def start(self) -> None:
        await self._bus.subscribe(Topic.MARKET_DATA, self._on_market_data)
        _log.info("analytics_service.started")

    async def stop(self) -> None:
        _log.info("analytics_service.stopped")

    def _get_state(self, instrument_id: str) -> _InstrumentState:
        if instrument_id not in self._instruments:
            self._instruments[instrument_id] = _InstrumentState(
                ofi_window_seconds=self._ofi_window_seconds,
                vol_half_life_seconds=self._vol_half_life_seconds,
                vpin_bucket_volume=self._vpin_bucket_volume,
            )
        return self._instruments[instrument_id]

    async def _on_market_data(self, event: object) -> None:
        if isinstance(event, TickEvent):
            await self._on_tick(event)
        elif isinstance(event, TradeEvent):
            self._on_trade(event)
        elif isinstance(event, OrderBookEvent):
            self._on_book(event)

    async def _on_tick(self, event: TickEvent) -> None:
        instrument = event.instrument
        state = self._get_state(instrument.instrument_id)

        bid = float(event.bid_price)
        ask = float(event.ask_price)
        bid_size = float(event.bid_size)
        ask_size = float(event.ask_size)
        ts_ns: int = event.ts_event

        obi_val = state.obi.update(bid_size, ask_size)
        ofi_val = state.ofi.update(bid, bid_size, ask, ask_size, ts_ns)
        micro_val = state.microprice.update(bid, bid_size, ask, ask_size)
        mid = (bid + ask) / 2.0
        sigma_val = state.vol.update((bid + ask) / 2.0, ts_ns)

        microprice = micro_val if micro_val is not None else mid

        snap = MicrostructureSnapshotEvent(
            ts_event=Timestamp(ts_ns),
            ts_ingest=Timestamp(ts_ns),
            source="analytics_service",
            instrument=instrument,
            bid_price=bid,
            ask_price=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            mid_price=mid,
            microprice=microprice,
            sigma=sigma_val,
            obi=obi_val,
            ofi=ofi_val,
            vpin=state.vpin.value,
            obi_l2=state.obi_l2,
            depth_bid_total=state.depth_bid_total,
            depth_ask_total=state.depth_ask_total,
        )

        try:
            await self._bus.publish(Topic.ANALYTICS, snap)
        except Exception:
            _log.warning(
                "analytics_service.publish_failed",
                instrument=instrument.instrument_id,
                exc_info=True,
            )

    def _on_trade(self, event: TradeEvent) -> None:
        state = self._get_state(event.instrument.instrument_id)
        state.vpin.update(float(event.price), float(event.quantity))

    def _on_book(self, event: OrderBookEvent) -> None:
        state = self._get_state(event.instrument.instrument_id)
        bid_total = sum(float(level.quantity) for level in event.bids)
        ask_total = sum(float(level.quantity) for level in event.asks)
        total = bid_total + ask_total
        state.obi_l2 = (bid_total - ask_total) / total if total > 0 else None
        state.depth_bid_total = bid_total
        state.depth_ask_total = ask_total
