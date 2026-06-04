"""Analytics service that subscribes to the event bus and computes analytics for the dashboard.

Subscribes to Topic.MARKET_DATA and publishes MicrostructureSnapshotEvents to Topic.ANALYTICS after every TickEvent, regardless of which strategy is running. 
VPIN is updated on TradeEvents. All indiicator instances are per-instrument and lazily craeted on the first event for that instrument.
"""

from __future__ import annotations

import structlog

from ..core.clock import LiveClock
from ..core.events import MicrostructureSnapshotEvent, TickEvent, TradeEvent
from ..core.types import Timestamp
from ..event_bus.base import AbstractEventBus, Topic
from .imbalance import OBI, OFI
from .microprice import Microprice
from .volatility import EWMAVolatility
from .vpin import VPIN

_log = structlog.get_logger(__name__)

class _InstrumentState: 
    """Indicaator set for single instrument. Created lazily on the first event for that instrument."""
    __slots__ = ("microprice", "obi", "ofi", "ewma_volatility", "vpin")

    def __init__(
            self,
            ofi_window_seconds: float, 
            vol_half_life_seconds: float,
            vpin_bucket_volume: float
            ) -> None:
        _clock = LiveClock()
        self.microprice = Microprice()
        self.obi = OBI()
        self.ofi = OFI(window_seconds=ofi_window_seconds, clock=_clock)
        self.vol = EWMAVolatility(half_life_seconds=vol_half_life_seconds)
        self.vpin = VPIN(bucket_volume=vpin_bucket_volume)
    
class AnalyticsService:
    """Subscribes to market data and publishes microstructure snapshots.

    Parameters mirror the defaults of Avellanda-Stoikov strategy parameters, but can be tuned independently. The service is designed to be always-on, so it does not require the strategy to be running.

    Override via constructor kwargs if needed, e.g. to align with the strategy parameters or to disable an indicator by setting its parameters to None.

    """
    def __init__(
        self,
        bus: AbstractEventBus,
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
        await self._bus.subscribe(Topic.MARKET_DATA, self._handle_market_data)
        _log.info("analytics_service_started", ofi_window_seconds=self._ofi_window_seconds, vol_half_life_seconds=self._vol_half_life_seconds, vpin_bucket_volume=self._vpin_bucket_volume)
    
    async def stop(self) -> None:
        # No need to unsubscribe since the bus is expected to be stopped when the service is stopped, but if we wanted to support hot-swapping the service we would need to unsubscribe here.
        _log.info("analytics_service_stopped")
    
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
            await self._on_trade(event)
    
    async def _on_tick(self, event: TickEvent) -> None:
        instrument = event.instrument
        state = self._get_state(instrument.instrument.id)

        bid = float(event.bid.price) if event.bid else None
        ask = float(event.ask.price) if event.ask else None
        bid_size = float(event.bid.size) if event.bid else None
        ask_size = float(event.ask.size) if event.ask else None
        ts_ns = event.ts_event

        obi_val = state.obi.update(bid_size, ask_size)
        ofi_val = state.ofi.update(bid, bid_size, ask, ask_size, ts_ns)
        micro_val = state.microprice.update(bid, bid_size, ask, ask_size)
        mid = (bid + ask) / 2 if bid is not None and ask is not None else None
        sigma_val = state.vol.update(mid, ts_ns) if mid is not None else None

        microprice = micro_val if micro_val is not None else mid

        snap = MicrostructureSnapshotEvent(
            ts_event = Timestamp(ts_ns),
            ts_ingest = Timestamp.now(),
            source = "analytics_service",
            instrument = instrument,
            bid_price = bid,
            ask_price = ask,
            bid_size = bid_size,
            ask_size = ask_size,
            mid_price = mid,
            microprice = microprice,
            sigma = sigma_val,
            obi = obi_val,
            ofi = ofi_val,
            vpin = state.vpin.value,
        )

        try: 
            await self._bus.publish(Topic.ANALYTICS, snap)
        except Exception: 
            _log.warn(
                "analytics_service_publish_failed",
                error=structlog.exc_info(),
                exc_info=True,
                instrument=instrument.instrument.id,
                event=event,
            )
    
    def _on_trade(self, event: TradeEvent) -> None:
       state = self._get_state(event.instrument.instrument.id)
       state.vpin.update(float(event.price), float(event.quantity))