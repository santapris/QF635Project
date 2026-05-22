"""Feed handler orchestration.

Wires together a connector, normalizer, and event bus. Adds the parts
that are policy rather than translation:

- exponential-backoff reconnect with a circuit breaker
- heartbeat watchdog that forces reconnect on stale feeds
- topic dispatch (which canonical event goes on which bus topic)
- alert publication for operators
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from ..core.clock import Clock
from ..core.events import (
    BaseEvent,
    FundingRateEvent,
    OrderBookEvent,
    RiskAlertEvent,
    TickEvent,
    TradeEvent,
)
from ..core.exceptions import FeedDisconnectedError
from ..core.instruments import Instrument
from ..core.types import Severity, Symbol
from ..event_bus.base import AbstractEventBus, Topic
from .base import AbstractConnector, AbstractNormalizer, InstrumentLookup

_log = logging.getLogger(__name__)

_NS_PER_SECOND = 1_000_000_000


@dataclass(frozen=True, slots=True)
class FeedHandlerConfig:
    """Tunable policy. Defaults match the architecture spec."""

    stale_threshold_seconds: float = 30.0
    """Force reconnect if no message arrives within this window."""

    max_reconnect_attempts: int = 10
    """After this many consecutive failures, raise FeedUnavailable alert and stop."""

    backoff_initial_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    """Reconnect delay grows: 1s, 2s, 4s, ... capped at backoff_max."""


def _topic_for(event: BaseEvent) -> str:
    """Map an event type to its bus topic. Single source of truth."""
    if isinstance(event, (TickEvent, TradeEvent, OrderBookEvent, FundingRateEvent)):
        return Topic.MARKET_DATA
    if isinstance(event, RiskAlertEvent):
        return Topic.ALERTS
    raise ValueError(f"feed handler does not know how to route {type(event).__name__}")


class FeedHandler:
    """One feed handler per venue. Owns the connection lifecycle."""

    def __init__(
        self,
        *,
        connector: AbstractConnector,
        normalizer: AbstractNormalizer,
        bus: AbstractEventBus,
        clock: Clock,
        instruments: dict[Symbol, Instrument],
        source: str,
        config: FeedHandlerConfig | None = None,
    ) -> None:
        self._connector = connector
        self._normalizer = normalizer
        self._bus = bus
        self._clock = clock
        self._instruments = instruments
        self._source = source
        self._cfg = config or FeedHandlerConfig()

        self._stopping = False
        self._running = False
        self._last_message_monotonic_ns: int | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None

    # --- Public API --------------------------------------------------------

    async def run(self) -> None:
        """Main loop. Returns when stopped or after circuit breaker fires."""
        if self._running:
            raise RuntimeError("FeedHandler already running")
        self._running = True
        self._stopping = False
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_watchdog(), name=f"feed-watchdog-{self._source}"
        )

        attempt = 0
        try:
            while not self._stopping:
                try:
                    await self._connector.connect()
                    attempt = 0  # reset on successful connection
                    self._last_message_monotonic_ns = self._clock.monotonic_ns()
                    await self._consume_loop()
                except FeedDisconnectedError as exc:
                    if self._stopping:
                        break
                    attempt += 1
                    if attempt > self._cfg.max_reconnect_attempts:
                        await self._publish_circuit_breaker_alert(exc)
                        break
                    delay = self._backoff_for(attempt)
                    _log.warning(
                        "feed disconnected; reconnect in %.1fs (attempt %d/%d): %s",
                        delay, attempt, self._cfg.max_reconnect_attempts, exc,
                    )
                    await asyncio.sleep(delay)
                except Exception:
                    # Anything else is unexpected — log loudly and break.
                    # We do not attempt to reconnect on unknown errors,
                    # because the failure mode is unknown.
                    _log.exception("feed handler crashed; stopping")
                    raise
                finally:
                    await self._connector.disconnect()
        finally:
            await self._cleanup()

    async def stop(self) -> None:
        """Request a graceful stop. ``run`` will return shortly afterwards."""
        self._stopping = True
        await self._connector.disconnect()

    # --- Loops -------------------------------------------------------------

    async def _consume_loop(self) -> None:
        """Drain the connector's message stream, normalize, publish."""
        async for raw in self._connector.messages():
            self._last_message_monotonic_ns = self._clock.monotonic_ns()
            try:
                events = self._normalizer.normalize(raw, self._instrument_lookup)
            except Exception:
                _log.exception(
                    "normalizer raised on payload from %s; skipping frame",
                    raw.source,
                )
                continue
            for event in events:
                await self._bus.publish(_topic_for(event), event)

    async def _heartbeat_watchdog(self) -> None:
        """Background task: forces reconnect if the feed goes silent."""
        # Poll at half the threshold but never tighter than 10 ms to keep
        # the loop cheap. Tests can use sub-second thresholds; production
        # values are typically 10–60 s and the lower bound never matters.
        check_interval = max(0.01, self._cfg.stale_threshold_seconds / 2)
        threshold_ns = int(self._cfg.stale_threshold_seconds * _NS_PER_SECOND)
        while not self._stopping:
            try:
                await asyncio.sleep(check_interval)
            except asyncio.CancelledError:
                return
            if self._last_message_monotonic_ns is None:
                continue
            elapsed_ns = self._clock.monotonic_ns() - self._last_message_monotonic_ns
            if elapsed_ns > threshold_ns:
                _log.warning(
                    "feed %s stale: no messages for %.1fs; forcing reconnect",
                    self._source, elapsed_ns / _NS_PER_SECOND,
                )
                await self._publish_stale_feed_alert(elapsed_ns)
                # Forcing disconnect causes the consume loop to raise
                # FeedDisconnectedError and the run loop to reconnect.
                await self._connector.disconnect()
                # Reset so we don't fire the alert again immediately.
                self._last_message_monotonic_ns = None

    # --- Helpers -----------------------------------------------------------

    def _backoff_for(self, attempt: int) -> float:
        """Exponential backoff: 1s, 2s, 4s, ... capped."""
        delay = self._cfg.backoff_initial_seconds * (2 ** (attempt - 1))
        return min(delay, self._cfg.backoff_max_seconds)

    def _instrument_lookup(self, symbol: Symbol) -> Instrument:
        try:
            return self._instruments[symbol]
        except KeyError as exc:
            raise KeyError(
                f"unknown symbol {symbol!r} from {self._source}"
            ) from exc

    async def _publish_stale_feed_alert(self, elapsed_ns: int) -> None:
        await self._bus.publish(
            Topic.ALERTS,
            RiskAlertEvent(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self._source,
                rule_name="feed_handler.stale_feed",
                severity=Severity.WARN,
                message=f"no market data for {elapsed_ns / _NS_PER_SECOND:.1f}s",
                metadata={"feed_source": self._source},
            ),
        )

    async def _publish_circuit_breaker_alert(
        self, last_exc: BaseException
    ) -> None:
        await self._bus.publish(
            Topic.ALERTS,
            RiskAlertEvent(
                ts_event=self._clock.now_ns(),
                ts_ingest=self._clock.now_ns(),
                source=self._source,
                rule_name="feed_handler.circuit_breaker",
                severity=Severity.BLOCK,
                message=(
                    f"feed unavailable after "
                    f"{self._cfg.max_reconnect_attempts} reconnect attempts: "
                    f"{last_exc}"
                ),
                metadata={"feed_source": self._source},
            ),
        )

    async def _cleanup(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        self._running = False


__all__ = ["FeedHandler", "FeedHandlerConfig"]
