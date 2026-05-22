"""Feed handler base abstractions.

A *connector* owns a transport (WebSocket, REST poller, file replay) and
yields raw exchange frames. A *normalizer* translates those raw frames
into canonical events. The :class:`FeedHandler` engine wires them
together and adds reconnect, heartbeat, and publish responsibilities.

The split exists because connection lifecycle (WebSocket reconnection
with exponential backoff, auth handshake, etc.) is orthogonal to message
format. We can swap a Binance WebSocket connector for a Binance REST
poller without touching the normalizer.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

from ..core.events import BaseEvent
from ..core.instruments import Instrument
from ..core.types import Symbol, Timestamp


@dataclass(frozen=True, slots=True)
class RawMessage:
    """One raw frame as it came off the wire, with our receive timestamp.

    ``payload`` is intentionally typed ``Any``: it might be raw bytes,
    decoded JSON, a protobuf, an arrow batch — whatever the connector's
    transport produces. Normalizers know what to expect from their paired
    connector.

    ``ts_ingest`` is captured by the connector at the earliest possible
    moment after read, so the wire-to-bus latency we report is a tight
    bound on actual processing time.
    """

    payload: Any
    ts_ingest: Timestamp
    source: str


# Resolves a venue-specific symbol string to our canonical Instrument.
# Injected so normalizers don't have to know about the instrument registry.
InstrumentLookup = Callable[[Symbol], Instrument]


class AbstractConnector(ABC):
    """Owns the connection to a single venue and yields raw messages.

    Implementations must:

    - Capture ``ts_ingest`` at the earliest read point.
    - Tag each message with a stable ``source`` string so downstream
      consumers and audit logs can identify the origin.
    - Raise :class:`~trading.core.exceptions.FeedDisconnectedError` from
      :meth:`messages` when the underlying transport drops, so the engine
      can apply its reconnect policy.
    """

    @abstractmethod
    async def connect(self) -> None:
        """Establish the underlying transport. May raise on auth failure."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close cleanly. Idempotent — safe to call multiple times."""

    @abstractmethod
    def messages(self) -> AsyncIterator[RawMessage]:
        """Async-iterate raw frames until disconnected."""


class AbstractNormalizer(ABC):
    """Stateless translator from raw venue frames to canonical events.

    Implementations should be pure functions of (raw, lookup) — no I/O,
    no hidden state. State that *must* persist across messages (e.g. an
    order book) belongs in dedicated objects (:class:`L2OrderBook`)
    threaded through the engine, not buried inside the normalizer.
    """

    @abstractmethod
    def normalize(
        self,
        raw: RawMessage,
        instrument_lookup: InstrumentLookup,
    ) -> list[BaseEvent]:
        """Convert one raw frame into zero or more canonical events.

        Returning ``[]`` is normal — heartbeats, subscription
        confirmations, and other control frames produce no events.
        """


__all__ = [
    "AbstractConnector",
    "AbstractNormalizer",
    "InstrumentLookup",
    "RawMessage",
]
