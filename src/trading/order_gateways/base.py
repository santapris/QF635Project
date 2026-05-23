"""OrderGateway abstraction.

A *order_gateway* is the platform's adapter to one venue. It owns the
direction-changing translation:

- inbound (from OMS): take canonical :class:`OrderRequest` /
  :class:`CancelRequest` / :class:`AmendRequest` events off the
  ``orders`` topic, convert to venue-specific REST/WebSocket calls,
  send.
- outbound (from venue): take venue responses, convert to canonical
  :class:`OrderAcknowledged` / :class:`OrderRejected` /
  :class:`OrderCancelled` events on ``orders`` and :class:`FillEvent`
  on ``fills``.

OrderGateways own venue-specific concerns: authentication, rate limits,
retry policies, error-code translation, and clock drift. The OMS
talks to all of them through this one interface.

Two implementations land in this batch:

- :class:`SimulationOrderGateway` — full-featured simulator with configurable
  fill semantics, latency, fees, and reject scenarios. The default for
  paper trading and the foundation for the backtest engine.
- Real exchange adapters (Binance, Coinbase, etc.) follow this same
  protocol; they're left for a later integration pass since each is a
  multi-day effort against a moving venue API.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from ..core.instruments import Instrument


class AbstractOrderGateway(ABC):
    """Venue adapter. Subscribes to ``orders``; publishes acks/rejects/fills."""

    @property
    @abstractmethod
    def venue(self) -> str:
        """Stable venue identifier, e.g. ``"BINANCE"``. Matches Instrument.exchange."""

    @abstractmethod
    async def start(self) -> None:
        """Bring up the order_gateway: connect, authenticate, subscribe to ``orders``."""

    @abstractmethod
    async def stop(self) -> None:
        """Shut down cleanly: drain pending, disconnect, release resources."""


class AbstractOrderGatewayRegistry(ABC):
    """Routes order events to the order_gateway that owns the relevant instrument.

    Production use: one order_gateway per venue, registry chooses by
    ``instrument.exchange``. The OMS doesn't care which venue a signal
    targets; the order_gateway selection happens here.

    For single-venue deployments, instantiate one order_gateway and skip the
    registry — the order_gateway can subscribe to ``orders`` directly.
    """

    @abstractmethod
    def register(self, order_gateway: AbstractOrderGateway, *, venues: Iterable[str]) -> None:
        """Associate a order_gateway with one or more venue identifiers."""

    @abstractmethod
    def order_gateway_for(self, instrument: Instrument) -> AbstractOrderGateway | None:
        """Return the order_gateway that handles ``instrument.exchange``, or None."""


__all__ = ["AbstractOrderGateway", "AbstractOrderGatewayRegistry"]
