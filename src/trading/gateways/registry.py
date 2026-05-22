"""Simple gateway registry.

Maps venue identifier -> gateway. Single-venue deployments can skip the
registry entirely; the gateway subscribes to ``orders`` directly and
filters by ``req.instrument.exchange``.
"""

from __future__ import annotations

from collections.abc import Iterable

from ..core.exceptions import ConfigError
from ..core.instruments import Instrument
from .base import AbstractGateway, AbstractGatewayRegistry


class GatewayRegistry(AbstractGatewayRegistry):
    """In-memory venue -> gateway map."""

    def __init__(self) -> None:
        self._by_venue: dict[str, AbstractGateway] = {}

    def register(self, gateway: AbstractGateway, *, venues: Iterable[str]) -> None:
        venue_list = list(venues) or [gateway.venue]
        for venue in venue_list:
            if venue in self._by_venue:
                raise ConfigError(
                    f"venue {venue!r} already registered to another gateway"
                )
            self._by_venue[venue] = gateway

    def gateway_for(self, instrument: Instrument) -> AbstractGateway | None:
        return self._by_venue.get(instrument.exchange)


__all__ = ["GatewayRegistry"]
