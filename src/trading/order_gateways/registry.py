"""Simple order_gateway registry.

Maps venue identifier -> order_gateway. Single-venue deployments can skip the
registry entirely; the order_gateway subscribes to ``orders`` directly and
filters by ``req.instrument.exchange``.
"""

from __future__ import annotations

from collections.abc import Iterable

from ..core.exceptions import ConfigError
from ..core.instruments import Instrument
from .base import AbstractOrderGateway, AbstractOrderGatewayRegistry


class OrderGatewayRegistry(AbstractOrderGatewayRegistry):
    """In-memory venue -> order_gateway map."""

    def __init__(self) -> None:
        self._by_venue: dict[str, AbstractOrderGateway] = {}

    def register(self, order_gateway: AbstractOrderGateway, *, venues: Iterable[str]) -> None:
        venue_list = list(venues) or [order_gateway.venue]
        for venue in venue_list:
            if venue in self._by_venue:
                raise ConfigError(
                    f"venue {venue!r} already registered to another order_gateway"
                )
            self._by_venue[venue] = order_gateway

    def order_gateway_for(self, instrument: Instrument) -> AbstractOrderGateway | None:
        return self._by_venue.get(instrument.exchange)


__all__ = ["OrderGatewayRegistry"]
