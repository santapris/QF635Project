"""Bus-subscribed event logging — human-readable terminal output of pipeline events.

This is the single source of truth for "print events as they flow through
the bus." Stage runners and the live runner both call
:func:`subscribe_event_logging` so log formats stay consistent across runs.

This is system-level observation (B) — not to be confused with
component-internal logging (A), which lives inside each component and
remains independent of the bus.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import structlog

from ..core.events import BaseEvent, OrderRejected
from ..event_bus.base import AbstractEventBus, Topic

_Logger = structlog.stdlib.BoundLogger

# Field allowlists per topic. Order matters only for log readability.
_FIELDS_BY_TOPIC: dict[str, tuple[str, ...]] = {
    Topic.MARKET_DATA: ("instrument_id", "bid_price", "ask_price", "price", "quantity"),
    Topic.SIGNALS: ("strategy_id", "instrument_id", "side", "quantity", "order_type", "price"),
    Topic.RISK_DECISIONS: ("order_id", "reason", "rule"),
    Topic.ORDERS: (
        "order_id", "client_order_id", "status", "side", "quantity", "price",
        "reason", "venue_error_code",
    ),
    Topic.FILLS: ("order_id", "fill_quantity", "fill_price", "commission", "fee"),
    Topic.POSITIONS: ("instrument_id", "net_quantity", "average_price", "unrealised_pnl"),
    Topic.ACCOUNT: ("balances",),
    Topic.ALERTS: ("rule", "reason", "severity"),
}

# Topics subscribed by default. Market data is deliberately excluded — a
# few hundred ticks/sec in the terminal is unreadable and pushes operational
# logs out of the scrollback. The dashboard is the right surface for ticks.
# Callers who genuinely want per-tick terminal output for one session can
# pass ``topics=(Topic.MARKET_DATA, ...)`` explicitly.
_DEFAULT_TOPICS: tuple[str, ...] = (
    Topic.SIGNALS,
    Topic.RISK_DECISIONS,
    Topic.ORDERS,
    Topic.FILLS,
    Topic.POSITIONS,
    Topic.ACCOUNT,
    Topic.ALERTS,
)

# Default per-topic log levels. Alerts surface above ordinary INFO traffic.
_DEFAULT_LEVELS: dict[str, str] = {
    Topic.ALERTS: "warning",
}


def _extract(event: BaseEvent, fields: tuple[str, ...]) -> dict[str, str]:
    return {k: str(getattr(event, k)) for k in fields if hasattr(event, k)}


def _make_handler(log: _Logger, topic: str, level: str):
    fields = _FIELDS_BY_TOPIC.get(topic, ())
    event_name = topic.replace("-", "_")
    log_fn = getattr(log, level)

    async def _handle(event: Any) -> None:
        fn = log.warning if isinstance(event, OrderRejected) else log_fn
        fn(
            event_name,
            event_type=type(event).__name__,
            **_extract(event, fields),
        )

    return _handle


async def subscribe_event_logging(
    bus: AbstractEventBus,
    log: _Logger,
    *,
    topics: Iterable[str] | None = None,
) -> None:
    """Subscribe structlog handlers to bus topics for terminal output.

    Parameters
    ----------
    bus:
        The application event bus.
    log:
        A structlog logger (typically ``structlog.get_logger("runner-name")``).
    topics:
        Topics to subscribe to. Defaults to the operationally-useful subset
        (market data is excluded — see _DEFAULT_TOPICS).
    """
    if topics is None:
        topics = _DEFAULT_TOPICS
    for topic in topics:
        level = _DEFAULT_LEVELS.get(topic, "info")
        await bus.subscribe(topic, _make_handler(log, topic, level))


__all__ = ["subscribe_event_logging"]
