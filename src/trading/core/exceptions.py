"""Exception hierarchy for the trading platform.

Conventions:
- All custom exceptions inherit from ``TradingError`` so callers can catch
  one root and know it came from us.
- Subclasses are used at module boundaries; the OMS raises ``OrderError``
  subclasses, the risk engine raises ``RiskError`` subclasses, etc.
- Exceptions carry structured context (not just a string) so they can be
  serialized into alert events without losing information.
"""

from __future__ import annotations

from typing import Any


class TradingError(Exception):
    """Root of all platform-specific exceptions."""

    def __init__(self, message: str, /, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context: dict[str, Any] = context

    def __repr__(self) -> str:
        ctx = ", ".join(f"{k}={v!r}" for k, v in self.context.items())
        return f"{type(self).__name__}({self.message!r}{', ' + ctx if ctx else ''})"


# --- Configuration / startup ----------------------------------------------


class ConfigError(TradingError):
    """Invalid or missing configuration."""


# --- Event bus ------------------------------------------------------------


class EventBusError(TradingError):
    """Base class for event bus failures."""


class TopicNotFoundError(EventBusError):
    """Subscribed to a topic that does not exist (strict bus only)."""


class BackpressureError(EventBusError):
    """Publisher overran a bounded queue."""


# --- Feed handler ---------------------------------------------------------


class FeedError(TradingError):
    """Base class for market data ingestion errors."""


class FeedDisconnectedError(FeedError):
    """WebSocket dropped; reconnect logic should engage."""


class StaleFeedError(FeedError):
    """No data received within the heartbeat threshold."""


class SequenceGapError(FeedError):
    """Detected a non-monotonic or skipped sequence number."""


# --- Risk -----------------------------------------------------------------


class RiskError(TradingError):
    """Base class for risk engine errors."""


class RiskRejection(RiskError):
    """A signal was rejected by a risk rule. Carries rule name + reason."""


class KillSwitchEngaged(RiskError):
    """The kill switch has been triggered. No new orders may flow."""


# --- OMS / Execution ------------------------------------------------------


class OrderError(TradingError):
    """Base class for order lifecycle errors."""


class InvalidStateTransitionError(OrderError):
    """Tried to move an order to an illegal state."""


class OrderNotFoundError(OrderError):
    """Lookup by id failed."""


class GatewayError(TradingError):
    """Base class for exchange gateway errors."""


class RateLimitedError(GatewayError):
    """Exchange returned a rate-limit response. Carries retry-after seconds."""


class GatewayAuthError(GatewayError):
    """Authentication with the exchange failed."""


# --- Position / accounting ------------------------------------------------


class PositionError(TradingError):
    """Base class for position engine errors."""


class ReconciliationMismatch(PositionError):
    """Internal position differs from exchange-reported position."""


__all__ = [
    "BackpressureError",
    "ConfigError",
    "EventBusError",
    "FeedDisconnectedError",
    "FeedError",
    "GatewayAuthError",
    "GatewayError",
    "InvalidStateTransitionError",
    "KillSwitchEngaged",
    "OrderError",
    "OrderNotFoundError",
    "PositionError",
    "RateLimitedError",
    "ReconciliationMismatch",
    "RiskError",
    "RiskRejection",
    "SequenceGapError",
    "StaleFeedError",
    "TopicNotFoundError",
    "TradingError",
]
