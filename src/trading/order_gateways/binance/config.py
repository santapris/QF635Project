"""Binance Spot adapter configuration.

Endpoints come from Binance docs (subject to change — verify against
https://binance-docs.github.io/apidocs/spot/en/ before live use).

URLs are loaded from environment-aware settings (dev → testnet, prod → live).
No URLs are hardcoded here — settings inject the correct endpoints for the
current environment.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True, slots=True)
class BinanceCredentials:
    api_key: str
    api_secret: str


@dataclass(frozen=True, slots=True)
class BinanceConfig:
    """Adapter configuration.

    All endpoint URLs are provided explicitly (typically via settings)
    so that dev/testnet and prod/live are resolved before this object
    is created. There is no ``testnet`` flag — the caller decides which
    URLs to pass.
    """

    spot_rest_base: str
    spot_ws_base: str
    futures_rest_base: str
    futures_ws_base: str

    futures: bool = False
    """If True, use Futures endpoints and paths instead of Spot."""

    recv_window_ms: int = 5_000
    """Per-request validity window. Binance rejects requests where
    server time differs from our timestamp by more than this."""

    request_timeout_seconds: float = 10.0
    """HTTP client timeout per request."""

    max_clock_drift_ms: int = 1_000
    """If our wall-clock disagrees with Binance's serverTime by more than
    this on startup, refuse to proceed (probably an ntp problem)."""

    listen_key_keepalive_seconds: float = 30 * 60
    """User-data WebSocket listen keys expire after 60 minutes; we PUT
    keepalive every 30 minutes to refresh them."""

    reconcile_interval_seconds: float = 60.0
    """How often the balance reconciler polls the account endpoint."""

    @classmethod
    def from_settings(cls, settings, **kwargs) -> "BinanceConfig":
        """Create BinanceConfig from a settings object.

        The settings object is expected to provide the four base URL
        fields (spot_rest_base, spot_ws_base, futures_rest_base,
        futures_ws_base) and an optional ``market`` field that
        determines whether to use futures vs spot.
        """
        _futures = (
            kwargs.pop("futures", None)
            if "futures" in kwargs
            else (getattr(settings, "market", "spot") == "futures")
        )
        return cls(
            spot_rest_base=settings.spot_rest_base,
            spot_ws_base=settings.spot_ws_base,
            futures_rest_base=settings.futures_rest_base,
            futures_ws_base=settings.futures_ws_base,
            futures=_futures,
            **kwargs,
        )

    @property
    def api_prefix(self) -> str:
        return "/fapi/v1" if self.futures else "/api/v3"

    @property
    def account_path(self) -> str:
        return "/fapi/v2/account" if self.futures else "/api/v3/account"

    @property
    def listen_key_path(self) -> str:
        return "/fapi/v1/listenKey" if self.futures else "/api/v3/userDataStream"

    @property
    def rest_base_url(self) -> str:
        return self.futures_rest_base if self.futures else self.spot_rest_base

    @property
    def ws_base_url(self) -> str:
        return self.futures_ws_base if self.futures else self.spot_ws_base


__all__ = [
    "BinanceConfig",
    "BinanceCredentials",
]
