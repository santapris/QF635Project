"""Binance Spot adapter configuration.

Endpoints come from Binance docs (subject to change — verify against
https://binance-docs.github.io/apidocs/spot/en/ before live use).

API keys are read from environment variables, never from TOML — this is a
hard rule for any code that handles real money.

Testnet defaults to true because the only sane way to run this adapter for
the first time is against testnet. Flipping to live requires an explicit
``live=True`` flag *and* a different pair of env vars, so a typo can't
accidentally point testnet credentials at production.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final


# --- Endpoint constants ---------------------------------------------------

# Spot production endpoints
SPOT_LIVE_REST: Final[str] = "https://api.binance.com"
SPOT_LIVE_WS: Final[str] = "wss://stream.binance.com:9443"
SPOT_LIVE_WS_API: Final[str] = "wss://ws-api.binance.com:443/ws-api/v3"

# Spot testnet endpoints
SPOT_TESTNET_REST: Final[str] = "https://testnet.binance.vision"
SPOT_TESTNET_WS: Final[str] = "wss://testnet.binance.vision"


# FUTURES Endpoints
FUTURES_LIVE_REST: Final[str] = "https://fapi.binance.com"
FUTURES_LIVE_WS: Final[str] = "wss://fstream.binance.com/private"
FUTURES_TESTNET_REST: Final[str] = "https://demo-fapi.binance.com"
FUTURES_TESTNET_WS: Final[str] = "wss://fstream.binancefuture.com"

# Env var names. The pair differs by environment so live credentials cannot
# accidentally be sent to testnet or vice versa.
ENV_LIVE_KEY: Final[str] = "BINANCE_API_KEY"
ENV_LIVE_SECRET: Final[str] = "BINANCE_API_SECRET"
ENV_TESTNET_KEY: Final[str] = "BINANCE_TESTNET_API_KEY"
ENV_TESTNET_SECRET: Final[str] = "BINANCE_TESTNET_API_SECRET"


@dataclass(frozen=True, slots=True)
class BinanceCredentials:
    api_key: str
    api_secret: str

    @classmethod
    def from_env(cls, *, testnet: bool) -> "BinanceCredentials":
        """Read credentials from environment. Raises if missing."""
        key_var = ENV_TESTNET_KEY if testnet else ENV_LIVE_KEY
        secret_var = ENV_TESTNET_SECRET if testnet else ENV_LIVE_SECRET
        key = os.environ.get(key_var)
        secret = os.environ.get(secret_var)
        if not key or not secret:
            raise RuntimeError(
                f"missing Binance credentials: set {key_var} and {secret_var} "
                f"in the environment ({'testnet' if testnet else 'LIVE'})"
            )
        return cls(api_key=key, api_secret=secret)


@dataclass(frozen=True, slots=True)
class BinanceConfig:
    """Adapter configuration."""

    futures: bool = False
    """If True, use Futures endpoints and credentials instead of Spot."""

    testnet: bool = True
    """If True, use Spot testnet URLs and BINANCE_TESTNET_* env vars."""

    # Operational knobs.
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
    """How often the balance reconciler polls /api/v3/account."""

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
        if self.futures:
            return FUTURES_TESTNET_REST if self.testnet else FUTURES_LIVE_REST
        return SPOT_TESTNET_REST if self.testnet else SPOT_LIVE_REST

    @property
    def ws_base_url(self) -> str:
        if self.futures:
            return FUTURES_TESTNET_WS if self.testnet else FUTURES_LIVE_WS
        return SPOT_TESTNET_WS if self.testnet else SPOT_LIVE_WS
    


__all__ = [
    "BinanceConfig",
    "BinanceCredentials",
    "SPOT_LIVE_REST",
    "SPOT_LIVE_WS",
    "SPOT_TESTNET_REST",
    "SPOT_TESTNET_WS",
]
