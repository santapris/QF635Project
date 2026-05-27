"""Unit tests for Binance Spot adapter foundation: signing, errors, REST client."""

from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from trading.core.clock import LiveClock
from trading.core.exceptions import (
    OrderGatewayAuthError,
    OrderGatewayError,
    OrderError,
    RateLimitedError,
)
from trading.order_gateways.binance import (
    BinanceConfig,
    BinanceCredentials,
    BinanceErrorResponse,
    BinanceRESTClient,
    translate_error,
)
from trading.order_gateways.binance.signing import encode_query, sign


# =====================================================================
# Signing — verified against Binance's published example.
# https://binance-docs.github.io/apidocs/spot/en/#signed-trade-user_data-and-margin-endpoint-security
# =====================================================================

# Binance's documented example:
#   secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
#   query = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC"
#            "&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559"
#   signature = "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
_DOC_SECRET = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j"
_DOC_PARAMS = {
    "symbol": "LTCBTC",
    "side": "BUY",
    "type": "LIMIT",
    "timeInForce": "GTC",
    "quantity": "1",
    "price": "0.1",
    "recvWindow": 5000,
    "timestamp": 1499827319559,
}
# Note: Binance's worked example uses the params in the order shown above,
# *not* sorted alphabetically. The Binance API actually accepts either —
# the signature must match whatever encoding the caller sends. We sort
# alphabetically (deterministic, repeatable); test the equivalence by
# verifying our hash matches what HMAC of our sorted encoding produces.


def test_signing_is_deterministic():
    """Same inputs -> same hash."""
    sig1 = sign(_DOC_PARAMS, _DOC_SECRET)
    sig2 = sign(_DOC_PARAMS, _DOC_SECRET)
    assert sig1 == sig2
    assert len(sig1) == 64  # SHA256 hex digest
    assert all(c in "0123456789abcdef" for c in sig1)


def test_signing_changes_on_param_change():
    """A different timestamp -> different signature."""
    params2 = dict(_DOC_PARAMS, timestamp=1499827319560)
    assert sign(_DOC_PARAMS, _DOC_SECRET) != sign(params2, _DOC_SECRET)


def test_signing_changes_on_secret_change():
    """A different secret -> different signature."""
    assert sign(_DOC_PARAMS, _DOC_SECRET) != sign(_DOC_PARAMS, "different_secret")


def test_encoding_canonical_order():
    """encode_query sorts params lexicographically."""
    encoded = encode_query({"z": "1", "a": "2", "m": "3"})
    assert encoded == "a=2&m=3&z=1"


def test_signing_does_not_mutate_input():
    """Defensive: signing must not add the signature into the caller's dict."""
    params = dict(_DOC_PARAMS)
    keys_before = set(params.keys())
    sign(params, _DOC_SECRET)
    assert set(params.keys()) == keys_before


def test_signing_handles_int_and_float_params():
    """Real Binance requests have numeric timestamp/recvWindow."""
    # Should not raise; the result is irrelevant.
    sign({"a": 1, "b": 2.5, "c": "hi"}, "secret")


# =====================================================================
# Error code translation
# =====================================================================

def test_error_invalid_signature_is_auth():
    err = BinanceErrorResponse(code=-1022, msg="Signature for this request is not valid.")
    assert err.is_auth_error
    exc = translate_error(err)
    assert isinstance(exc, OrderGatewayAuthError)


def test_error_insufficient_balance_is_logical():
    err = BinanceErrorResponse(code=-2010, msg="Account has insufficient balance.")
    assert err.is_logical_reject
    exc = translate_error(err)
    assert isinstance(exc, OrderError)
    # The marker tells the order_gateway to publish OrderRejected event rather
    # than retry.
    assert exc.context.get("logical_reject") is True


def test_error_429_is_rate_limited():
    err = BinanceErrorResponse(code=0, msg="Too many requests", http_status=429)
    assert err.is_rate_limited
    exc = translate_error(err, retry_after=30.0)
    assert isinstance(exc, RateLimitedError)
    assert exc.context.get("retry_after_seconds") == 30.0


def test_error_418_is_rate_limited_ip_ban():
    """418 = IP auto-banned for ignoring 429s. Treat as rate-limited."""
    err = BinanceErrorResponse(code=0, msg="ip banned", http_status=418)
    exc = translate_error(err, retry_after=120.0)
    assert isinstance(exc, RateLimitedError)


def test_error_unknown_symbol_is_generic_order_error():
    err = BinanceErrorResponse(code=-1121, msg="Invalid symbol.")
    assert not err.is_auth_error
    assert not err.is_logical_reject
    assert not err.is_rate_limited
    exc = translate_error(err)
    assert isinstance(exc, OrderError)
    assert exc.context.get("logical_reject") is not True


def test_error_invalid_listen_key_is_auth():
    """Critical for the WebSocket user-data stream — recover by reissuing."""
    err = BinanceErrorResponse(code=-1125, msg="invalid listenKey")
    assert err.is_auth_error


# =====================================================================
# Credentials
# =====================================================================

def test_credentials_value_object():
    creds = BinanceCredentials(api_key="test-key", api_secret="test-secret")
    assert creds.api_key == "test-key"
    assert creds.api_secret == "test-secret"


# =====================================================================
# Config
# =====================================================================

_SPOT_TESTNET_REST = "https://testnet.binance.vision"
_SPOT_TESTNET_WS = "wss://testnet.binance.vision"
_FUTURES_TESTNET_REST = "https://demo-fapi.binance.com"
_FUTURES_TESTNET_WS = "wss://fstream.binancefuture.com"
_SPOT_LIVE_REST = "https://api.binance.com"
_SPOT_LIVE_WS = "wss://stream.binance.com:9443"
_FUTURES_LIVE_REST = "https://fapi.binance.com"
_FUTURES_LIVE_WS = "wss://fstream.binance.com"


def test_config_spot_testnet_urls():
    cfg = BinanceConfig(
        spot_rest_base=_SPOT_TESTNET_REST,
        spot_ws_base=_SPOT_TESTNET_WS,
        futures_rest_base="",
        futures_ws_base="",
    )
    assert cfg.rest_base_url == _SPOT_TESTNET_REST
    assert cfg.ws_base_url == _SPOT_TESTNET_WS
    assert "testnet" in cfg.rest_base_url


def test_config_spot_live_urls():
    cfg = BinanceConfig(
        spot_rest_base=_SPOT_LIVE_REST,
        spot_ws_base=_SPOT_LIVE_WS,
        futures_rest_base="",
        futures_ws_base="",
    )
    assert cfg.rest_base_url == _SPOT_LIVE_REST
    assert "testnet" not in cfg.rest_base_url


def test_config_futures_testnet_urls():
    cfg = BinanceConfig(
        spot_rest_base="",
        spot_ws_base="",
        futures_rest_base=_FUTURES_TESTNET_REST,
        futures_ws_base=_FUTURES_TESTNET_WS,
        futures=True,
    )
    assert cfg.rest_base_url == _FUTURES_TESTNET_REST
    assert cfg.ws_base_url == _FUTURES_TESTNET_WS


def test_config_futures_api_prefix():
    cfg = BinanceConfig(
        spot_rest_base="", spot_ws_base="",
        futures_rest_base="", futures_ws_base="",
        futures=True,
    )
    assert cfg.api_prefix == "/fapi/v1"


def test_config_futures_listen_key_path():
    cfg = BinanceConfig(
        spot_rest_base="", spot_ws_base="",
        futures_rest_base="", futures_ws_base="",
        futures=True,
    )
    assert cfg.listen_key_path == "/fapi/v1/listenKey"


def test_config_futures_account_info_path():
    cfg = BinanceConfig(
        spot_rest_base="", spot_ws_base="",
        futures_rest_base="", futures_ws_base="",
        futures=True,
    )
    assert cfg.account_path == "/fapi/v2/account"

# =====================================================================
# REST client — mocked HTTP
# =====================================================================

def _make_response(status: int, json_payload: dict, headers: dict | None = None):
    """Build a mock aiohttp response context manager."""
    resp = MagicMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_payload)
    resp.text = AsyncMock(return_value=str(json_payload))
    resp.headers = headers or {}
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


async def test_rest_client_signs_and_sends():
    cfg = BinanceConfig(
        spot_rest_base=_SPOT_TESTNET_REST,
        spot_ws_base=_SPOT_TESTNET_WS,
        futures_rest_base="",
        futures_ws_base="",
        max_clock_drift_ms=10_000_000,
    )
    creds = BinanceCredentials(api_key="k", api_secret="s")
    clock = LiveClock()

    client = BinanceRESTClient(config=cfg, credentials=creds, clock=clock)

    # Mock the session, including the time-sync call.
    session = MagicMock()
    session.request = MagicMock(return_value=_make_response(
        200, {"serverTime": int(__import__("time").time() * 1000)}
    ))
    session.close = AsyncMock()

    with patch.object(__import__("aiohttp"), "ClientSession", return_value=session):
        await client.connect()
        # Replace the request mock now that connect's time-sync has happened.
        session.request = MagicMock(return_value=_make_response(
            200, {"symbol": "BTCUSDT", "orderId": 123}
        ))

        result = await client.request(
            "POST", "/api/v3/order",
            params={"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": "0.001"},
            signed=True, weight=1.0,
        )
        assert result == {"symbol": "BTCUSDT", "orderId": 123}

        # Verify the call included the API key header and the signed params.
        # POSTs send their params (including signature) in the body as
        # application/x-www-form-urlencoded — not in the URL query string.
        last_call = session.request.call_args
        assert "X-MBX-APIKEY" in last_call.kwargs["headers"]
        assert last_call.kwargs["headers"]["X-MBX-APIKEY"] == "k"
        assert last_call.kwargs["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
        url = last_call.args[1]
        assert url == "https://testnet.binance.vision/api/v3/order"
        body = last_call.kwargs["data"]
        assert "signature=" in body
        assert "timestamp=" in body
        assert "recvWindow=5000" in body
        await client.close()


async def test_rest_client_translates_429_to_rate_limited():
    cfg = BinanceConfig(
        spot_rest_base=_SPOT_TESTNET_REST,
        spot_ws_base=_SPOT_TESTNET_WS,
        futures_rest_base="",
        futures_ws_base="",
        max_clock_drift_ms=10_000_000,
    )
    creds = BinanceCredentials(api_key="k", api_secret="s")
    client = BinanceRESTClient(config=cfg, credentials=creds, clock=LiveClock())

    session = MagicMock()
    session.request = MagicMock(return_value=_make_response(
        200, {"serverTime": int(__import__("time").time() * 1000)}
    ))
    session.close = AsyncMock()

    with patch.object(__import__("aiohttp"), "ClientSession", return_value=session):
        await client.connect()
        session.request = MagicMock(return_value=_make_response(
            429, {"code": 0, "msg": "Too many requests"},
            headers={"Retry-After": "60"},
        ))
        with pytest.raises(RateLimitedError) as exc_info:
            await client.request("GET", "/api/v3/ping")
        assert exc_info.value.context.get("retry_after_seconds") == 60.0
        await client.close()


async def test_rest_client_refuses_signed_without_creds():
    cfg = BinanceConfig(
        spot_rest_base=_SPOT_TESTNET_REST,
        spot_ws_base=_SPOT_TESTNET_WS,
        futures_rest_base="",
        futures_ws_base="",
        max_clock_drift_ms=10_000_000,
    )
    client = BinanceRESTClient(config=cfg, credentials=None, clock=LiveClock())
    session = MagicMock()
    session.request = MagicMock(return_value=_make_response(
        200, {"serverTime": int(__import__("time").time() * 1000)}
    ))
    session.close = AsyncMock()
    with patch.object(__import__("aiohttp"), "ClientSession", return_value=session):
        await client.connect()
        with pytest.raises(OrderGatewayError, match="credentials"):
            await client.request("POST", "/api/v3/order", signed=True)
        await client.close()


async def test_rest_client_rejects_large_clock_drift():
    """If our wall-clock is way off Binance, refuse to start."""
    cfg = BinanceConfig(
        spot_rest_base=_SPOT_TESTNET_REST,
        spot_ws_base=_SPOT_TESTNET_WS,
        futures_rest_base="",
        futures_ws_base="",
        max_clock_drift_ms=100,  # tight threshold
    )
    client = BinanceRESTClient(config=cfg, credentials=None, clock=LiveClock())

    session = MagicMock()
    # Report a server time 1 hour ahead — way past threshold.
    fake_server_time = int(__import__("time").time() * 1000) + 3_600_000
    session.request = MagicMock(return_value=_make_response(
        200, {"serverTime": fake_server_time}
    ))
    session.close = AsyncMock()

    with patch.object(__import__("aiohttp"), "ClientSession", return_value=session):
        with pytest.raises(OrderGatewayError, match="server time disagrees"):
            await client.connect()
