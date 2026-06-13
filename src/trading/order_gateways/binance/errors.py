"""Binance error code mapping.

Binance returns errors as ``{"code": -2010, "msg": "..."}`` with negative
codes. The codes are stable across testnet and production. Our OMS
distinguishes a few categories:

- **Retryable transport**: connection errors, timeouts, 5xx — retry with
  backoff.
- **Rate limited**: 429, 418, or specific negative codes — back off
  according to ``Retry-After`` header.
- **Auth/permission**: invalid signature, wrong API key, IP restriction.
- **Bad request**: malformed params, unknown symbol, validation failure.
- **Logical reject**: insufficient balance, would-cross POST_ONLY,
  position limit. The order genuinely cannot be placed; not a bug.

This module is the single place where Binance-specific codes turn into
our exceptions. The OMS sees only our types.
"""

from __future__ import annotations

from dataclasses import dataclass

from ...core.exceptions import (
    OrderGatewayAuthError,
    OrderGatewayError,
    OrderError,
    RateLimitedError,
)


# Selected Binance error codes worth distinguishing.
# Full list: https://binance-docs.github.io/apidocs/spot/en/#error-codes

# Authentication / permission
_E_INVALID_SIGNATURE = -1022
_E_INVALID_API_KEY = -2014
_E_API_KEY_PERMISSION = -2015
_E_INVALID_LISTEN_KEY = -1125

# Bad request
_E_UNKNOWN_SYMBOL = -1121
_E_INVALID_QUANTITY = -1013
_E_BAD_PRECISION = -1111
_E_INVALID_TIMESTAMP = -1021

# Logical reject (order-level)
_E_INSUFFICIENT_BALANCE = -2010
_E_ORDER_WOULD_TRIGGER_IMMEDIATELY = -2021  # STOP_LOSS-type
_E_MIN_NOTIONAL = -1013  # also used here in some cases

# Amend-specific outcomes (handled inline in order_gateway, not via translate_error)
E_AMEND_NOOP = -5027            # already at requested price/qty — order unchanged
E_AMEND_MODIFY_LIMIT = -5026    # order hit its per-order modify cap; no further
                                # amends will ever succeed — cancel and re-place

# Rate limits
_E_TOO_MANY_REQUESTS = -1003  # rare; usually Binance returns 429/418 HTTP


_AUTH_CODES = frozenset({
    _E_INVALID_SIGNATURE, _E_INVALID_API_KEY, _E_API_KEY_PERMISSION,
    _E_INVALID_LISTEN_KEY,
})

_LOGICAL_REJECT_CODES = frozenset({
    _E_INSUFFICIENT_BALANCE, _E_ORDER_WOULD_TRIGGER_IMMEDIATELY,
})

_RATE_LIMIT_CODES = frozenset({_E_TOO_MANY_REQUESTS})


@dataclass(frozen=True, slots=True)
class BinanceErrorResponse:
    """One Binance JSON error payload."""

    code: int
    msg: str
    http_status: int = 0

    @classmethod
    def from_payload(cls, payload: dict, http_status: int = 0) -> "BinanceErrorResponse":
        return cls(
            code=int(payload.get("code", 0)),
            msg=str(payload.get("msg", "")),
            http_status=http_status,
        )

    @property
    def is_rate_limited(self) -> bool:
        return (
            self.http_status in (418, 429)
            or self.code in _RATE_LIMIT_CODES
        )

    @property
    def is_auth_error(self) -> bool:
        return self.code in _AUTH_CODES

    @property
    def is_logical_reject(self) -> bool:
        return self.code in _LOGICAL_REJECT_CODES


def translate_error(err: BinanceErrorResponse, *, retry_after: float | None = None) -> Exception:
    """Map a Binance error to one of our canonical exception types.

    ``retry_after`` comes from the HTTP ``Retry-After`` header on 418/429
    responses; if Binance specifies a back-off, we honour it.
    """
    if err.is_rate_limited:
        return RateLimitedError(
            f"binance rate-limited: {err.msg}",
            code=err.code,
            retry_after_seconds=retry_after,
            http_status=err.http_status,
        )
    if err.is_auth_error:
        return OrderGatewayAuthError(
            f"binance auth failed: {err.msg} (code {err.code})",
            code=err.code,
        )
    if err.is_logical_reject:
        # These are real rejection reasons — insufficient balance, etc.
        # OrderError carries the code; the order_gateway inspects ``is_logical_reject``
        # on the BinanceErrorResponse and publishes an OrderRejected event
        # rather than raising further.
        return OrderError(
            f"binance rejected order: {err.msg}",
            code=err.code,
            venue_error_code=str(err.code),
            logical_reject=True,
        )
    # Bad request, unknown symbol, validation — surface as generic OrderError.
    if err.code != 0:
        return OrderError(
            f"binance error {err.code}: {err.msg}",
            code=err.code,
            http_status=err.http_status,
        )
    # No code: probably a generic HTTP-level failure.
    return OrderGatewayError(
        f"binance HTTP {err.http_status}: {err.msg}",
        http_status=err.http_status,
    )


__all__ = [
    "BinanceErrorResponse",
    "E_AMEND_MODIFY_LIMIT",
    "E_AMEND_NOOP",
    "translate_error",
]
