"""Binance REST client.

Wraps ``aiohttp`` to provide:

- HMAC-signed requests for ``SIGNED`` endpoints.
- Automatic ``X-MBX-APIKEY`` header for ``USER_DATA`` endpoints.
- Server-time-aware timestamp generation (corrects for our wall-clock
  drift relative to Binance).
- Token-bucket rate limiting (uses our existing :class:`RateLimiter`).
- Centralised error translation.

The client is intentionally minimal — it does not know about specific
endpoints (orders, account, etc.); higher layers compose request params
and call :meth:`request`. This keeps endpoint definitions in one place
upstairs and makes the client trivially testable.

Three concerns this code is explicit about:

1. **Clock drift.** On startup we call ``GET /api/v3/time`` once and
   record the offset; every signed request adjusts ``timestamp`` by
   that offset. Without this, a slightly-behind server clock causes
   every signed call to fail with code -1021.

2. **Request weights.** Binance assigns each endpoint a "weight" (e.g.
   POST /order = 1, GET /openOrders = 6). The rate limiter takes the
   weight as cost.

3. **Retry-After.** On 418/429 the response carries a ``Retry-After``
   header (in seconds). We extract it so callers can back off correctly.
"""

from __future__ import annotations

import asyncio
import structlog
import time
from typing import Any, Mapping
from urllib.parse import urlencode

try:
    import aiohttp
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore[assignment]

from ...core.clock import Clock
from ...core.exceptions import OrderError, OrderGatewayError
from ..rate_limiter import RateLimiter
from .config import BinanceConfig, BinanceCredentials
from .errors import BinanceErrorResponse, translate_error
from .signing import encode_query, sign

_INVALID_TIMESTAMP_CODE = -1021

_log = structlog.get_logger(__name__)


class BinanceRESTClient:
    """Async REST client for Binance Spot."""

    def __init__(
        self,
        *,
        config: BinanceConfig,
        credentials: BinanceCredentials | None,
        clock: Clock,
        rate_limiter: RateLimiter | None = None,
    ) -> None:
        if aiohttp is None:
            raise ImportError(
                "aiohttp is required for the Binance adapter. "
                "Install with: pip install 'aiohttp>=3.9'"
            )
        self._config = config
        self._creds = credentials
        self._clock = clock
        # Binance Spot allows roughly 1200 weight per minute. Default to that.
        self._rate_limiter = rate_limiter or RateLimiter(
            capacity=1200.0,
            refill_per_second=1200.0 / 60.0,
            clock=clock,
        )
        self._session: aiohttp.ClientSession | None = None
        # Offset added to wall-clock to match Binance server time.
        # Positive if Binance is ahead of us; updated on connect.
        self._server_time_offset_ms: int = 0
        self._connected = False
        self._resync_task: asyncio.Task[None] | None = None
        # Serialises concurrent resyncs triggered by -1021 retries so a
        # burst of in-flight signed requests doesn't cause N redundant
        # /time calls.
        self._resync_lock = asyncio.Lock()

    # --- Lifecycle --------------------------------------------------------

    async def connect(self) -> None:
        """Open the HTTP session and sync clock with Binance.

        Idempotent. Call before any request.
        """
        if self._connected:
            return
        timeout = aiohttp.ClientTimeout(total=self._config.request_timeout_seconds)
        self._session = aiohttp.ClientSession(timeout=timeout)
        try:
            await self._sync_server_time()
        except Exception:
            await self._session.close()
            self._session = None
            raise
        self._connected = True
        if self._config.clock_resync_interval_seconds > 0:
            self._resync_task = asyncio.create_task(
                self._resync_loop(), name="binance-rest-clock-resync",
            )

    async def close(self) -> None:
        if self._resync_task is not None:
            self._resync_task.cancel()
            try:
                await self._resync_task
            except (asyncio.CancelledError, Exception):
                pass
            self._resync_task = None
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._connected = False

    async def __aenter__(self) -> "BinanceRESTClient":
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.close()

    # --- Time sync --------------------------------------------------------

    async def _sync_server_time(self, *, initial: bool = True) -> None:
        """Set ``_server_time_offset_ms`` based on Binance's serverTime.

        On the initial sync, raises if our wall-clock disagrees by more
        than ``max_clock_drift_ms`` — that is almost always a misconfigured
        NTP and we should fail loud. On periodic resync we log and
        continue: drift is exactly the condition this method exists to
        correct, and a long-running runner is more useful than one that
        crashes the first time the host's clock wobbles.
        """
        before_ms = int(time.time() * 1000)
        data = await self._raw_get(self._config.api_prefix + "/time")
        after_ms = int(time.time() * 1000)
        local_mid = (before_ms + after_ms) // 2
        server_ms = int(data["serverTime"])
        offset = server_ms - local_mid
        if initial and abs(offset) > self._config.max_clock_drift_ms:
            raise OrderGatewayError(
                "Binance server time disagrees with local clock by "
                f"{offset}ms (threshold {self._config.max_clock_drift_ms}ms). "
                "Check NTP synchronization.",
                offset_ms=offset,
            )
        previous = self._server_time_offset_ms
        self._server_time_offset_ms = offset
        if initial:
            _log.info("binance_clock_offset", offset_ms=offset)
        else:
            _log.info(
                "binance_clock_resynced",
                offset_ms=offset,
                previous_offset_ms=previous,
                delta_ms=offset - previous,
            )

    async def _resync_loop(self) -> None:
        interval = self._config.clock_resync_interval_seconds
        while True:
            try:
                await asyncio.sleep(interval)
                async with self._resync_lock:
                    await self._sync_server_time(initial=False)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log.warning("binance_clock_resync_failed", error=str(exc))

    def _now_ms_for_binance(self) -> int:
        """Wall-clock in ms, adjusted to match Binance server time."""
        return int(time.time() * 1000) + self._server_time_offset_ms

    # --- Public request API -----------------------------------------------

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        signed: bool = False,
        user_data: bool = False,
        weight: float = 1.0,
    ) -> Any:
        """Perform a REST call. Returns the parsed JSON body.

        :param signed: include timestamp + signature (for SIGNED endpoints).
        :param user_data: include X-MBX-APIKEY header (for USER_DATA endpoints).
            Signed implies user_data.
        :param weight: rate-limit weight from the Binance docs.

        Raises one of:
        - :class:`RateLimitedError` on 418/429
        - :class:`OrderGatewayAuthError` on auth failures
        - :class:`OrderError` on order-related rejections
        - :class:`OrderGatewayError` for everything else
        """
        if not self._connected:
            raise OrderGatewayError("REST client not connected; call connect() first")

        # Reserve rate-limit tokens before sending. Blocks if needed.
        await self._rate_limiter.acquire(cost=weight)

        url = self._config.rest_base_url + path

        def _build() -> tuple[str, dict[str, str]]:
            all_params: dict[str, Any] = dict(params or {})
            headers: dict[str, str] = {}
            if signed:
                if self._creds is None:
                    raise OrderGatewayError("signed request requires credentials")
                all_params["timestamp"] = self._now_ms_for_binance()
                all_params["recvWindow"] = self._config.recv_window_ms
                signature = sign(all_params, self._creds.api_secret)
                all_params["signature"] = signature
                headers["X-MBX-APIKEY"] = self._creds.api_key
            elif user_data:
                if self._creds is None:
                    raise OrderGatewayError("user_data request requires credentials")
                headers["X-MBX-APIKEY"] = self._creds.api_key
            return (encode_query(all_params) if all_params else ""), headers

        query, headers = _build()
        try:
            return await self._send(method, url, query=query, headers=headers)
        except OrderError as exc:
            # -1021: timestamp/recvWindow rejection. Resync (once, under a
            # lock so concurrent retries share one /time call), rebuild the
            # signed query with the new offset, and retry exactly once.
            if not signed or exc.context.get("code") != _INVALID_TIMESTAMP_CODE:
                raise
            _log.warning("binance_invalid_timestamp_resyncing", error=str(exc))
            async with self._resync_lock:
                await self._sync_server_time(initial=False)
            query, headers = _build()
            return await self._send(method, url, query=query, headers=headers)

    async def _raw_get(self, path: str) -> Any:
        """Unsigned GET used internally (e.g. for time sync)."""
        return await self._send("GET", self._config.rest_base_url + path, query="", headers={})

    # --- Underlying HTTP --------------------------------------------------

    async def _send(
        self,
        method: str,
        url: str,
        *,
        query: str,
        headers: Mapping[str, str],
    ) -> Any:
        assert self._session is not None
        # Binance requires GET/DELETE params in the URL query string, but
        # POST/PUT params in the request body as application/x-www-form-urlencoded.
        # Sending POST params in the URL causes -1116 / -1100 because Binance
        # reads the body for POST endpoints and sees an empty payload.
        if method.upper() in ("POST", "PUT"):
            full_url = url
            body = query or None
            req_headers = {**headers, "Content-Type": "application/x-www-form-urlencoded"}
        else:
            full_url = f"{url}?{query}" if query else url
            body = None
            req_headers = dict(headers)
        _log.debug(
            "binance_http_request",
            method=method, url=full_url, body=body,
        )
        try:
            async with self._session.request(
                method, full_url, headers=req_headers, data=body
            ) as resp:
                # Some endpoints return non-JSON on auth failure (rare); guard.
                try:
                    payload = await resp.json()
                except Exception:
                    text = await resp.text()
                    raise OrderGatewayError(
                        f"binance non-JSON response: {text[:200]}",
                        http_status=resp.status,
                    )
                if resp.status >= 400:
                    retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                    err = BinanceErrorResponse.from_payload(
                        payload if isinstance(payload, dict) else {},
                        http_status=resp.status,
                    )
                    raise translate_error(err, retry_after=retry_after)
                return payload
        except aiohttp.ClientError as exc:
            raise OrderGatewayError(f"binance transport error: {exc}") from exc

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None


__all__ = ["BinanceRESTClient"]
