from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx


@dataclass
class BinanceREST:
    base: str
    api_key: Optional[str] = None
    api_secret: Optional[str] = None

    def _headers(self) -> Dict[str, str]:
        headers = {"User-Agent": "mqf-microstructure/0.1"}
        if self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key
        return headers

    async def ping(self) -> dict:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self.base}/fapi/v1/ping")
            r.raise_for_status()
            return r.json()

    async def time(self) -> dict:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{self.base}/fapi/v1/time")
            r.raise_for_status()
            return r.json()

    async def exchange_info(self, symbol: str) -> dict:
        params = {"symbol": symbol.upper()}
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(f"{self.base}/fapi/v1/exchangeInfo", params=params)
            r.raise_for_status()
            return r.json()

    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        if not self.api_secret:
            raise RuntimeError("API secret required for signed request")
        query = httpx.QueryParams(params).render()
        sig = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        return {**params, "signature": sig}

    async def user_stream_start(self) -> dict:
        if not self.api_key:
            raise RuntimeError("API key required for user data stream")
        async with httpx.AsyncClient(timeout=10.0, headers=self._headers()) as client:
            r = await client.post(f"{self.base}/fapi/v1/listenKey")
            r.raise_for_status()
            return r.json()

    async def user_stream_keepalive(self, listen_key: str) -> None:
        async with httpx.AsyncClient(timeout=10.0, headers=self._headers()) as client:
            r = await client.put(f"{self.base}/fapi/v1/listenKey", params={"listenKey": listen_key})
            r.raise_for_status()

