from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import AsyncIterator, Optional

import websockets


@dataclass
class BinanceWS:
    public_base: str  # e.g., wss://fstream.binance.com
    user_base: str    # e.g., wss://fstream.binance.com/ws

    async def agg_trade(self, symbol: str) -> AsyncIterator[bytes]:
        """Yield raw frames from aggTrade stream."""
        stream = f"{symbol.lower()}@aggTrade"
        url = f"{self.public_base}/ws/{stream}"
        async for msg in self._connect(url):
            yield msg

    async def depth5(self, symbol: str) -> AsyncIterator[bytes]:
        stream = f"{symbol.lower()}@depth5@100ms"
        url = f"{self.public_base}/ws/{stream}"
        async for msg in self._connect(url):
            yield msg

    async def user_data(self, listen_key: str) -> AsyncIterator[bytes]:
        url = f"{self.user_base}/{listen_key}"
        async for msg in self._connect(url):
            yield msg

    async def _connect(self, url: str) -> AsyncIterator[bytes]:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(url, ping_interval=15, ping_timeout=10) as ws:
                    backoff = 1.0
                    async for message in ws:
                        yield message
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def read_one_public(self, url: str) -> bytes:
        """Connect, read exactly one message, then close cleanly."""
        async with websockets.connect(url, ping_interval=15, ping_timeout=10) as ws:
            msg = await ws.recv()
            return msg

    async def read_one_agg_trade(self, symbol: str) -> bytes:
        stream = f"{symbol.lower()}@aggTrade"
        url = f"{self.public_base}/ws/{stream}"
        return await self.read_one_public(url)

    async def read_one_user(self, listen_key: str) -> bytes:
        url = f"{self.user_base}/{listen_key}"
        async with websockets.connect(url, ping_interval=15, ping_timeout=10) as ws:
            msg = await ws.recv()
            return msg
