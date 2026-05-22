"""In-process simulated connector.

Used by:

- Unit tests that exercise the full ``FeedHandler`` pipeline without a
  real network connection.
- The backtest engine, when it wants to replay historical raw frames
  through the same normalizer code path as live trading. (Most
  backtests skip the connector entirely and inject canonical events
  directly — this is for the cases where you want to test the
  normalizer too.)

The connector is controllable via :meth:`inject` and
:meth:`inject_disconnect`. Tests push frames or a disconnect signal,
then await consumption.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any, Final

from ...core.clock import Clock
from ...core.exceptions import FeedDisconnectedError
from ..base import AbstractConnector, RawMessage

# Sentinel pushed onto the inbox to signal a simulated disconnect. Using
# a module-level singleton instead of None so test code can't accidentally
# inject None and look like a disconnect.
_DISCONNECT: Final[object] = object()


class SimulatedConnector(AbstractConnector):
    """Connector backed by an in-process queue. Test/backtest only."""

    def __init__(self, *, source: str, clock: Clock) -> None:
        self._source = source
        self._clock = clock
        self._inbox: asyncio.Queue[Any] = asyncio.Queue()
        self._connected = False

    async def connect(self) -> None:
        if self._connected:
            return
        # Fresh inbox each connect — a sentinel left over from the
        # previous session would corrupt the new one.
        self._inbox = asyncio.Queue()
        self._connected = True

    async def disconnect(self) -> None:
        if not self._connected:
            return
        self._connected = False
        # Wake any parked iterator so the engine can react.
        self._inbox.put_nowait(_DISCONNECT)

    async def messages(self) -> AsyncIterator[RawMessage]:
        if not self._connected:
            raise FeedDisconnectedError("not connected", source=self._source)
        while self._connected:
            item = await self._inbox.get()
            if item is _DISCONNECT:
                self._connected = False
                raise FeedDisconnectedError(
                    "simulated disconnect", source=self._source
                )
            yield item

    # --- Test injection API ------------------------------------------------

    def inject(self, payload: Any) -> None:
        """Push a raw payload that will be yielded as a RawMessage."""
        msg = RawMessage(
            payload=payload,
            ts_ingest=self._clock.now_ns(),
            source=self._source,
        )
        self._inbox.put_nowait(msg)

    def inject_raw(self, raw: RawMessage) -> None:
        """Push a fully-formed RawMessage (preserves caller's ts_ingest)."""
        self._inbox.put_nowait(raw)

    def inject_disconnect(self) -> None:
        """Cause the next :meth:`messages` iteration to raise."""
        self._inbox.put_nowait(_DISCONNECT)


__all__ = ["SimulatedConnector"]
