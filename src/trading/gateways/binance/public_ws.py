"""Binance public WebSocket connector.

Connects to ``wss://stream.binance.com:9443/stream?streams=<list>`` (or
the testnet equivalent) and yields raw JSON frames. The Binance
:class:`~trading.feed_handler.normalizers.BinanceNormalizer` from Batch 3
of the core platform consumes these frames and produces canonical
events.

This is a *public* connector — no authentication, no listen key. The
:class:`BinanceUserDataConnector` (in user_data.py) handles the private
stream separately.

The combined-stream endpoint wraps each frame as
``{"stream": "<name>", "data": {...}}``. Our existing normalizer
already unwraps this shape, so the connector emits the wrapped form
as-is and lets the normalizer handle it.

Connection lifecycle:

- :meth:`connect` opens the WebSocket and waits for the first message.
- :meth:`messages` is an async iterator that yields :class:`RawMessage`
  per frame received. If the connection drops it raises
  :class:`FeedDisconnectedError`; the feed handler engine retries with
  backoff.
- Binance sends periodic ping frames; the ``websockets`` library
  responds with pongs automatically. We additionally watch for
  prolonged silence (handled by the feed handler's stale-feed watchdog).
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Sequence

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
except ImportError:  # pragma: no cover
    websockets = None  # type: ignore[assignment]
    ConnectionClosed = Exception  # type: ignore[misc,assignment]

from ...core.clock import Clock
from ...core.exceptions import FeedDisconnectedError
from ...feed_handler.base import AbstractConnector, RawMessage
from .config import BinanceConfig

_log = logging.getLogger(__name__)


class BinancePublicWSConnector(AbstractConnector):
    """Public market-data WebSocket connector for Binance Spot.

    ``streams`` is the list of Binance stream identifiers
    (e.g. ``"btcusdt@bookTicker"``, ``"btcusdt@aggTrade"``,
    ``"btcusdt@depth"``). Use the helpers in
    :mod:`trading.gateways.binance.stream_names` to construct these.
    """

    def __init__(
        self,
        *,
        config: BinanceConfig,
        streams: Sequence[str],
        clock: Clock,
        source: str = "binance-public",
    ) -> None:
        if websockets is None:
            raise ImportError(
                "websockets is required for the Binance WebSocket connectors. "
                "Install with: pip install 'websockets>=12'"
            )
        if not streams:
            raise ValueError("streams must be non-empty")
        self._config = config
        self._streams = list(streams)
        self._clock = clock
        self._source = source
        self._ws = None
        self._connected = False

    # --- AbstractConnector protocol --------------------------------------

    async def connect(self) -> None:
        if self._connected:
            return
        url = self._build_url()
        _log.info("connecting to binance public WS: %d streams", len(self._streams))
        try:
            # ping_interval keeps the connection healthy; ping_timeout fails fast
            # when the server stops responding.
            self._ws = await websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                max_size=2**20,  # 1 MB; depth snapshots can be sizable
                close_timeout=5,
            )
        except Exception as exc:
            raise FeedDisconnectedError(
                f"failed to connect to binance public WS: {exc}",
                source=self._source,
            ) from exc
        self._connected = True

    async def disconnect(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._connected = False

    async def messages(self) -> AsyncIterator[RawMessage]:
        if not self._connected or self._ws is None:
            raise FeedDisconnectedError("not connected", source=self._source)
        try:
            async for raw in self._ws:
                # Binance sends JSON text frames. Tolerate both str and bytes
                # (the library decodes for us when sent as text, but be
                # defensive).
                if isinstance(raw, bytes):
                    text = raw.decode("utf-8")
                else:
                    text = raw
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    _log.warning("binance WS sent non-JSON: %s", text[:200])
                    continue
                yield RawMessage(
                    payload=payload,
                    ts_ingest=self._clock.now_ns(),
                    source=self._source,
                )
        except ConnectionClosed as exc:
            self._connected = False
            raise FeedDisconnectedError(
                f"binance public WS closed: {exc}",
                source=self._source,
            ) from exc
        except Exception as exc:
            self._connected = False
            raise FeedDisconnectedError(
                f"binance public WS error: {exc}",
                source=self._source,
            ) from exc

    # --- Helpers ---------------------------------------------------------

    def _build_url(self) -> str:
        # Combined streams endpoint accepts a slash-separated list under
        # the /stream?streams= query.
        joined = "/".join(self._streams)
        return f"{self._config.ws_base_url}/stream?streams={joined}"


__all__ = ["BinancePublicWSConnector"]
