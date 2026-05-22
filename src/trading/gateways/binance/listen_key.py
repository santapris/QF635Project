"""Listen-key coordinator for the Binance user data stream.

A listen key is the credential for the private user-data WebSocket. The
lifecycle is rigid:

1. ``POST /api/v3/userDataStream`` to obtain a key.
2. ``PUT /api/v3/userDataStream?listenKey=...`` every <60 minutes to
   refresh; we use 30 min for safety margin.
3. Key expires silently if not refreshed. After expiry the WebSocket
   keeps the TCP connection open but stops delivering messages.

We do not currently use the DELETE endpoint on shutdown — keys auto-
expire and the explicit delete is just politeness.

The manager runs as a background task. The :class:`BinanceUserDataConnector`
gets the current key via :meth:`current_key` and reconnects whenever
:meth:`wait_for_recreation` fires (i.e. the previous key was lost and a
new one was obtained).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Final

from .config import BinanceConfig
from .rest_client import BinanceRESTClient

_log = logging.getLogger(__name__)


_LISTEN_KEY_PATH: Final[str] = "/api/v3/userDataStream"


class ListenKeyManager:
    """Owns the listen-key lifecycle.

    Construct with a connected REST client. Start to begin the keepalive
    loop. Stop to cancel the background task (the key will then expire
    on Binance's side within the hour).
    """

    def __init__(
        self,
        *,
        rest: BinanceRESTClient,
        config: BinanceConfig,
    ) -> None:
        self._rest = rest
        self._config = config
        self._key: str | None = None
        # Set whenever a *new* key is obtained (initial fetch or recreation
        # after expiry). The connector waits on this to know it should
        # reconnect.
        self._recreation_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._stop = False

    @property
    def current_key(self) -> str | None:
        return self._key

    async def start(self) -> None:
        """Obtain the initial key and start the keepalive loop."""
        if self._task is not None:
            return
        self._key = await self._obtain_key()
        self._recreation_event.set()
        self._task = asyncio.create_task(
            self._keepalive_loop(), name="binance-listen-key-keepalive"
        )

    async def stop(self) -> None:
        self._stop = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def wait_for_recreation(self) -> str:
        """Wait until a new key has been issued. Returns the new key.

        After ``start()`` the event is already set with the initial key.
        After a stop/start cycle or expiry-and-recreate, it's re-set with
        the new value. Callers should clear-then-wait if they want to
        ignore the initial set.
        """
        await self._recreation_event.wait()
        assert self._key is not None
        key = self._key
        self._recreation_event.clear()
        return key

    # --- Internals -------------------------------------------------------

    async def _obtain_key(self) -> str:
        """POST a new listen key. Returns the key string."""
        resp = await self._rest.request(
            "POST", _LISTEN_KEY_PATH,
            user_data=True,  # POST is USER_DATA, not SIGNED — just needs the API key header
            weight=1,
        )
        key = str(resp["listenKey"])
        _log.info("binance listen key obtained")
        return key

    async def _keepalive(self, key: str) -> None:
        """PUT refresh. Raises if Binance reports the key as invalid."""
        await self._rest.request(
            "PUT", _LISTEN_KEY_PATH,
            params={"listenKey": key},
            user_data=True,
            weight=1,
        )

    async def _keepalive_loop(self) -> None:
        """Background loop: refresh every ``listen_key_keepalive_seconds``.

        On any failure, log and obtain a fresh key; signal recreation so
        the connector reconnects.
        """
        interval = self._config.listen_key_keepalive_seconds
        while not self._stop:
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                return
            if self._stop:
                return
            assert self._key is not None
            try:
                await self._keepalive(self._key)
                _log.debug("binance listen key kept alive")
            except Exception:
                # Keepalive failed — typically because key has expired
                # already (-1125 from translate_error). Recover by
                # obtaining a fresh key and signalling recreation.
                _log.exception("binance listen key keepalive failed; reissuing")
                try:
                    self._key = await self._obtain_key()
                    self._recreation_event.set()
                except Exception:
                    _log.exception("binance listen key reissue failed; will retry next cycle")


__all__ = ["ListenKeyManager"]
