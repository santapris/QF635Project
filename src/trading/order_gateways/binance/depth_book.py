"""Binance Spot depth book manager.

Maintaining a *correct* L2 book from Binance's depth stream requires a
specific sequence:

1. Open the WS depth diff stream and buffer events.
2. Fetch a REST snapshot from ``{api_prefix}/depth`` (with sufficient
   ``limit`` — typically 1000).
3. Drop any buffered events with ``u <= lastUpdateId``.
4. The first event you apply MUST satisfy
   ``U <= lastUpdateId + 1 AND u >= lastUpdateId + 1``.
   If no buffered event matches that, the snapshot was too stale —
   start over.
5. From there on, every subsequent event must satisfy
   ``U == previous_u + 1`` (no gaps).

Doing this any other way produces a subtly wrong book.  Strategies that
key off mid or spread won't notice immediately; strategies that walk the
book (size-aware execution, market making with smart skew) will silently
misprice.

This module wraps :class:`~trading.feed_handler.order_book.L2OrderBook`
from the core platform and handles the interleave logic.
"""

from __future__ import annotations

import structlog
from collections import deque
from decimal import Decimal
from typing import Final

from trading.order_gateways.binance.config import BinanceConfig

from ...core.events import OrderBookEvent, TickEvent
from ...core.exceptions import SequenceGapError
from ...core.instruments import Instrument
from ...core.types import Price, Quantity
from ...feed_handler.order_book import L2OrderBook
from .rest_client import BinanceRESTClient
from .symbols import SymbolMapper

_log = structlog.get_logger(__name__)

# Binance accepts limit values: 5, 10, 20, 50, 100, 500, 1000, 5000.
_SNAPSHOT_LIMIT: Final[int] = 1000
_W_DEPTH_SNAPSHOT_1000: Final[float] = 50.0  # weight at limit=1000


class DepthBookManager:
    """Wraps L2OrderBook + interleave logic.

    Construct one per (symbol, instrument). Feed it depth diff events
    via :meth:`apply_diff`; it handles buffering, snapshot fetching,
    and gap detection internally.

    Public surface:
    - :meth:`bootstrap` — fetch the REST snapshot and prime the book.
    - :meth:`apply_diff` — apply one WS diff payload.
    - :meth:`make_tick_event` / :meth:`make_book_event` — delegate to L2OrderBook.
    """

    def __init__(
        self,
        *,
        rest: BinanceRESTClient,
        symbols: SymbolMapper,
        instrument: Instrument,
        config: BinanceConfig | None = None,
    ) -> None:
        self._rest = rest
        self._symbols = symbols
        self._instrument = instrument
        self._config = config = config or getattr(rest, "_config", None) or BinanceConfig(
            spot_rest_base="", spot_ws_base="",
            futures_rest_base="", futures_ws_base="",
        )
        self._book = L2OrderBook(instrument)
        # Buffer used while we're fetching the snapshot.
        self._pre_snapshot_buffer: deque[dict] = deque()
        self._snapshot_pending = False
        self._snapshot_last_update_id: int | None = None
        # Updated to the last successfully-applied diff's `u`.
        self._last_u: int | None = None

    @property
    def book(self) -> L2OrderBook:
        return self._book

    @property
    def is_initialized(self) -> bool:
        return self._book.is_initialized

    # --- Bootstrap --------------------------------------------------------

    async def bootstrap(self) -> None:
        """Buffer WS events, fetch snapshot, apply with interleave rule.

        Caller responsibility:
        1. Call :meth:`apply_diff` for every incoming WS event from
           when the WS connection is up (the manager buffers them).
        2. Call :meth:`bootstrap` once. It will fetch the snapshot and
           replay the buffer with the proper interleave.
        3. After bootstrap returns, subsequent :meth:`apply_diff` calls
           apply directly to the book (no more buffering).

        On gap detection (rule 5 violated), the book is reset and the
        caller must call :meth:`bootstrap` again.
        """
        self._snapshot_pending = True
        wire = self._symbols.wire_symbol(self._instrument)
        snapshot = await self._rest.request(
            "GET", self._config.api_prefix + "/depth",
            params={"symbol": wire, "limit": _SNAPSHOT_LIMIT},
            weight=_W_DEPTH_SNAPSHOT_1000,
        )
        last_update_id = int(snapshot["lastUpdateId"])
        bids = [(Decimal(p), Decimal(q)) for p, q in snapshot["bids"]]
        asks = [(Decimal(p), Decimal(q)) for p, q in snapshot["asks"]]
        self._book.apply_snapshot(
            sequence=last_update_id, bids=bids, asks=asks,
        )
        self._snapshot_last_update_id = last_update_id
        self._last_u = last_update_id

        # Replay the buffered events per the documented rules.
        self._replay_buffer(last_update_id)
        self._snapshot_pending = False

    def _replay_buffer(self, last_update_id: int) -> None:
        """Apply buffered events from the right point, dropping stale ones.

        Per Binance docs:
        - Drop events where ``u <= lastUpdateId`` (stale).
        - The first event to apply must satisfy
          ``U <= lastUpdateId+1 <= u``. If none does, the snapshot is too
          old; we abort and the caller has to retry.
        """
        target = last_update_id + 1
        first_applied = False
        while self._pre_snapshot_buffer:
            evt = self._pre_snapshot_buffer.popleft()
            u = int(evt["u"])
            U = int(evt["U"])
            if u <= last_update_id:
                continue  # stale
            if not first_applied:
                if not (U <= target <= u):
                    # Snapshot was too stale even for the first applicable
                    # event — caller must re-bootstrap.
                    _log.warning(
                        "depth_snapshot_too_stale_aborting_book",
                        last_update_id=last_update_id, first_usable_U=U, first_usable_u=u,
                    )
                    self._book.reset()
                    self._last_u = None
                    self._snapshot_last_update_id = None
                    return
                first_applied = True
            self._apply_event_to_book(evt)

    # --- Apply WS diffs ---------------------------------------------------

    def apply_diff(self, payload: dict) -> None:
        """Apply (or buffer) one WS depth update.

        ``payload`` is the Binance WS frame: ``{"e":"depthUpdate", "U":..., "u":..., "b":[...], "a":[...]}``.

        Behaviour:
        - Before bootstrap: buffer.
        - After bootstrap and not yet initialised: should not happen
          (bootstrap initialises); but guard.
        - After bootstrap: apply, expecting ``U == last_u + 1``. Gap → raise.
        """
        if self._snapshot_pending or not self._book.is_initialized:
            self._pre_snapshot_buffer.append(payload)
            return
        u = int(payload["u"])
        U = int(payload["U"])
        if self._last_u is not None and U != self._last_u + 1:
            # Gap detected. Capture identifying details, reset, then signal
            # caller to re-bootstrap.
            expected = self._last_u + 1
            _log.warning(
                "depth_diff_gap_resetting_book",
                last_u=self._last_u, incoming_U=U, incoming_u=u,
            )
            self._book.reset()
            self._last_u = None
            raise SequenceGapError(
                "binance depth gap",
                instrument=self._instrument.instrument_id,
                expected=expected,
                received=U,
            )
        self._apply_event_to_book(payload)

    def _apply_event_to_book(self, payload: dict) -> None:
        bids = [(Decimal(p), Decimal(q)) for p, q in payload.get("b", [])]
        asks = [(Decimal(p), Decimal(q)) for p, q in payload.get("a", [])]
        u = int(payload["u"])
        # The underlying L2OrderBook just wants monotonic sequence numbers
        # for its own gap detection. Drive it from the book's *own* current
        # sequence + 1, not from Binance's u (which is non-contiguous when
        # we drop stale buffered events).
        current_seq = self._book.sequence or 0
        self._book.apply_delta(
            sequence=current_seq + 1, bids=bids, asks=asks,
        )
        self._last_u = u


__all__ = ["DepthBookManager"]
