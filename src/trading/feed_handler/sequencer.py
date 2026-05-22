"""Sequence number tracking with gap detection.

Order books embed sequence numbers in their delta stream so that a
client can detect dropped or reordered messages. ``L2OrderBook`` does
this for its own update channel, but other channels (trade prints,
ticker streams) sometimes ship sequence numbers too. :class:`Sequencer`
is the standalone utility for those cases.

Reset semantics: the engine should reset the sequencer whenever it
issues a fresh snapshot or reconnects. Sequence numbering after such
events typically restarts.
"""

from __future__ import annotations

from ..core.exceptions import SequenceGapError


class Sequencer:
    """Tracks a single monotonically increasing sequence number stream."""

    __slots__ = ("_last", "_strict", "_name")

    def __init__(self, *, strict: bool = True, name: str = "sequencer") -> None:
        """
        :param strict: if True, raise on any gap. If False, only raise on
            backwards movement (gaps are reported via :meth:`gap_size`).
        :param name: shows up in raised errors. Useful when many sequencers
            run side by side.
        """
        self._last: int | None = None
        self._strict = strict
        self._name = name

    @property
    def last(self) -> int | None:
        return self._last

    def reset(self) -> None:
        self._last = None

    def observe(self, seq: int) -> int:
        """Record a new sequence number. Returns the gap size (0 = contiguous).

        Raises :class:`SequenceGapError` if:

        - the new value moves backwards (``seq <= last``); always raises
        - there is a forward gap and ``strict`` is True
        """
        if self._last is None:
            self._last = seq
            return 0
        if seq <= self._last:
            raise SequenceGapError(
                "non-monotonic sequence",
                name=self._name,
                last=self._last,
                received=seq,
            )
        gap = seq - self._last - 1
        if gap > 0 and self._strict:
            raise SequenceGapError(
                "sequence gap detected",
                name=self._name,
                last=self._last,
                received=seq,
                gap=gap,
            )
        self._last = seq
        return gap


__all__ = ["Sequencer"]
