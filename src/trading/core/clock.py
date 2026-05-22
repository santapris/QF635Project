"""Clock abstraction.

Determinism rule: **no code in strategy/, risk/, position/, or oms/ may
call ``time.time()`` or ``datetime.now()`` directly.** They must take a
``Clock`` and ask it. This single rule is what makes the same code
produce identical results in live trading and backtests.

Two production implementations:

- :class:`LiveClock` — wraps the system monotonic and wall clocks.
- :class:`SimulatedClock` — driven by the replay engine; advances in
  response to events, never of its own accord.

Both expose nanosecond-precision timestamps because that is what we put
on every event. Conversion helpers to/from ``datetime`` are provided for
display and for the rare API that demands it.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from threading import Lock
from typing import Protocol, runtime_checkable

from .types import Timestamp

_NS_PER_SECOND = 1_000_000_000


@runtime_checkable
class Clock(Protocol):
    """All time access in business logic goes through this interface."""

    def now_ns(self) -> Timestamp:
        """Wall-clock time in nanoseconds since the Unix epoch (UTC)."""
        ...

    def monotonic_ns(self) -> int:
        """Monotonic counter in nanoseconds. For latency measurements only.

        Never compare values from monotonic_ns across processes; the zero
        point is undefined.
        """
        ...

    def now(self) -> datetime:
        """Wall-clock time as a timezone-aware UTC ``datetime``."""
        ...


# --- Helpers ---------------------------------------------------------------


def ns_to_datetime(ns: Timestamp) -> datetime:
    """Convert a nanosecond timestamp into a timezone-aware UTC datetime."""
    return datetime.fromtimestamp(ns / _NS_PER_SECOND, tz=timezone.utc)


def datetime_to_ns(dt: datetime) -> Timestamp:
    """Convert a datetime (naive or aware) into a nanosecond timestamp.

    Naive datetimes are interpreted as UTC. Production code should always
    pass aware datetimes; the naive branch exists for parsing legacy data.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    seconds = dt.timestamp()
    return Timestamp(int(seconds * _NS_PER_SECOND))


# --- LiveClock -------------------------------------------------------------


class LiveClock:
    """Production clock backed by the OS."""

    __slots__ = ()

    def now_ns(self) -> Timestamp:
        return Timestamp(time.time_ns())

    def monotonic_ns(self) -> int:
        return time.monotonic_ns()

    def now(self) -> datetime:
        return datetime.now(tz=timezone.utc)


# --- SimulatedClock --------------------------------------------------------


class SimulatedClock:
    """Clock under explicit control of the replay engine.

    The replay engine calls :meth:`set_time` (or :meth:`advance`) before
    dispatching each event. Strategies, risk rules, etc. then read the
    current time via :meth:`now_ns` and observe the value the engine set.

    Thread-safe because some backtests run multiple consumers of the same
    clock; the lock is uncontended in single-threaded backtests.
    """

    __slots__ = ("_now_ns", "_monotonic_ns", "_lock")

    def __init__(self, start: Timestamp | datetime | None = None) -> None:
        if start is None:
            initial = 0
        elif isinstance(start, datetime):
            initial = datetime_to_ns(start)
        else:
            initial = int(start)
        self._now_ns: int = initial
        self._monotonic_ns: int = 0
        self._lock = Lock()

    # --- Clock protocol ---

    def now_ns(self) -> Timestamp:
        with self._lock:
            return Timestamp(self._now_ns)

    def monotonic_ns(self) -> int:
        with self._lock:
            return self._monotonic_ns

    def now(self) -> datetime:
        return ns_to_datetime(self.now_ns())

    # --- Engine-only API ---

    def set_time(self, ns: Timestamp) -> None:
        """Set the simulated wall-clock time. Must be monotonically non-decreasing.

        Going backwards in simulated time is almost always a bug — typically
        an out-of-order replay. We raise rather than silently allowing it.
        """
        with self._lock:
            if ns < self._now_ns:
                raise ValueError(
                    f"SimulatedClock cannot move backwards: "
                    f"current={self._now_ns}, requested={ns}"
                )
            delta = ns - self._now_ns
            self._now_ns = ns
            self._monotonic_ns += delta

    def advance(self, ns_delta: int) -> None:
        """Advance the simulated clock by ``ns_delta`` nanoseconds."""
        if ns_delta < 0:
            raise ValueError("ns_delta must be non-negative")
        with self._lock:
            self._now_ns += ns_delta
            self._monotonic_ns += ns_delta


__all__ = [
    "Clock",
    "LiveClock",
    "SimulatedClock",
    "datetime_to_ns",
    "ns_to_datetime",
]
