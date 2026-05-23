"""Token-bucket rate limiter.

Real exchanges publish strict rate limits (e.g. Binance: 1200 weight per
minute). A order_gateway that exceeds them gets HTTP 429 responses and, with
repeated violations, IP-banned. The rate limiter is one of those
"unglamorous but mandatory" pieces of infrastructure.

This implementation:

- Token bucket with configurable capacity and refill rate.
- :meth:`acquire` blocks (asyncio) until a token is available.
- :meth:`try_acquire` returns immediately, ``True`` on success, ``False``
  if the bucket is empty.
- Uses the injected :class:`Clock` for time — replays deterministically
  on a :class:`SimulatedClock`.

Note on simulated clocks: the asyncio ``sleep`` we use here is real
wall-clock sleep, not simulated. In a backtest the rate limiter is
typically disabled or run with infinite capacity — there's nothing to
rate-limit against.
"""

from __future__ import annotations

import asyncio

from ..core.clock import Clock

_NS_PER_SECOND = 1_000_000_000


class RateLimiter:
    """Token bucket. ``acquire`` is async; ``try_acquire`` is sync."""

    __slots__ = ("_clock", "_capacity", "_refill_per_sec", "_tokens", "_last_ns", "_lock")

    def __init__(
        self,
        *,
        capacity: float,
        refill_per_second: float,
        clock: Clock,
        initial_tokens: float | None = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if refill_per_second <= 0:
            raise ValueError("refill_per_second must be positive")
        self._clock = clock
        self._capacity = capacity
        self._refill_per_sec = refill_per_second
        self._tokens: float = capacity if initial_tokens is None else min(initial_tokens, capacity)
        self._last_ns: int = clock.monotonic_ns()
        # asyncio.Lock for re-entrant safety; bucket math itself is fast.
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now_ns = self._clock.monotonic_ns()
        elapsed_s = (now_ns - self._last_ns) / _NS_PER_SECOND
        if elapsed_s <= 0:
            return
        self._tokens = min(
            self._capacity, self._tokens + elapsed_s * self._refill_per_sec
        )
        self._last_ns = now_ns

    @property
    def available(self) -> float:
        """Tokens available right now (read-only snapshot)."""
        self._refill()
        return self._tokens

    def try_acquire(self, cost: float = 1.0) -> bool:
        """Take ``cost`` tokens if available. No waiting. Returns success."""
        if cost <= 0:
            raise ValueError("cost must be positive")
        self._refill()
        if self._tokens >= cost:
            self._tokens -= cost
            return True
        return False

    async def acquire(self, cost: float = 1.0) -> None:
        """Take ``cost`` tokens, waiting until they're available."""
        if cost <= 0:
            raise ValueError("cost must be positive")
        if cost > self._capacity:
            raise ValueError("cost exceeds bucket capacity")
        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= cost:
                    self._tokens -= cost
                    return
                deficit = cost - self._tokens
                wait_s = deficit / self._refill_per_sec
                await asyncio.sleep(wait_s)


__all__ = ["RateLimiter"]
