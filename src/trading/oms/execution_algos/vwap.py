"""Volume-Weighted Average Price.

Slices a parent quantity in proportion to a configured volume profile.
The profile is a list of relative weights, one per equal-duration time
bucket; e.g. ``[1, 2, 3, 2, 1]`` over a 5-minute window means slice 9
fires the largest piece in the middle minute.

The profile is *given*, not learned. A production VWAP pulls historical
intraday volume curves from a data store; the algo here takes any
profile so callers can plug in the right source. A flat profile reduces
VWAP to TWAP.

Implementation:

- Profile is normalised at construction so weights sum to 1.
- Each bucket has a target absolute quantity = weight * parent_quantity.
- The algo fires one child per bucket boundary, sized to the bucket's
  share. Remainder from rounding goes to the final bucket.
- Quantity rounding to lot size happens at the OMS layer.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from ...core.exceptions import ConfigError
from ...core.types import OrderType, Price, Quantity, TimeInForce, Timestamp
from .base import ChildOrderSpec, ExecutionAlgo


_NS_PER_SECOND = 1_000_000_000


class VWAPAlgo(ExecutionAlgo):
    """Slice by a volume profile."""

    __slots__ = (
        "_bucket_qtys", "_bucket_interval_ns", "_start_ns", "_buckets_sent",
        "_order_type", "_time_in_force", "_price", "_cancelled",
    )

    def __init__(
        self,
        *,
        quantity: Quantity,
        duration_seconds: float,
        profile: Sequence[float],
        start_ns: Timestamp,
        order_type: OrderType = OrderType.MARKET,
        time_in_force: TimeInForce = TimeInForce.IOC,
        price: Price | None = None,
    ) -> None:
        if quantity <= 0:
            raise ConfigError("quantity must be positive")
        if duration_seconds <= 0:
            raise ConfigError("duration_seconds must be positive")
        if not profile:
            raise ConfigError("profile must be non-empty")
        if any(w < 0 for w in profile):
            raise ConfigError("profile weights must be non-negative")
        total_weight = sum(profile)
        if total_weight <= 0:
            raise ConfigError("profile weights must sum to a positive number")

        # Compute per-bucket quantities. The final bucket absorbs any
        # rounding remainder so the total exactly matches ``quantity``.
        normalized = [Decimal(str(w)) / Decimal(str(total_weight)) for w in profile]
        bucket_qtys = [Quantity(quantity * w) for w in normalized[:-1]]
        sent_so_far = sum(bucket_qtys, Decimal(0))
        bucket_qtys.append(Quantity(quantity - sent_so_far))
        self._bucket_qtys = bucket_qtys

        self._bucket_interval_ns = int(
            duration_seconds * _NS_PER_SECOND
        ) // len(profile)
        self._start_ns = start_ns
        self._buckets_sent = 0
        self._order_type = order_type
        self._time_in_force = time_in_force
        self._price = price
        self._cancelled = False

    def next_slice(self, now_ns: Timestamp) -> ChildOrderSpec | None:
        if self._cancelled or self._buckets_sent >= len(self._bucket_qtys):
            return None
        target_ns = self._start_ns + self._buckets_sent * self._bucket_interval_ns
        if now_ns < target_ns:
            return None

        qty = self._bucket_qtys[self._buckets_sent]
        self._buckets_sent += 1

        # Skip zero-qty buckets silently (sparse profile). Move to the next
        # bucket by recursion — we won't recurse more than once per call
        # since we only advance one bucket per recursion at the same time.
        if qty == 0:
            return self.next_slice(now_ns)

        return ChildOrderSpec(
            quantity=qty,
            order_type=self._order_type,
            time_in_force=self._time_in_force,
            price=self._price,
        )

    def is_done(self) -> bool:
        return self._cancelled or self._buckets_sent >= len(self._bucket_qtys)

    def cancel(self) -> None:
        self._cancelled = True


__all__ = ["VWAPAlgo"]
