"""Order router.

NOTE: currently unused by the OMS. The engine moved to a per-leg
reconciliation model (see :class:`~trading.oms.engine.OMSEngine`),
which submits each :class:`OrderLeg` directly rather than through an
:class:`ExecutionAlgo`. This module is kept as the integration point
for when per-leg execution algos (TWAP/VWAP slicing of a single leg's
quantity, smart-order routing across venues) are reintroduced — at
which point ``route`` should take an :class:`OrderLeg` rather than a
whole :class:`SignalEvent`.

For now: one venue, choice of execution algorithm based on
``SignalEvent.metadata["execution_algo"]``. Defaults to
:class:`ImmediateAlgo`.
"""

from __future__ import annotations

from decimal import Decimal

from ..core.events import SignalEvent
from ..core.exceptions import ConfigError
from ..core.types import Price, Quantity, Timestamp
from .execution_algos import ExecutionAlgo, ImmediateAlgo, TWAPAlgo, VWAPAlgo


class OrderRouter:
    """Maps an approved signal to an execution algorithm."""

    def route(
        self,
        signal: SignalEvent,
        *,
        approved_quantity: Quantity,
        now_ns: Timestamp,
    ) -> ExecutionAlgo:
        """Pick an algo for this signal and approved size."""
        algo_name = signal.metadata.get("execution_algo", "immediate").lower()

        leg = signal.legs[0]

        if algo_name == "immediate":
            return ImmediateAlgo(
                quantity=approved_quantity,
                order_type=leg.order_type,
                time_in_force=leg.time_in_force,
                price=leg.price,
            )

        if algo_name == "twap":
            duration = float(signal.metadata.get("duration_seconds", "60"))
            num_slices = int(signal.metadata.get("num_slices", "10"))
            return TWAPAlgo(
                quantity=approved_quantity,
                duration_seconds=duration,
                num_slices=num_slices,
                start_ns=now_ns,
                order_type=leg.order_type,
                time_in_force=leg.time_in_force,
                price=leg.price,
            )

        if algo_name == "vwap":
            duration = float(signal.metadata.get("duration_seconds", "60"))
            profile_raw = signal.metadata.get("profile", "1,1,1,1,1")
            profile = [float(w) for w in profile_raw.split(",")]
            return VWAPAlgo(
                quantity=approved_quantity,
                duration_seconds=duration,
                profile=profile,
                start_ns=now_ns,
                order_type=leg.order_type,
                time_in_force=leg.time_in_force,
                price=leg.price,
            )

        raise ConfigError(
            f"unknown execution_algo: {algo_name!r}",
            allowed=["immediate", "twap", "vwap"],
        )


__all__ = ["OrderRouter"]
