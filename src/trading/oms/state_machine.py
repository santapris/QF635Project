"""Order state machine.

Centralised legal-transition table. Every status change in
:class:`Order` goes through :func:`validate_transition`. The OMS treats
illegal transitions as bugs in the order_gateway adapter (or in our own
logic) and raises rather than silently accepting them — a desynced
state machine is the worst kind of OMS bug.
"""

from __future__ import annotations

from ..core.exceptions import InvalidStateTransitionError
from ..core.types import OrderStatus

# Allowed transitions. Keys are source states (None = no prior state, used
# only when creating a fresh order). Values are sets of legal destinations.
#
# Edge cases worth noting:
# - PENDING_NEW -> CANCELLED: cancel arrived before the order_gateway acked.
#   Some venues handle this; we accept it.
# - PENDING_CANCEL -> FILLED / PARTIALLY_FILLED: cancel raced against
#   a fill on the venue's side and lost. Real and frequent in production.
# - Terminal states (FILLED, CANCELLED, REJECTED, EXPIRED): no transitions.

_LEGAL_TRANSITIONS: dict[OrderStatus | None, frozenset[OrderStatus]] = {
    None: frozenset({OrderStatus.PENDING_NEW}),
    OrderStatus.PENDING_NEW: frozenset({
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.REJECTED,
        OrderStatus.CANCELLED,
        # Partial/full fills before an explicit ack are vanishingly rare
        # but happen on some venues; allow them rather than fight reality.
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
    }),
    OrderStatus.ACKNOWLEDGED: frozenset({
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.PENDING_CANCEL,
        OrderStatus.CANCELLED,
        OrderStatus.EXPIRED,
    }),
    OrderStatus.PARTIALLY_FILLED: frozenset({
        OrderStatus.PARTIALLY_FILLED,  # subsequent partial
        OrderStatus.FILLED,
        OrderStatus.PENDING_CANCEL,
        OrderStatus.CANCELLED,
        OrderStatus.EXPIRED,
    }),
    OrderStatus.PENDING_CANCEL: frozenset({
        OrderStatus.CANCELLED,
        OrderStatus.PARTIALLY_FILLED,  # fill raced the cancel
        OrderStatus.FILLED,            # fill raced the cancel and won
    }),
    # Terminal states deliberately have no entries; lookup falls through.
}


def validate_transition(
    from_status: OrderStatus | None, to_status: OrderStatus
) -> None:
    """Raise if the transition is illegal. Returns silently otherwise."""
    allowed = _LEGAL_TRANSITIONS.get(from_status)
    if allowed is None:
        raise InvalidStateTransitionError(
            f"no transitions allowed from terminal/unknown state {from_status}",
            from_status=str(from_status),
            to_status=to_status.value,
        )
    if to_status not in allowed:
        raise InvalidStateTransitionError(
            f"illegal transition: {from_status} -> {to_status}",
            from_status=str(from_status),
            to_status=to_status.value,
            legal=sorted(s.value for s in allowed),
        )


def is_terminal(status: OrderStatus) -> bool:
    return status not in _LEGAL_TRANSITIONS


__all__ = ["is_terminal", "validate_transition"]
