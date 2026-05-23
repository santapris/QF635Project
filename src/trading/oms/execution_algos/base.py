"""Execution algorithm abstraction.

An execution algorithm is a coordinator that decides *when* and *how
much* to send to the order_gateway in service of a single parent order. It
emits a sequence of :class:`ChildOrderSpec`s; the OMS turns each spec
into an :class:`OrderRequest`.

Algos are stateful and live for the duration of one parent order. They
get ticked on every market data update (:meth:`on_tick`) and notified
of every fill on a child order they emitted (:meth:`on_fill`). They
expose :meth:`next_slice` so the OMS can ask "is there anything to send
right now?" — typically called immediately after ``on_tick``.

This design is deliberately polling-based rather than callback-based.
The OMS calls ``next_slice`` whenever it has reason to; the algo
returns one spec or ``None``. Keeps algo internals simple and replay
deterministic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from ...core.events import FillEvent, TickEvent
from ...core.types import OrderType, Price, Quantity, TimeInForce, Timestamp


@dataclass(frozen=True, slots=True)
class ChildOrderSpec:
    """One slice the algo wants to send. The OMS adds the boring bits
    (ids, strategy, instrument) and publishes the resulting OrderRequest."""

    quantity: Quantity
    order_type: OrderType
    time_in_force: TimeInForce
    price: Price | None = None


class ExecutionAlgo(ABC):
    """One instance per parent order. Stateful, single-use."""

    @abstractmethod
    def next_slice(self, now_ns: Timestamp) -> ChildOrderSpec | None:
        """Decide whether to emit a child order now. Returns None to skip."""

    def on_tick(self, tick: TickEvent) -> None:
        """Process a market data update. Default: no-op."""

    def on_fill(self, fill: FillEvent) -> None:
        """Process a fill on a child order. Default: track via the OMS, not us."""

    @abstractmethod
    def is_done(self) -> bool:
        """True when the algo has nothing more to emit (filled, cancelled, or expired)."""

    def cancel(self) -> None:
        """Stop emitting further slices. Default: subclasses override if needed."""


__all__ = ["ChildOrderSpec", "ExecutionAlgo"]
