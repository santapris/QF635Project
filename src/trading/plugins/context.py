from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..core.clock import Clock
    from ..core.instruments import Instrument
    from ..event_bus.base import AbstractEventBus
    from ..oms import OMSEngine
    from ..position import PositionEngine


@dataclass(frozen=True, slots=True)
class BuildContext:
    """Shared dependencies passed to every plugin's build()."""

    bus: "AbstractEventBus"
    clock: "Clock"
    instruments: dict[str, "Instrument"]
    oms: "OMSEngine | None" = None
    position: "PositionEngine | None" = None
