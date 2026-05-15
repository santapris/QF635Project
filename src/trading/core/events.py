from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class BaseEvent(BaseModel):
    model_config = {"frozen": True}

    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    schema_version: int = 1
    trace_id: str | None = None
    timestamp_exchange: Optional[datetime] = None
    timestamp_received: datetime = Field(default_factory=lambda: datetime.utcnow())


class TickEvent(BaseEvent):
    event_type: str = "tick"
    instrument_id: str
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    exchange: str
    sequence_number: int | None = None


class TradeEvent(BaseEvent):
    event_type: str = "trade"
    instrument_id: str
    price: float
    quantity: float
    side: Literal["buy", "sell"]
    trade_id: str
    exchange: str
    sequence_number: int | None = None


class OrderBookEvent(BaseEvent):
    event_type: str = "order_book"
    instrument_id: str
    exchange: str
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    is_snapshot: bool
    sequence_number: int | None = None


class SignalEvent(BaseEvent):
    event_type: str = "signal"
    strategy_id: str
    instrument_id: str
    side: Literal["buy", "sell", "close"]
    target_quantity: float
    target_price: float | None = None
    confidence: float = 1.0
    rationale: str = ""


class RiskDecision(BaseModel):
    passed: bool
    reason: str = ""

