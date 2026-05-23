"""Shared pytest fixtures."""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading.core import (
    AssetType,
    Instrument,
    LiveClock,
    SimulatedClock,
    StrategyId,
)
from trading.event_bus import MemoryBus
from trading.logging import configure_logging


@pytest.fixture(scope="session", autouse=True)
def _configure_structlog() -> None:
    configure_logging(level="DEBUG")


@pytest.fixture
def clock() -> LiveClock:
    return LiveClock()


@pytest.fixture
def sim_clock() -> SimulatedClock:
    # Start at a recognisable epoch — 2023-11-14 UTC in nanoseconds.
    return SimulatedClock(start=1_700_000_000_000_000_000)


@pytest.fixture
def memory_bus() -> MemoryBus:
    return MemoryBus()


@pytest.fixture
def btc() -> Instrument:
    return Instrument(
        symbol="BTC-USDT",
        exchange="BINANCE",
        asset_type=AssetType.SPOT,
        base_currency="BTC",
        quote_currency="USDT",
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.0001"),
    )


@pytest.fixture
def eth() -> Instrument:
    return Instrument(
        symbol="ETH-USDT",
        exchange="BINANCE",
        asset_type=AssetType.SPOT,
        base_currency="ETH",
        quote_currency="USDT",
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.001"),
    )


@pytest.fixture
def strategy_id() -> StrategyId:
    return StrategyId("test-strategy")
