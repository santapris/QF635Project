"""Dashboard per-strategy controls: list snapshot + pause/resume commands."""

from __future__ import annotations

import pytest

from trading.core import StrategyId
from trading.monitoring.dashboard_server import DashboardServer


class _NullBus:
    async def publish(self, topic, event): pass
    async def subscribe(self, topic, handler): pass
    async def start(self): pass
    async def stop(self): pass


class _FakeRegistry:
    """Minimal registry surface the dashboard uses."""

    def __init__(self, ids: list[str]) -> None:
        self._ids = [StrategyId(i) for i in ids]
        self._paused: set = set()

    @property
    def strategy_ids(self) -> list[StrategyId]:
        return list(self._ids)

    def is_paused(self, sid) -> bool:
        return StrategyId(sid) in self._paused

    def pause(self, sid) -> None:
        sid = StrategyId(sid)
        if sid not in self._ids:
            raise KeyError(sid)
        self._paused.add(sid)

    def resume(self, sid) -> None:
        sid = StrategyId(sid)
        if sid not in self._ids:
            raise KeyError(sid)
        self._paused.discard(sid)


class _FakeOMS:
    def __init__(self) -> None:
        self.cancelled: list = []

    async def cancel_strategy_orders(self, strategy_id, *, why="strategy_paused") -> int:
        self.cancelled.append((strategy_id, why))
        return 0


def _server(registry=None, oms=None) -> DashboardServer:
    return DashboardServer(bus=_NullBus(), strategy_registry=registry, oms_engine=oms)


def test_strategies_payload_unavailable_without_registry() -> None:
    payload = _server()._strategies_payload()
    assert payload["available"] is False
    assert payload["strategies"] == []


def test_strategies_payload_lists_all_with_paused_state() -> None:
    reg = _FakeRegistry(["a", "b"])
    reg.pause("b")
    payload = _server(reg)._strategies_payload()
    assert payload["available"] is True
    by_id = {s["strategy_id"]: s["paused"] for s in payload["strategies"]}
    assert by_id == {"a": False, "b": True}


@pytest.mark.asyncio
async def test_pause_strategy_pauses_and_cancels_orders() -> None:
    reg, oms = _FakeRegistry(["a"]), _FakeOMS()
    payload = await _server(reg, oms)._pause_strategy("a")
    assert reg.is_paused("a")
    assert oms.cancelled == [(StrategyId("a"), "strategy_paused")]
    by_id = {s["strategy_id"]: s["paused"] for s in payload["strategies"]}
    assert by_id["a"] is True


@pytest.mark.asyncio
async def test_pause_without_oms_still_pauses() -> None:
    reg = _FakeRegistry(["a"])
    await _server(reg, None)._pause_strategy("a")
    assert reg.is_paused("a")


@pytest.mark.asyncio
async def test_resume_strategy_clears_pause() -> None:
    reg = _FakeRegistry(["a"])
    reg.pause("a")
    payload = await _server(reg)._resume_strategy("a")
    assert not reg.is_paused("a")
    by_id = {s["strategy_id"]: s["paused"] for s in payload["strategies"]}
    assert by_id["a"] is False


@pytest.mark.asyncio
async def test_pause_unknown_strategy_raises_keyerror() -> None:
    reg = _FakeRegistry(["a"])
    with pytest.raises(KeyError):
        await _server(reg)._pause_strategy("nope")


@pytest.mark.asyncio
async def test_pause_without_registry_raises() -> None:
    with pytest.raises(RuntimeError):
        await _server()._pause_strategy("a")
