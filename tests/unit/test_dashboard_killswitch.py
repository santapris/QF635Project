"""Dashboard kill-switch endpoints: status snapshot + reset command."""

from __future__ import annotations

import pytest

from trading.core import LiveClock
from trading.monitoring.dashboard_server import DashboardServer
from trading.risk.kill_switch import KillSwitch


class _NullBus:
    async def publish(self, topic, event): pass
    async def subscribe(self, topic, handler): pass
    async def start(self): pass
    async def stop(self): pass


class _FakeRiskEngine:
    """Just enough surface for the dashboard: a ``kill_switch`` property."""

    def __init__(self) -> None:
        self._ks = KillSwitch(LiveClock())

    @property
    def kill_switch(self) -> KillSwitch:
        return self._ks


def _server(risk_engine=None) -> DashboardServer:
    return DashboardServer(bus=_NullBus(), risk_engine=risk_engine)


def test_killswitch_payload_unavailable_without_engine() -> None:
    payload = _server()._killswitch_payload()
    assert payload["available"] is False
    assert payload["engaged"] is False


def test_killswitch_payload_reports_armed_state() -> None:
    payload = _server(_FakeRiskEngine())._killswitch_payload()
    assert payload["available"] is True
    assert payload["engaged"] is False


def test_killswitch_payload_reports_engaged_state() -> None:
    engine = _FakeRiskEngine()
    engine.kill_switch.engage(triggered_by="daily_loss_limit", reason="loss > limit")
    payload = _server(engine)._killswitch_payload()
    assert payload["available"] is True
    assert payload["engaged"] is True
    assert payload["triggered_by"] == "daily_loss_limit"
    assert payload["reason"] == "loss > limit"
    assert payload["triggered_at_ns"] > 0


@pytest.mark.asyncio
async def test_reset_killswitch_rearms() -> None:
    engine = _FakeRiskEngine()
    engine.kill_switch.engage(triggered_by="vpin_circuit_breaker", reason="toxic")
    assert engine.kill_switch.engaged

    payload = await _server(engine)._reset_killswitch()

    assert engine.kill_switch.engaged is False
    assert payload["engaged"] is False
    assert payload["available"] is True


@pytest.mark.asyncio
async def test_reset_killswitch_without_engine_raises() -> None:
    with pytest.raises(RuntimeError):
        await _server()._reset_killswitch()


class _RecordingRiskEngine(_FakeRiskEngine):
    """Adds the engine-level engage entry point the dashboard routes through."""

    async def engage_kill_switch(self, *, triggered_by: str, reason: str) -> None:
        self.kill_switch.engage(triggered_by=triggered_by, reason=reason)


@pytest.mark.asyncio
async def test_engage_killswitch_latches() -> None:
    engine = _RecordingRiskEngine()
    payload = await _server(engine)._engage_killswitch(
        triggered_by="operator", reason="manual test"
    )
    assert engine.kill_switch.engaged is True
    assert payload["engaged"] is True
    assert payload["triggered_by"] == "operator"
    assert payload["reason"] == "manual test"


@pytest.mark.asyncio
async def test_engage_killswitch_without_engine_raises() -> None:
    with pytest.raises(RuntimeError):
        await _server()._engage_killswitch(triggered_by="operator", reason="x")
