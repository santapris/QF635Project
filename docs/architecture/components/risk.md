# Risk Management Engine

**Package**: `trading.risk`

## Responsibilities

- Validate every `SignalEvent` against pre-trade risk rules before order submission
- Track real-time exposure per instrument and portfolio
- Enforce position limits, drawdown limits, concentration limits
- Trigger kill switch on breach of critical thresholds
- Publish `RiskDecision` (approved / rejected / modified with reason)

**Inputs**: `SignalEvent`, `PositionUpdateEvent`, `FillEvent`  
**Outputs**: `RiskDecision`, `KillSwitchEvent`, `RiskAlertEvent`

## Module Structure

```
risk/
├── base.py              # AbstractRiskRule interface
├── engine.py            # Orchestrates rule evaluation with short-circuit on BLOCK/KILL
├── state.py             # Real-time exposure tracker
├── kill_switch.py       # Idempotent kill switch; manual reset only
└── rules/
    ├── daily_loss_limit.py
    ├── instrument_allowlist.py
    ├── max_notional.py
    ├── max_order_size.py
    ├── max_position.py
    └── throttle.py
```

## Risk Rule Interface

```python
class AbstractRiskRule(ABC):
    @abstractmethod
    def evaluate(
        self,
        signal: SignalEvent,
        portfolio_state: PortfolioState,
        config: RiskConfig,
    ) -> RiskRuleResult: ...
    # RiskRuleResult: passed=bool, reason=str, severity=INFO|WARN|BLOCK|KILL
```

## Pre-Trade Limits

| Limit Type            | Default Value       | Scope          |
|-----------------------|---------------------|----------------|
| Max order notional    | $100,000            | Per strategy   |
| Max position notional | $1,000,000          | Per instrument |
| Max daily loss        | 2% of AUM           | Per strategy   |
| Max drawdown          | 5% of AUM           | Global         |
| Max open orders       | 50                  | Per strategy   |
| Loss rate             | $10,000 / 5 min     | Global         |
| Concentration         | 20% in single asset | Global         |

## Kill Switch

```python
class KillSwitch:
    def __init__(self, bus: AbstractEventBus, oms: OMS):
        self._triggered = False

    async def trigger(self, reason: str, operator: str = "system"):
        if self._triggered:
            return  # Idempotent
        self._triggered = True
        logger.critical("KILL_SWITCH_TRIGGERED", reason=reason, operator=operator)

        await self.bus.publish("system", KillSwitchEvent(reason=reason))  # block new signals
        await self.oms.cancel_all_orders()
        await self.alerting.send_critical(f"Kill switch: {reason}")
        await self.audit_logger.log_kill_switch(reason, operator)
```

**Trigger conditions**: drawdown > N%, loss rate > X/min, manual command, system error  
**Reset**: requires explicit operator confirmation — never auto-reset in production

## Audit Logging

Every order, fill, risk decision, and system event is written to an append-only audit log containing:
- UTC timestamp
- Component ID
- Event type
- Full event payload (JSON)
- Operator identity (for manual actions)

Storage: PostgreSQL `audit_log` table + S3 archive (never deleted).
