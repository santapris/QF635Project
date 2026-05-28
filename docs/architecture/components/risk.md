# Risk Management Engine

**Package**: `trading.risk`

## Responsibilities

- Evaluate every `SignalEvent` against pre-trade rules — **per leg** — before
  order submission
- Track real-time exposure per instrument and portfolio, including
  **working (unfilled) orders** so limits reflect effective exposure
- Enforce position limits, drawdown limits, concentration limits
- Trigger kill switch on breach of critical thresholds
- Publish `RiskDecision` with per-leg approvals/rejections

**Inputs**: `SignalEvent`, `PositionUpdateEvent`, `FillEvent`,
`OpenOrdersSnapshotEvent`  
**Outputs**: `RiskDecision`, `KillSwitchEvent`, `RiskAlertEvent`

## Per-Leg Evaluation

A `SignalEvent` carries one or more `OrderLeg`s. Each leg runs the full rule
chain independently; the outcome depends on `signal.atomic`:

- `atomic=False` (default): failing legs are dropped, surviving legs are
  approved. The decision carries both `approved_legs` and `rejected_legs`.
- `atomic=True`: any leg rejection rejects the whole signal — no partial
  placement.

A `KILL`-severity rejection engages the kill switch and short-circuits the
remaining legs of that signal.

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

Rules evaluate a single leg against the shared `RiskState`. A rule may approve,
approve-with-a-smaller-clamped-quantity, or reject.

```python
class AbstractRiskRule(ABC):
    @abstractmethod
    def evaluate(
        self,
        signal: SignalEvent,
        leg: OrderLeg,
        state: RiskState,
    ) -> RuleResult: ...
    # RuleResult: approved=bool, severity=INFO|WARN|BLOCK|KILL,
    #             reason=str, approved_quantity=Quantity|None (clamp)
```

## Working-Order-Aware Limits

`RiskState` tracks two things per `(strategy, instrument)`: confirmed position
(from fills / position updates) and **working-order exposure** (from the OMS's
`OpenOrdersSnapshotEvent`, kept side-separated as `working_buy`/`working_sell`).

`MaxPositionRule` computes headroom against *effective* exposure — confirmed
position plus working orders on the same side — not confirmed fills alone.
Without this, several orders each individually approved against stale
fills-only state could collectively breach the cap once they fill (a
double-approve hole). The working view trails signal evaluation by at most one
in-flight signal, since `open-orders` and `signals` are separate topics;
`MaxPositionRule` is a backstop and tolerates that lag.

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
