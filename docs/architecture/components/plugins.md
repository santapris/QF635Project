# Plugins

**Package**: `trading.plugins`

## Responsibilities

- Decouple the generic application builder from venue/strategy/rule specifics
- Maintain three runtime registries (gateways, strategies, risk rules) that the builder dispatches to by string key
- Provide a uniform `BuildContext` so plugins receive the same dependencies regardless of type
- Validate plugin parameters via per-plugin Pydantic models at build time

**Consumers**: `trading.config.builder` (read-only — looks up by `type` string)
**Producers**: any module that calls `registry.register(name, plugin)` at import time

## Why Plugins

Before plugins, [trading.config.schema](../../../src/trading/config/schema.py) and [trading.config.builder](../../../src/trading/config/builder.py) named every supported venue, strategy, and risk rule by class. Adding a new exchange or strategy meant editing two generic files, and the discriminated unions over `Literal[...]` had to be kept in sync.

After plugins, the generic packages contain zero references to specific venues, strategies, or rules. Adding a new component is a self-contained change in its own package; `config/` is never touched. The dependency arrow is always **plugins → config**, never the reverse.

## Module Structure

```
plugins/
├── __init__.py     # Re-exports registries, protocols, BuildContext; imports builtin
├── builtin.py      # Side-effect imports of every first-party plugin module
├── context.py      # BuildContext dataclass (bus, clock, instruments, oms, position)
├── protocols.py    # GatewayPlugin / StrategyPlugin / RulePlugin protocols
└── registry.py     # Generic _Registry[T] + the three module-level singletons
```

The plugin *implementations* live next to the component they wrap, not inside `plugins/`:

```
order_gateways/
├── simulation_plugin.py            # registers "simulation" and "backtest"
└── binance/
    └── plugin.py                   # registers "binance"

strategy/examples/
└── plugins.py                      # registers "momentum", "mean_reversion", "market_making", "ping_pong"

risk/rules/
└── plugins.py                      # registers all six built-in rules
```

## Registries

Three module-level singletons, each a thin `dict[str, T]` wrapper:

| Registry            | Plugin type      | Build returns                                      |
|---------------------|------------------|----------------------------------------------------|
| `gateway_registry`  | `GatewayPlugin`  | `(AbstractOrderGateway, list[extra_services])`     |
| `strategy_registry` | `StrategyPlugin` | `AbstractStrategy`                                 |
| `rule_registry`     | `RulePlugin`     | `AbstractRiskRule`                                 |

Each registry raises `ConfigError` on unknown lookup with the list of registered names, and on duplicate registration.

## Plugin Protocol

Each plugin is a small class with two members:

```python
class _BinancePlugin:
    Params = BinanceParams   # a Pydantic BaseModel with extra="forbid"

    def build(self, params: BinanceParams, ctx: BuildContext, *, venue: str):
        # construct the venue gateway + supporting services using ctx.bus, ctx.clock, etc.
        return gateway, [service_a, service_b]
```

- **`Params`** is the per-plugin parameter schema. Validation is two-phase: the central `GatewaySpec` keeps `params: dict[str, Any]`, and the plugin validates that dict against `Params` when `build()` is called. Each plugin owns its schema, including which keys are required and whether extras are forbidden.
- **`build()`** receives a `BuildContext` carrying shared dependencies (event bus, clock, instruments map, OMS, position engine). Build kwargs vary slightly by plugin type — `strategy_id` / `instruments` for strategies, `venue` for gateways, none for rules.

The protocols in [protocols.py](../../../src/trading/plugins/protocols.py) are structural — duck typing rather than nominal inheritance, so plugins don't need to subclass anything.

## Registration Lifecycle

1. **Import time**: every plugin module calls `register()` at module scope. This populates the registries.
2. **Bootstrap**: `trading.plugins.builtin` imports every first-party plugin module. The `trading.config` package imports `trading.plugins` at the top of its `__init__.py`, so any consumer that uses `load_config` / `build_live_app` triggers full registration as a side effect.
3. **Build time**: the builder calls `registry.get(spec.type)` to fetch the plugin, validates `spec.params` against `plugin.Params`, and calls `plugin.build(...)`. Unknown types raise `ConfigError`.

## TOML Shape

`GatewaySpec` uses a Pydantic `model_validator(mode="before")` that collects unknown top-level keys into `params`. This lets TOML stay flat instead of forcing a nested `[order_gateways.params]` section:

```toml
[[order_gateways]]
type = "binance"
venue = "BINANCE"
testnet = true                # collected into params.testnet
credentials_env = "BINANCE_TESTNET"   # → params.credentials_env
```

Strategies and rules use explicit nested sections (`[strategies.parameters]`, `[risk.global_rules.params]`) to match the existing convention.

## Adding a New Plugin

1. Implement the component class in its existing package (e.g. `strategy/examples/my_strategy.py`).
2. Add a `Params` model and a plugin class to the package's `plugins.py`:
   ```python
   class MyStrategyParams(BaseModel):
       model_config = ConfigDict(extra="forbid")
       lookback: int = 10

   class _MyStrategyPlugin:
       Params = MyStrategyParams
       def build(self, params, ctx, *, strategy_id, instruments):
           return MyStrategy(strategy_id=strategy_id, instruments=instruments, lookback=params.lookback)

   strategy_registry.register("my_strategy", _MyStrategyPlugin())
   ```
3. If you added a new `plugins.py` module rather than extending an existing one, add a `noqa: F401` import to [trading/plugins/builtin.py](../../../src/trading/plugins/builtin.py) so it loads at startup.

No changes to `config/schema.py`, `config/builder.py`, or any generic file are required.

## Failure Modes

- **Unknown `type`**: `ConfigError("unknown <kind> type 'foo'; registered: bar, baz")` at build time.
- **Invalid params**: `ConfigError("invalid parameters for <kind> ...: <pydantic error>")` at build time, including the Pydantic message identifying the bad field.
- **Duplicate registration**: `ConfigError("<kind> 'foo' already registered")` at import time.

## Related

- [Order Gateways](order-gateways.md) — gateway plugin consumers
- [Strategy](strategy.md) — strategy plugin consumers
- [Risk](risk.md) — risk rule plugin consumers
- [Engineering Practices](../engineering-practices.md) — broader code conventions
