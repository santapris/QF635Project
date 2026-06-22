# Trading Platform

An event-driven algorithmic trading platform. Same code paths for backtest,
paper trading, and (with real order_gateway adapters added) live trading.

## Prerequisites

- Python 3.11 or newer
- Node.js 18+ and npm (only required for the dashboard)
- Linux / macOS / Windows (WSL2 recommended on Windows)
- A Binance testnet API key if you want to run [src/trading/runners/examples/binance_testnet.py](src/trading/runners/examples/binance_testnet.py) — create one at [demo.binance.com/en/my/settings/api-management](https://demo.binance.com/en/my/settings/api-management)

## Quick start

```bash
make install-dev
make test
make backtest     # runs configs/backtest_example.toml
```

```bash
python3.11 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
python -m pip install -U pip wheel
python -m pip install uv
uv pip install -e ".[binance]"
```

To run paper trading against the simulation order_gateway:

```bash
make run-paper    # Ctrl-C to stop
```

To run and validate binance_testnet:
```bash
cp .env.example .env   # then fill in BINANCE_TESTNET_API_KEY and BINANCE_TESTNET_API_SECRET
make run-testnet
```

## Dashboard

A React + Vite dashboard lives in [dashboard/](dashboard/). It visualizes
account state, positions, fills, and strategy activity emitted on the event
bus.

```bash
cd dashboard
npm install
npm run dev       # serves on http://localhost:3000
```

`npm run build` produces a static bundle in `dashboard/dist/`.

## Environment variables

The backend reads configuration from a `.env` file in the project root.
Copy [.env.example](.env.example) and fill in the values you need:

| Variable | Purpose | Default |
| --- | --- | --- |
| `ENVIRONMENT` | Log/metric tag | `dev` |
| `LOG_LEVEL` | Log verbosity (`DEBUG` / `INFO` / `WARNING` / `ERROR`) | `INFO` |
| `DASHBOARD_PORT` | Dashboard WebSocket port (0 disables) | `8765` |
| `BINANCE_TESTNET_API_KEY` / `_API_SECRET` | Read when a venue has `credentials_env = "BINANCE_TESTNET"` | — |
| `BINANCE_LIVE_API_KEY` / `_API_SECRET` | Read when a venue has `credentials_env = "BINANCE_LIVE"` | — |

Exchange URLs and which credentials each venue uses live in the TOML config
(see [configs/](configs/)). Each Binance venue names its credential prefix
via `credentials_env`, so testnet and live keys can coexist in one `.env`.

In deployment, the same variables are injected by the orchestrator (K8s
`Secret`, Vault, AWS Secrets Manager).

## Status

- Core domain model (Decimal money, NewType IDs, frozen Pydantic events)
- Event bus (in-memory, asyncio, Kafka)
- Feed handler with order book reconstruction and reconnect logic
- Strategy framework with momentum / mean-reversion / market-making / ping-pong examples, plus market-making models: Avellaneda-Stoikov, GLFT (Guéant-Lehalle-Fernandez-Tapia, single + grid), order-book-imbalance alpha, grid/ladder, and microprice
- Risk engine with clamping rules, throttle, and latched kill switch
- Position engine with WAVG / FIFO / LIFO accounting
- OMS with state machine, signal/decision join, TWAP/VWAP execution, and cross-strategy internal netting (self-trade prevention)
- Simulation order_gateway (paper trading) and backtest order_gateway (time-jumping)
- Binance Spot & Futures order_gateway with REST/WebSocket, depth book management, and testnet connectivity
- Backtest engine with deterministic simulated clock, Sharpe/Sortino/drawdown metrics
- Plugin registries for gateways, strategies, and risk rules — generic `config/` package, venue/strategy/rule specifics live next to their implementations

Deferred: additional exchange adapters, Kafka end-to-end test infrastructure.

## Layout

```
src/trading/
  core/          types, events, clock, instruments, exceptions, positions
  event_bus/     in-memory, asyncio, Kafka pub-sub
  feed_handler/  market data ingestion + order book + connectors + normalizers
  strategy/      strategy framework, registry, indicator_lib, examples
  risk/          pre-trade risk engine + rules + kill switch
  position/      position tracking + WAVG/FIFO/LIFO accounting + pnl
  oms/           OMS, state machine, execution algos (TWAP/VWAP)
  order_gateways/      simulation + Binance adapter + rate limiter + venue registry
  backtest/      replay engine + scheduling order_gateway + metrics + report
  config/        generic Pydantic schema + TOML loader + builder + settings
  plugins/       gateway / strategy / rule registries + BuildContext
  runners/       run_live + run_backtest (production); examples/ holds the stage1-4 demos
  health.py      component health checks + backpressure gauge
  logging.py     structlog configuration
  monitoring/    dashboard + heartbeat + event logging
  persistence/   (deferred)
tests/
  unit/          per-module tests
  integration/   cross-module end-to-end tests (pipeline + binance wiring)
configs/         example TOML configs + sample data
```

## Architecture

Components communicate through one event bus. Strategies emit signals; the
risk engine approves or clamps them into decisions; the OMS turns approved
decisions into orders; the order_gateway turns orders into venue calls; venue
responses flow back as acks/fills; the position engine aggregates fills
into positions and PnL; the risk engine consumes positions to enforce
limits. No component knows about any other directly — only through events.

Determinism: strategies, risk, OMS, position, and order_gateway logic depend
only on injected `Clock`. In production the clock is wall-clock; in
backtests it is a `SimulatedClock` advanced by the replay engine. The
same code produces identical results in both environments.

See [docs/architecture/](docs/architecture/) for the full design documents.

## Extensibility

Gateways, strategies, and risk rules are plugins. The `config/` package is
generic — it doesn't name any specific exchange, strategy, or rule. The
builder dispatches by string key into three registries (`gateway_registry`,
`strategy_registry`, `rule_registry`) populated at import time by plugin
modules sitting next to each implementation:

```
order_gateways/binance/plugin.py          → registers "binance"
order_gateways/simulation_plugin.py       → registers "simulation", "backtest"
strategy/examples/plugins.py              → registers "momentum", "mean_reversion", "avellaneda_stoikov", "glft", "obi_alpha", "grid", "microprice_mm", ...
risk/rules/plugins.py                     → registers "max_position", "throttle", ...
```

Adding a new exchange or strategy means writing the implementation and one
plugin file. No changes to `config/schema.py` or `config/builder.py`.

### Selecting plugins in TOML

The `type` field on each component selects its plugin; the rest of the
fields are passed to that plugin's parameter model.

```toml
# A simulation gateway — type selects the plugin, other fields are params.
[[order_gateways]]
type = "simulation"
venue = "SIM"
maker_bps = 1.0
taker_bps = 5.0
fill_ms = 10.0

# A Binance gateway — same shape, different plugin, different params.
[[order_gateways]]
type = "binance"
venue = "BINANCE"
testnet = true
credentials_env = "BINANCE_TESTNET"

# Strategy params go in a nested table.
[[strategies]]
strategy_id = "mom-1"
type = "momentum"
instruments = ["BINANCE:BTC-USDT"]
[strategies.parameters]
fast_period = "20"
slow_period = "50"

# Risk rules likewise.
[[risk.global_rules]]
type = "instrument_allowlist"
[risk.global_rules.params]
allowed_instrument_ids = "BINANCE:BTC-USDT"

[[risk.per_strategy."mom-1"]]
type = "max_position"
[risk.per_strategy."mom-1".params]
max_long = "0.5"
max_short = "0.5"
```

Unknown `type` values fail at build time with a list of registered plugins.
Unknown fields fail at build time against the plugin's own pydantic schema.

See [docs/architecture/components/plugins.md](docs/architecture/components/plugins.md)
for the full plugin contract and how to add a new one.

## License

MIT — see [LICENSE](LICENSE).
