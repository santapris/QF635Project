# Engineering Practices

## Type System

- **All source code**: strict type hints; no `Any` unless unavoidable
- Use `typing.Protocol` for duck-typed interfaces (not ABCs)
- Use `NewType` for domain primitives: `OrderId = NewType("OrderId", UUID)`
- Use `TypeAlias` for complex types: `Price: TypeAlias = Decimal`

```toml
[tool.mypy]
strict = true
python_version = "3.12"
ignore_missing_imports = false
disallow_untyped_defs = true
disallow_any_explicit = true
warn_return_any = true
```

## Linting & Formatting

```toml
[tool.ruff]
target-version = "py312"
line-length = 100
select = [
    "E", "W",   # pycodestyle
    "F",        # pyflakes
    "I",        # isort
    "B",        # flake8-bugbear
    "UP",       # pyupgrade
    "SIM",      # flake8-simplify
    "TCH",      # flake8-type-checking
    "RUF",      # ruff-specific rules
]
ignore = ["E501"]  # line length handled by formatter

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
```

## Testing Hierarchy

```
tests/
├── unit/           # No I/O, no bus, pure function tests
│                   # Target: 200ms total, 90%+ coverage of core/
│
├── integration/    # Uses MemoryBus; wires real components together
│                   # Uses Docker-provided Postgres/Redis/Kafka
│                   # Target: < 30s total
│
└── system/         # Full backtest runs; golden output comparison
                    # Target: < 5 min total; run on every main branch push
```

**Fixtures philosophy**
- Use `pytest-asyncio` for all async tests
- Factory functions over fixtures for event objects
- Shared `conftest.py` provides: `memory_bus`, `simulated_clock`, `mock_order_gateway`

## Configuration Management

```toml
# config/base.toml — defaults only; no secrets
[trading]
environment = "development"
log_level = "INFO"

[risk]
max_position_notional_usd = 100_000
max_daily_loss_pct = 0.02
drawdown_kill_switch_pct = 0.05

[feed_handler]
reconnect_max_attempts = 10
stale_feed_threshold_seconds = 30
```

```python
# core/config.py
from pydantic_settings import BaseSettings, TomlConfigSettingsSource

class TradingConfig(BaseSettings):
    model_config = SettingsConfigDict(toml_file="config/base.toml")

    environment: str
    log_level: str
    risk: RiskConfig
    feed_handler: FeedHandlerConfig

    @classmethod
    def settings_customise_sources(cls, settings_cls, **kwargs):
        return (
            TomlConfigSettingsSource(settings_cls, "config/base.toml"),
            TomlConfigSettingsSource(settings_cls, f"config/{ENVIRONMENT}.toml"),
            kwargs["env_settings_source"],  # Env vars override all
        )
```

## Secrets Handling

- **Never** commit secrets to git (enforced by `git-secrets` pre-commit hook)
- Local development: `.env` file (gitignored)
- Staging/Production: environment variables injected by CI/CD or Kubernetes Secrets
- Advanced: HashiCorp Vault or AWS Secrets Manager for rotation

```bash
# .env (gitignored)
BINANCE_API_KEY=xxx
BINANCE_API_SECRET=xxx
DATABASE_URL=postgresql+asyncpg://user:password@localhost/trading
REDIS_URL=redis://localhost:6379/0
```

## Dependency Management

- **Tool**: `uv` (fast, lockfile-based, workspace support)
- **Policy**: pin all transitive dependencies in `uv.lock`
- **Security**: `uv audit` in CI to detect vulnerabilities
- **Updates**: Dependabot weekly PRs; human review before merge

## Architecture Decision Records (ADRs)

All significant architectural choices are documented in `docs/adr/`. Template:

```markdown
# ADR-NNN: Title

## Status: Proposed | Accepted | Superseded

## Context
Why this decision needed to be made.

## Decision
What was decided.

## Consequences
- Positive: ...
- Negative: ...
- Migration: ...
```
