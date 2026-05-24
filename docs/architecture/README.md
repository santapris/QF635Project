# System Architecture

> **Version**: 1.0.0 | **Status**: Living Document | **Language**: Python 3.12+ | **Paradigm**: Event-Driven, Event-Sourced

## Documents

### General
| File | Contents |
|------|----------|
| [overview.md](overview.md) | High-level architecture, event flow diagrams, concurrency model |
| [data-contracts.md](data-contracts.md) | Canonical event schemas (Pydantic models) and topic map |
| [technology-stack.md](technology-stack.md) | Language, messaging, database, serialization, and tooling choices |
| [project-structure.md](project-structure.md) | Directory layout, package rules, naming conventions |
| [roadmap.md](roadmap.md) | Phased implementation plan (Phase 1–7) |
| [engineering-practices.md](engineering-practices.md) | Typing, linting, testing, config, secrets, dependency management |

### Components
| File | Contents |
|------|----------|
| [components.md](components.md) | Component index and stateful/stateless table |
| [components/feed-handler.md](components/feed-handler.md) | Feed Handler — market data ingestion and normalization |
| [components/event-bus.md](components/event-bus.md) | Event Bus — pub/sub backbone, topic map, backpressure |
| [components/strategy.md](components/strategy.md) | Strategy Engine — signal generation, indicator library |
| [components/risk.md](components/risk.md) | Risk Engine — pre-trade rules, kill switch, audit logging |
| [components/oms.md](components/oms.md) | OMS — order lifecycle, execution algos |
| [components/order-gateways.md](components/order-gateways.md) | Order Gateways — exchange adapters, rate limiting |
| [components/position.md](components/position.md) | Position & PnL Engine — inventory, cost basis, mark-to-market |
| [components/backtest.md](components/backtest.md) | Backtest Engine — replay, simulated exchange, slippage models |
| [components/plugins.md](components/plugins.md) | Plugins — registries that keep config/ generic and venue/strategy/rule code self-contained |

### Infrastructure
| File | Contents |
|------|----------|
| [infrastructure/observability.md](infrastructure/observability.md) | Structured logging, Prometheus metrics, Grafana dashboards |
| [infrastructure/storage.md](infrastructure/storage.md) | Persistence layer, storage decision matrix, data retention |
| [infrastructure/deployment.md](infrastructure/deployment.md) | Local dev setup, Docker Compose, CI/CD, Kubernetes |

## Core Principles

- **Event sourcing**: every state change is an immutable, append-only event; current state = fold over event stream
- **Deterministic replay**: identical inputs always produce identical outputs; same code paths in backtest and live
- **Loose coupling**: components communicate exclusively through the event bus — no direct cross-component calls
- **Separation of concerns**: Strategy decides *what* to trade; Risk decides *whether*; Execution decides *how*
- **Exchange normalization**: all raw venue data is translated to a canonical schema before entering the bus

## Quick Start

See [infrastructure/deployment.md](infrastructure/deployment.md) for local setup instructions and [roadmap.md](roadmap.md) for the phased implementation plan.
