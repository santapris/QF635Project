# Architecture Overview

## High-Level Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL WORLD                                  │
│    Exchange WebSocket APIs    REST APIs    FIX Gateways    Data Vendors │
└─────────────────────┬───────────────────────────────────┬───────────────┘
                      │ raw market data                   │ order responses
                      ▼                                   ▲
┌─────────────────────────────────────────────────────────────────────────┐
│                      INGESTION / GATEWAY LAYER                          │
│  ┌──────────────────┐  ┌───────────────────┐  ┌──────────────────────┐  │
│  │   Feed Handler   │  │  Exchange Gateway │  │  Broker Gateway      │  │
│  │  (normalisation) │  │  (order routing)  │  │  (FIX/REST adapter)  │  │
│  └────────┬─────────┘  └────────┬──────────┘  └──────────┬───────────┘  │
└───────────┼─────────────────────┼────────────────────────┼──────────────┘
            │NormalizedMarketEvent│ OrderEvent/FillEvent   │
            ▼                     ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         EVENT BUS / BROKER                              │
│              (Kafka in production / asyncio.Queue in prototype)         │
│  Topics: market-data | orders | fills | signals | risk | positions      │
└──┬──────────────────┬──────────────────┬──────────────────┬─────────────┘
   │                  │                  │                  │
   ▼                  ▼                  ▼                  ▼
┌──────────┐   ┌───────────────┐   ┌──────────────┐  ┌──────────────────┐
│ Strategy │   │     Risk      │   │  Execution / │  │   Position/PnL   │
│ / Signal │   │  Management   │   │     OMS      │  │     Engine       │
│  Engine  │   │    Engine     │   │              │  │                  │
└────┬─────┘   └──────┬────────┘   └──────┬───────┘  └──────────────────┘
     │ SignalEvent    │ RiskDecision      │ OrderEvent
     └────────────────┴───────────────────┴──────────────────────────────▶
                                                            back to bus
┌─────────────────────────────────────────────────────────────────────────┐
│                     INFRASTRUCTURE LAYER                                │
│  TimescaleDB │ PostgreSQL │ Redis │ S3/MinIO │ Prometheus │ Grafana     │
└─────────────────────────────────────────────────────────────────────────┘
```

## Event Flow — Live Trading

```
1.  Exchange WebSocket  ──▶  Feed Handler
2.  Feed Handler        ──▶  EVENT BUS (topic: market-data)
3.  Strategy Engine    ◀──   EVENT BUS (subscribe: market-data)
4.  Strategy Engine     ──▶  EVENT BUS (topic: signals)
5.  Risk Engine        ◀──   EVENT BUS (subscribe: signals + positions)
6.  Risk Engine         ──▶  EVENT BUS (topic: risk-decisions)
7.  OMS                ◀──   EVENT BUS (subscribe: risk-decisions)
8.  OMS                 ──▶  Exchange OrderGateway  ──▶  Exchange
9.  Exchange           ──▶  Exchange OrderGateway   ──▶  EVENT BUS (topic: fills)
10. Position Engine    ◀──   EVENT BUS (subscribe: fills)
11. Position Engine     ──▶  EVENT BUS (topic: positions)
12. Risk Engine        ◀──   EVENT BUS (subscribe: positions)  [feedback loop]
```

## Event Flow — Backtesting

```
1.  Historical DB / Parquet Files  ──▶  Replay Engine
2.  Replay Engine                  ──▶  simulated EVENT BUS (same interface)
3.  Strategy Engine (unchanged)   ◀──   simulated EVENT BUS
4.  Risk Engine (unchanged)       ◀──   simulated EVENT BUS
5.  Simulated OMS                  ──▶  Simulated Exchange (slippage/latency model)
6.  Simulated Exchange             ──▶  simulated EVENT BUS (fills)
7.  Position Engine (unchanged)   ◀──   simulated EVENT BUS
```

> **Key invariant**: Strategy, Risk, and Position engines are **identical** in live and backtest modes.
> Only the bus implementation and exchange order gateway differ.

## Concurrency Model

| Phase | Model | Notes |
|-------|-------|-------|
| Phase 1–2 | Single process, `asyncio` event loop, coroutine-per-component | |
| Phase 3+ | Multi-process; each component is a separate process; Kafka as bus | |
| Phase 7+ | Kubernetes pods; each component independently scaled | |
| Phase 7+ | C++ migration for feed handler and OMS hot paths | Publish to same Kafka topics |

## Critical Path Latency Budget

```
Exchange → Feed Handler:          ~1–5ms   (network)
Feed Handler → Event Bus:         <100µs
Event Bus → Strategy:             <50µs
Strategy computation:             <1ms
Strategy → Risk Engine:           <50µs
Risk evaluation:                  <500µs
Risk → OMS:                       <50µs
OMS → Exchange OrderGateway:      <100µs
Exchange OrderGateway → Exchange: ~1–5ms   (network)
──────────────────────────────────────────
Total round-trip (excl. network): ~3ms
Total end-to-end (incl. network): ~10–20ms
```

## System State Recovery Sequence

```
1. Connect to Kafka; seek to last committed offset
2. Load position snapshots from Redis (last 24h)
3. If Redis miss: replay fills from PostgreSQL
4. Load open orders from PostgreSQL
5. Reconcile positions with exchange REST API
6. Resume market data subscription
7. Mark system as READY; begin processing signals
```
