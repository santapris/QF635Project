# Consolidated System Architecture — Event‑Sourced Trading Platform (Live Microstructure MM + Multi‑Strategy)

Version: 1.0 • Status: Living Document • Runtime: Python 3.11+

---

## 1. Overview

A modular, event‑sourced, exchange‑agnostic trading platform optimized for live microstructure strategies (market making, order‑flow alpha) and easily extensible to other strategies and venues. All state changes are driven by immutable events on named topics, enabling deterministic replay, auditability, and component decoupling.

Core principles
- Event sourcing and deterministic replay (same code paths for backtest and live)
- Strict boundaries: Strategy, Risk, OMS, Gateway, Position/PnL, Feed, Backtest
- Topic contracts over function coupling (pub/sub first)
- Venue/strategy agnosticism via adapters and plugin interfaces
- Operability: kill switch, observability, throttling, reconciliation

---

## 2. Topic Map (Canonical)

- market-data: TickEvent, TradeEvent, OrderBookEvent
- signals: SignalEvent (from Strategy)
- risk-decisions: RiskDecision (APPROVED | REJECTED | MODIFIED)
- orders: OrderRequest (from OMS)
- fills: FillEvent, OrderAcknowledged (from Gateways)
- positions: PositionUpdateEvent, PnLSnapshotEvent (from Position Engine)
- alerts: warnings/errors from Risk/OMS/Feed
- system: KillSwitchEvent and admin broadcasts

Partitioning: by instrument_id, or {instrument_id, strategy_id} when multi‑strategy per instrument.

---

## 3. High‑Level Architecture (HLD)

```
[ Exchanges ]── WS/REST/FIX ──▶ Feed Handlers ──▶ Event Bus ──▶ Strategy ──▶ Risk ──▶ OMS ──▶ Gateways ──▶ Exchange
                                              ▲              │           │          │                  │
                                              │              ▼           ▼          ▼                  ▼
                                        Backtest/Replay ◀── Positions/PnL ◀── Fills ◀── Gateways     Observability
```

Components
- Feed Handlers
  - WS/REST connectors per venue; normalize to canonical events; L2/L3 reconstruction; gap detection and snapshot resync; publish to market‑data.
- Strategy Engine
  - Consumes market‑data and positions; computes signals with microstructure analytics (OBI, OFI, microprice, VPIN, vol); emits SignalEvent.
- Risk Engine
  - Pre‑trade rule pipeline (position, notional, drawdown, loss rate, concentration, VPIN circuit breaker). Emits RiskDecision. Owns explicit kill switch.
- OMS (Order Management)
  - Order FSM, timeouts, cancel/replace, idempotency, dual throttles (request weight + order counts), outbox → publishes orders.
- Gateways (Exchange/Broker)
  - Venue protocol adapters (HMAC REST, WS user streams, FIX). Map internal IDs ↔ external, retries with jitter, publish acks/fills.
- Position & PnL
  - Real‑time inventory and PnL; FIFO/LIFO/WAC; periodic reconciliation; publishes positions and PnL snapshots.
- Event Bus
  - AsyncioBus (prototype) and KafkaBus (production). Ordered delivery per partition; backpressure.
- Backtest / Replay
  - Replays canonical events; simulated exchange with book‑walk and latency models; golden‑output regression tests.
- Observability
  - Structured JSON logs; Prometheus metrics; traces with correlation IDs; health + staleness monitors; operator console with RBAC.

---

## 4. Low‑Level Design (LLD)

4.1 Event Contracts (Pydantic v2, immutable)
- BaseEvent: event_id, event_type, schema_version, trace_id, timestamp_exchange, timestamp_received, metadata
- TickEvent, TradeEvent, OrderBookEvent(is_snapshot, sequence_number, bids, asks)
- SignalEvent(strategy_id, instrument_id, side, target_quantity, target_price|None, confidence, rationale)
- RiskDecision(status, approved_quantity|None, reasons[])
- OrderRequest(client_order_id, order_type, time_in_force, limit_price|None, quantity, side, exchange)
- OrderAcknowledged(exchange_order_id, status)
- FillEvent(fill_id, client_order_id, exchange_order_id, side, price, qty, is_maker, commission)
- PositionUpdateEvent(net_qty, avg_cost, mtm), PnLSnapshotEvent

4.2 Bus Interfaces
- AbstractEventBus.publish(topic, event), subscribe(topic, handler), subscribe_many, flush
- AsyncioBus (single‑process), KafkaBus (acks, partitions, idempotent producer)

4.3 Feed Handler Modules
- connector.py: WS lifecycle (depth@100ms, aggTrade, user stream), keepalive, jitter backoff
- normalizer.py: venue JSON → canonical events; symbol normalization
- order_book.py: L2 diff + snapshot sync (buffer → GET snapshot → apply forward; gap → resubscribe)
- sequencer.py: sequence tracking and gap detection
- publisher.py: push to market‑data topic

4.4 Strategy & Analytics
- AbstractStrategy.on_tick/on_fill/on_position_update → [SignalEvent]
- Analytics library (pure): OBI, OFI, microprice, spread; EWMA/Parkinson vol; VPIN volume‑clock; A‑S reservation price, optimal spread, OBI tilt; quote filters (ticks/lots/minNotional/post‑only guards)

4.5 Risk Engine
- engine.py orchestrates rules with short‑circuit on BLOCK/KILL
- rules/: position limit, notional, drawdown, loss‑rate, concentration, VPIN circuit breaker
- kill_switch.py: idempotent trigger; cancels all via OMS; blocks new signals; manual reset only; audit log

4.6 OMS
- state_machine.py: PENDING_NEW → ACKNOWLEDGED → PARTIAL/FILLED/CANCELLED/REJECTED
- engine.py: lifecycle, partials, timeouts, cancel/replace (-2011 filled‑before‑cancel handling)
- rate_limiter.py: weight windows synced to X‑MBX‑USED‑WEIGHT headers
- order_limiter.py: 1s/1m/1d order windows; can_place/record/wait_if_needed
- outbox: durable OrderRequest persisted before publish → relay ensures delivery

4.7 Gateways
- base.py: place/cancel/replace/query/subscribe_fills
- binance/: REST HMAC, user data stream (listenKey keepalive 25m), retries with jitter, mapping, idempotency by client_order_id + exchange_order_id
- others: coinbase/, ib_fix/, simulation/

4.8 Position & PnL
- engine.on_fill → update inventory; mark‑to‑market by mid/microprice; realized/unrealized; spread capture; adverse selection (mp at t+N)
- reconciler.py: periodic venue reconciliation; alert on discrepancies

4.9 Backtest / Replay
- engine.py with injected Clock; `flush()` between events for causality; golden outputs
- simulated_exchange.py: conservative fill model (limit fills only on cross) or book‑walk with impact; latency models

4.10 Persistence & Storage
- PostgreSQL: orders, fills, audit_log, event_outbox (Alembic migrations)
- TimescaleDB: ticks/bars hypertables
- Redis: hot caches (positions snapshot, params)
- S3/MinIO: Parquet archives (market ticks, order book snapshots, backtests)

4.11 Observability & Ops
- structlog JSON logs; Prometheus metrics (latency histograms, counters); OpenTelemetry traces (trace_id propagation)
- Health monitors: feed staleness, gateway error rate; reconnect backoff with jitter; chaos drills
- Operator console: RBAC, enable/disable strategy, manual orders, kill switch, config hot‑reload

---

## 5. Phased Rollout

- Phase 1 (MVP, single process): AsyncioBus; Binance testnet depth/aggTrade; A‑S quoting; position limit + VPIN breaker; OMS+gateway; Parquet logging
- Phase 2 (robust live): user data stream fills; outbox; idempotency; throttles; reconciliation; dashboards
- Phase 3 (scale): KafkaBus; Timescale/Postgres; tracing; shadow trading; multi‑strategy, multi‑venue
- Phase 4 (prod): HA deployment, RBAC console, DR runbooks, chaos testing, canary rollouts

---

## 6. Python Packages (by Capability)

Core runtime
- pydantic>=2.6 (immutable event models)
- httpx>=0.27 (REST client)
- websockets>=12 (WS client)
- anyio>=4 (concurrency primitives used by httpx)
- msgspec or orjson (fast JSON serialization; optional but recommended)
- uvloop (optional perf boost on Linux; not on Windows)

Event bus / messaging
- aiokafka (async Kafka client) OR confluent-kafka (librdkafka; higher throughput; non‑async)

Persistence & storage
- psycopg[binary] (PostgreSQL/Timescale driver)
- SQLAlchemy>=2 and alembic (ORM + migrations) — optional if writing SQL directly
- redis>=5 (hot cache)
- pyarrow (Parquet I/O); pandas (optional for research and backtests)
- boto3 or minio (S3/MinIO integration)

Observability
- structlog or python-json-logger (structured logs)
- prometheus-client (metrics)
- opentelemetry-api, opentelemetry-sdk, opentelemetry-exporter-otlp (tracing)

Strategy analytics & research
- numpy
- scipy (CDF for VPIN bulk classification)
- pandas (not required at runtime; useful for research, backtests, analysis)
- matplotlib or plotly (optional for notebooks/dashboards)

Testing & quality
- pytest, pytest-asyncio
- mypy (types), ruff (lint/format)

Tooling / CLI (optional)
- python-dotenv (env loading in dev)
- typer or click (operator CLI)

Notes
- Choose either aiokafka or confluent-kafka. aiokafka aligns with asyncio; confluent offers higher throughput with thread pools.
- For minimal MVP (single process, testnet): httpx, websockets, pydantic, python-dotenv (optional). The rest can be added per phase.

---

## 7. Configuration & Security

- Environment‑driven settings with safe defaults; secrets from env or secret manager (no hardcoding)
- Config registry (Redis/DB) with signed/audited changes for prod; hot‑reload guarded by feature flags
- NTP and clock drift monitoring; record both exchange and receipt timestamps

---

## 8. Audit, Idempotency, and Safety

- Outbox pattern for OMS → orders publishing; dedup by client_order_id
- Fill dedup by {exchange_order_id, fill_id}; idempotent cancel/replace
- Kill switch: explicit, idempotent, manual reset only; audited
- Partitioning strategy preserves order per instrument (and per strategy if required)

---

## 9. Deliverables & Next Steps

- Phase 1 deliverables: AsyncioBus, Binance feed normalizer, A‑S strategy, minimal Risk, OMS+Binance gateway, Position/PnL, connectivity tests, structured logs, Parquet recorder
- Immediate next: implement AsyncioBus + normalized events for depth/aggTrade; wire one end‑to‑end path Strategy→Risk→OMS→Gateway (paper trading mode), then enable user data fills

---

End of document.
