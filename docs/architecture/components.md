# Components

Each core component has its own document:

| Component | Package | Document |
|-----------|---------|----------|
| Feed Handler | `trading.feed_handler` | [components/feed-handler.md](components/feed-handler.md) |
| Event Bus | `trading.event_bus` | [components/event-bus.md](components/event-bus.md) |
| Strategy Engine | `trading.strategy` | [components/strategy.md](components/strategy.md) |
| Risk Engine | `trading.risk` | [components/risk.md](components/risk.md) |
| OMS | `trading.oms` | [components/oms.md](components/oms.md) |
| Order Gateways | `trading.order_gateways` | [components/order-gateways.md](components/order-gateways.md) |
| Position & PnL | `trading.position` | [components/position.md](components/position.md) |
| Backtest Engine | `trading.backtest` | [components/backtest.md](components/backtest.md) |

## Stateful vs Stateless

| Service         | Stateful? | State Location                             |
|-----------------|-----------|--------------------------------------------|
| Feed Handler    | Yes       | In-process order book; rebuilt on restart  |
| Strategy Engine | Yes       | In-process + Redis (for hot reload)        |
| Risk Engine     | Yes       | In-process + Redis                         |
| OMS             | Yes       | In-process + PostgreSQL                    |
| Position Engine | Yes       | In-process + PostgreSQL                    |
| Event Bus       | Yes       | Kafka log                                  |
| Monitoring      | No        | Prometheus pull model                      |

Stateful services must handle restart gracefully by replaying recent events from Kafka or loading their last snapshot from the database.
