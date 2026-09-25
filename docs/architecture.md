# Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Clients                                     │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │ HTTP/REST
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          Order Service (8081)                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐   │
│  │  HTTP    │→ │ Service  │→ │   Repo   │→ │    PostgreSQL        │   │
│  │ Handler  │  │  Logic   │  │  Layer   │  │  orders + outbox     │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────────────┘   │
│                                      │                                  │
│                                      ▼                                  │
│                              ┌──────────────┐                          │
│                              │    Outbox    │                          │
│                              │   Publisher  │                          │
│                              └──────┬───────┘                          │
└─────────────────────────────────────┼───────────────────────────────────┘
                                      │
                                      │ orders.v1
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Apache Kafka (KRaft)                             │
│                                                                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────────┐   │
│  │ orders.v1  │  │payments.v1 │  │ orders.    │  │ payments.      │   │
│  │ 3 parts    │  │ 3 parts    │  │ retry.v1   │  │ dlt.v1         │   │
│  └─────┬──────┘  └─────┬──────┘  └────────────┘  └────────────────┘   │
│        │               │                                                │
└────────┼───────────────┼────────────────────────────────────────────────┘
         │               │
    ┌────┴────┐    ┌─────┴─────┐
    │         │    │           │
    ▼         │    ▼           │
┌────────┐   │  ┌──────────┐  │
│Payment │   │  │Notification│ │
│Service │   │  │ Service  │  │
│ (8082) │   │  │  (8083)  │  │
└────────┘   │  └──────────┘  │
    │        │       │         │
    │        │       │         │
    ▼        │       ▼         │
┌─────────────────────────────────────────────────────────────────────────┐
│                     PostgreSQL (per service)                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                 │
│  │ payments_db  │  │ notifications│  │  orders_db   │                 │
│  │              │  │     _db      │  │              │                 │
│  └──────────────┘  └──────────────┘  └──────────────┘                 │
└─────────────────────────────────────────────────────────────────────────┘
```

## Service Communication

### Synchronous (REST)
- Client → Order Service: Create order, get order

### Asynchronous (Kafka)
- Order Service → Payment Service: `order.created`
- Order Service → Notification Service: `order.created`
- Payment Service → Notification Service: `payment.completed`, `payment.failed`

## Data Ownership

Each service owns its data:

| Service | Database | Tables |
|---------|----------|--------|
| Order Service | orders_db | orders, outbox_events |
| Payment Service | payments_db | payments |
| Notification Service | notifications_db | notifications |

## Event Flow

```
1. POST /orders
   └→ Order Service creates order + outbox event (same transaction)

2. Outbox Publisher polls
   └→ Publishes to orders.v1

3. Payment Service consumes orders.v1
   └→ Processes payment
   └→ Publishes payment.completed/failed to payments.v1

4. Notification Service consumes orders.v1 AND payments.v1
   └→ Sends notifications for each event
```

## Why Separate Databases?

- **Independence**: Each service can evolve its schema independently
- **Scaling**: Each database can be scaled independently
- **Fault isolation**: One database failure doesn't affect others
- **Clear ownership**: No shared schema confusion

## Why the Outbox Pattern?

Without outbox:
```
INSERT order → SUCCESS
Publish event → FAILURE
Result: Order exists but no event → Inconsistent!
```

With outbox:
```
BEGIN TRANSACTION
  INSERT order
  INSERT outbox_event
COMMIT
→ Both succeed or both fail → Consistent!

Outbox Publisher (async):
  Read outbox → Publish → Mark processed
```
