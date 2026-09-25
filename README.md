# Kafka Microservices Learning Project

A production-style learning project demonstrating Apache Kafka concepts through a practical **Order / Payment / Notification** microservices system built in Go.

## Overview

This project is a **learning laboratory** for understanding Kafka in real-world microservices development. It demonstrates important Kafka concepts through working code, not just theory.

### What You'll Learn

- Kafka fundamentals (topics, partitions, consumer groups, offsets)
- Event-driven architecture patterns
- The Outbox pattern for reliable event publishing
- Idempotency for duplicate handling
- Retry and Dead Letter Topic (DLT) patterns
- Consumer lag and backpressure
- Rebalancing and scaling
- Delivery semantics (at-least-once, at-most-once, exactly-once)
- Schema evolution
- And much more...

---

## Architecture

```
                    HTTP
                     │
                     ▼
              ┌──────────────┐
              │ Order Service│
              │   (Port 8081)│
              └──────┬───────┘
                     │
                     │ order.created (via Outbox)
                     ▼
                  Kafka
             ┌───────┴────────┐
             │                │
             ▼                ▼
       Payment Group     Notification Group
             │                │
             ▼                ▼
      Payment Service   Notification Service
      (Port 8082)       (Port 8083)
             │
             │ payment.completed / failed
             ▼
           Kafka
             │
             ▼
      Notification Service
```

### Services

| Service | Port | Responsibility |
|---------|------|----------------|
| Order Service | 8081 | REST API for order creation, publishes `order.created` events |
| Payment Service | 8082 | Consumes order events, processes payments, publishes payment results |
| Notification Service | 8083 | Consumes order and payment events, simulates sending notifications |

### Infrastructure

| Component | Port | Purpose |
|-----------|------|---------|
| Kafka Broker | 9092/9093 | Event streaming platform (KRaft mode) |
| Kafka UI | 8080 | Visual tool for inspecting Kafka |
| PostgreSQL (Orders) | 5432 | Order service database |
| PostgreSQL (Payments) | 5433 | Payment service database |
| PostgreSQL (Notifications) | 5434 | Notification service database |
| Redis | 6379 | Optional - for idempotency cache examples |

---

## Quick Start

### Prerequisites

- Go 1.21+
- Docker & Docker Compose
- [Air](https://github.com/air-verse/air) for hot reload (optional)

### 1. Start Infrastructure

```bash
# Start Kafka, PostgreSQL, Redis, and Kafka UI
docker compose up -d

# Wait for services to be healthy
docker compose ps
```

### 2. Verify Topics Created

```bash
make topics
```

You should see:
- `orders.v1`
- `payments.v1`
- `notifications.v1`
- `orders.retry.v1`
- `orders.dlt.v1`
- `payments.retry.v1`
- `payments.dlt.v1`

### 3. Start Services

Open **3 separate terminals**:

```bash
# Terminal 1 - Order Service
make order

# Terminal 2 - Payment Service
make payment

# Terminal 3 - Notification Service
make notification
```

### 4. Create Your First Order

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer-1",
    "items": [
      {"product_id": "product-1", "quantity": 2, "price": 100.00}
    ]
  }'
```

### 5. Observe the Flow

Watch the logs in each terminal:

1. **Order Service**: Order created, outbox event inserted
2. **Outbox Publisher**: Event published to `orders.v1`
3. **Payment Service**: Consumed `order.created`, processed payment, published `payment.completed`
4. **Notification Service**: Consumed both events, sent notifications

### 6. Explore Kafka UI

Open [http://localhost:8080](http://localhost:8080) to see:
- Topics and partitions
- Messages and their contents
- Consumer groups and offsets
- Consumer lag

---

## Kafka Concepts Demonstrated

### Topics and Partitions

Topics are named feeds where messages are published. Each topic is split into **partitions** for parallelism.

```
orders.v1 -> 3 partitions
payments.v1 -> 3 partitions
notifications.v1 -> 3 partitions
```

**Why partitions?**
- Enable parallel processing
- Allow scaling consumers
- Provide ordering within a partition (not globally)

**Message Key**: Orders use `order_id` as the key, ensuring all events for the same order go to the same partition.

### Consumer Groups

A consumer group is a set of consumers that cooperate to consume a topic.

```
orders.v1
   │
   ├── payment-service group
   │      └── Payment Service
   │
   └── notification-service group
          └── Notification Service
```

**Key Points:**
- Each group independently receives ALL messages
- Within a group, each partition is assigned to exactly ONE consumer
- Different groups can process the same message independently

### Offsets

An offset is a position within a partition. Each consumer group maintains its own offset.

```
Partition 0: [msg0:offset=0] [msg1:offset=1] [msg2:offset=2]
                                      ↑
                              payment-service offset=1
                              notification-service offset=2
```

**Important:**
- Offsets are per-partition, not global
- Each consumer group tracks its own progress
- Committing offset = "I've successfully processed up to here"

### Offset Commit Strategy

This project uses **manual offset commit** (at-least-once delivery):

```
consume → validate → process → success → commit offset
```

**Why not commit before processing?**
If you commit first and then crash, the message is lost.

**Why at-least-once?**
If processing succeeds but commit fails, the message is processed again. This requires **idempotency**.

---

## Hands-On Experiments

### Experiment 1: Basic Flow

1. Create an order via REST API
2. Observe the event flow through all services
3. Check Kafka UI for messages in each topic

**What to observe:**
- `order.created` → `orders.v1`
- `payment.completed` → `payments.v1`
- Notifications sent for both events

### Experiment 2: Multiple Consumer Groups

Both `payment-service` and `notification-service` groups consume from `orders.v1`.

**What to observe:**
- Both services receive the same `order.created` event
- Each group maintains independent offsets
- Check Kafka UI → Consumer Groups to see both groups

### Experiment 3: More Consumers Than Partitions

With 3 partitions and 5 consumers in one group:

```bash
# Start 5 instances of payment service (modify ports)
# Only 3 will be active, 2 will be idle
```

**What to observe:**
- Only 3 consumers get partition assignments
- 2 consumers are idle (no partitions assigned)
- Adding more consumers beyond partition count doesn't help

**Why?** Each partition can only be assigned to one consumer at a time.

### Experiment 4: Rebalancing

1. Start payment service (gets partitions 0, 1, 2)
2. Start another payment service instance
3. Observe partition reassignment in logs

**What to observe:**
```
[consumer=payment-1] assigned partition=0
[consumer=payment-1] assigned partition=1
[consumer=payment-2] assigned partition=2
```

4. Stop one consumer
5. Observe remaining consumer gets all partitions

### Experiment 5: Duplicate Event (Idempotency)

Payment processing is idempotent using `event_id`:

```
payment event received (event_id=abc123)
payment processed
offset not committed (simulated)
same event received again (event_id=abc123)
idempotency check detects duplicate
payment is NOT charged twice
```

**What to observe:**
- Check payment service logs for "duplicate payment event detected"
- Only ONE payment record in database per event

### Experiment 6: Slow Consumer

Enable slow consumer mode:

```bash
# In .env or environment
SLOW_CONSUMER=true
SLOW_CONSUMER_DELAY=5s
```

**What to observe:**
- Consumer lag increases in Kafka UI
- Messages pile up in partitions
- Producer rate > consumer processing rate = lag

### Experiment 7: Retry

Force a payment failure (e.g., amount > 10000):

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer-1",
    "items": [
      {"product_id": "product-1", "quantity": 1, "price": 15000.00}
    ]
  }'
```

**What to observe:**
- Payment fails
- Event sent to `payments.retry.v1`
- Retry consumer picks it up
- After max retries, sent to `payments.dlt.v1`

### Experiment 8: Dead Letter Topic (DLT)

After max retries exceeded:

**What to observe:**
- Message appears in `payments.dlt.v1`
- DLT message contains:
  - Original topic, partition, offset
  - Error message
  - Retry count
  - Original payload

### Experiment 9: Ordering

Send multiple events for the same order:

```bash
# Create order
# Update order
# Cancel order
```

**What to observe:**
- Same `order_id` key → same partition
- Events processed in order within that partition
- Kafka ordering is **per-partition**, not global

### Experiment 10: Outbox Pattern

The Order Service uses the Outbox pattern:

```
PostgreSQL Transaction
    ├── INSERT INTO orders
    └── INSERT INTO outbox_events

Outbox Publisher (polling)
    └── Read unprocessed → Publish to Kafka → Mark processed
```

**What to observe:**
- Business data and event are saved atomically
- If Kafka is down, events remain in outbox
- Publisher retries until Kafka is available

### Experiment 11: Consumer Scaling

Compare throughput:

| Configuration | Expected Behavior |
|---------------|-------------------|
| 3 partitions / 1 consumer | Sequential processing, max lag |
| 3 partitions / 3 consumers | Parallel processing, optimal |
| 3 partitions / 5 consumers | Same as 3/3, 2 idle consumers |

### Experiment 12: Broker Failure (Cluster Mode)

Using the 3-broker cluster profile:

```bash
docker compose -f docker-compose.cluster.yml up -d

# Stop one broker
docker stop kafka-broker-2

# Observe:
# - Leader election
# - ISR changes
# - Service continues working
```

---

## Topic Naming Convention

```
orders.v1         → Main topic, version 1
payments.v1       → Main topic, version 1
orders.retry.v1   → Retry topic for orders
orders.dlt.v1     → Dead Letter Topic for orders
```

**Why version topics?**
- Schema evolution without breaking consumers
- `orders.v2` can coexist with `orders.v1`
- Consumers migrate at their own pace

**Why separate retry/DLT topics?**
- Isolate problematic messages
- Different retention policies
- Easier monitoring and alerting

---

## Event Structure

All events use a common envelope:

```json
{
  "event_id": "uuid",
  "event_type": "order.created",
  "version": 1,
  "occurred_at": "2024-01-15T10:30:00Z",
  "correlation_id": "uuid",
  "causation_id": "uuid",
  "producer": "order-service",
  "data": { ... }
}
```

**Headers** (Kafka message headers):
- `event_type`
- `correlation_id`
- `causation_id`
- `event_id`
- `producer`
- `version`

**Why headers?**
- Metadata doesn't belong in payload
- Can be read without deserializing payload
- Useful for routing and filtering

---

## Schema Evolution

The project demonstrates V1 → V2 evolution:

**OrderCreatedDataV1:**
```go
type OrderCreatedDataV1 struct {
    OrderID     string
    CustomerID  string
    Items       []OrderItem
    TotalAmount float64
    CreatedAt   time.Time
}
```

**OrderCreatedDataV2:**
```go
type OrderCreatedDataV2 struct {
    OrderID       string
    CustomerID    string
    CustomerEmail string    // NEW FIELD
    Items         []OrderItem
    TotalAmount   float64
    Currency      string    // NEW FIELD
    CreatedAt     time.Time
}
```

**Backward Compatibility:**
- V2 consumers can read V1 events (new fields have defaults)
- V1 consumers ignore unknown V2 fields

**Breaking Changes:**
- Removing a field
- Changing a field type
- Renaming a field

---

## Delivery Semantics

### At-Most-Once
```
commit offset → process
```
- Fast
- Messages can be lost

### At-Least-Once (This Project)
```
process → commit offset
```
- No message loss
- Duplicates possible (requires idempotency)

### Exactly-Once
- Requires Kafka transactions + idempotent producer
- Complex to implement correctly
- **Note:** Kafka transactions only make Kafka writes atomic, NOT Kafka + PostgreSQL

---

## The Outbox Pattern

**Problem:** How to ensure database write and Kafka publish are atomic?

```
DB INSERT succeeds → Kafka publish fails → Inconsistent!
Kafka publish succeeds → DB INSERT fails → Inconsistent!
```

**Solution:** Outbox Pattern

```
PostgreSQL Transaction
    ├── INSERT INTO orders (business data)
    └── INSERT INTO outbox_events (event to publish)

Outbox Publisher (separate process)
    └── Poll outbox → Publish to Kafka → Mark processed
```

**Benefits:**
- Atomic: Both succeed or both fail together
- Reliable: Publisher retries on failure
- Decoupled: Business logic doesn't depend on Kafka availability

---

## Configuration

### Environment Variables

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9093` | Kafka broker addresses |
| `SLOW_CONSUMER` | `false` | Enable slow consumer simulation |
| `SLOW_CONSUMER_DELAY` | `5s` | Delay per message |
| `LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |

---

## Makefile Commands

```bash
make up              # Start infrastructure
make down            # Stop infrastructure
make logs            # View infrastructure logs
make topics          # List Kafka topics
make topic-describe TOPIC=orders.v1  # Describe a topic
make consumer-groups # List consumer groups
make consumer-group-describe GROUP=payment-service  # Describe group

make order           # Start order service with hot reload
make payment         # Start payment service with hot reload
make notification    # Start notification service with hot reload

make build           # Build all services
make test            # Run all tests
make lint            # Run linter

make cluster-up      # Start 3-broker cluster
make cluster-down    # Stop cluster
```

---

## Kafka KRaft Mode

This project uses Kafka in **KRaft mode** (no ZooKeeper).

### Kafka + ZooKeeper (Legacy)
```
Producer → Broker → ZooKeeper (metadata)
Consumer → Broker → ZooKeeper (offsets)
```

### Kafka KRaft (Modern)
```
Producer → Broker/Controller (metadata + data)
Consumer → Broker/Controller (metadata + data)
```

**Benefits of KRaft:**
- Simpler architecture (one less component)
- Faster partition leadership changes
- Better scalability (millions of partitions)
- Single process for broker + controller

---

## Observing Consumer Lag

### Using Kafka UI
1. Open http://localhost:8080
2. Go to Consumer Groups
3. Select a group
4. View lag per partition

### Using CLI
```bash
docker exec kafka-broker kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group payment-service
```

Output:
```
TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders.v1  0          42              45              3
orders.v1  1          38              40              2
orders.v1  2          40              40              0
```

---

## Troubleshooting

### Services Won't Start

**Problem:** Connection refused to Kafka

**Solution:**
```bash
# Check Kafka is healthy
docker compose ps

# Wait for topic creation
docker compose logs topic-creator

# Restart services
docker compose restart kafka
```

### Messages Not Being Consumed

**Problem:** Messages in topic but not consumed

**Solution:**
```bash
# Check consumer group status
make consumer-group-describe GROUP=payment-service

# Check if consumer is connected
docker compose logs payment-service | grep "consumer started"
```

### Duplicate Processing

**Problem:** Same payment processed twice

**Solution:** This is expected with at-least-once delivery. The idempotency check using `event_id` prevents duplicate charges. Check logs for "duplicate payment event detected".

### High Consumer Lag

**Problem:** Consumer lag keeps increasing

**Solutions:**
1. Add more consumers (up to partition count)
2. Optimize processing logic
3. Increase partitions (requires topic recreation)
4. Reduce per-message processing time

---

## Testing

```bash
# Run all tests
make test

# Run specific service tests
make order-test
make payment-test
make notification-test

# Run with verbose output
go test -v ./services/payment-service/...
```

---

## Project Structure

```
kafka-practice/
├── docker-compose.yml           # Single broker setup
├── docker-compose.cluster.yml   # 3-broker cluster
├── Makefile
├── README.md
├── .env.example
│
├── services/
│   ├── order-service/
│   │   ├── cmd/main.go          # Entry point
│   │   ├── internal/
│   │   │   ├── config/          # Configuration
│   │   │   ├── handler/         # HTTP handlers
│   │   │   ├── model/           # Domain models
│   │   │   ├── repository/      # Database access
│   │   │   ├── service/         # Business logic
│   │   │   └── outbox/          # Outbox publisher
│   │   ├── Dockerfile
│   │   └── .air.toml
│   │
│   ├── payment-service/
│   │   ├── cmd/main.go
│   │   ├── internal/
│   │   │   ├── config/
│   │   │   ├── consumer/        # Kafka consumers
│   │   │   ├── model/
│   │   │   ├── repository/
│   │   │   └── service/
│   │   ├── Dockerfile
│   │   └── .air.toml
│   │
│   └── notification-service/
│       └── ... (similar structure)
│
├── pkg/
│   ├── events/                  # Event definitions
│   │   ├── envelope.go          # Common envelope
│   │   ├── order_events.go
│   │   ├── payment_events.go
│   │   └── notification_events.go
│   ├── kafka/                   # Kafka utilities
│   │   ├── producer.go
│   │   ├── consumer.go
│   │   ├── publisher.go
│   │   └── topics.go
│   └── observability/
│       └── logger.go
│
├── docs/
│   ├── architecture.md
│   ├── kafka-concepts.md
│   ├── topics.md
│   ├── failure-scenarios.md
│   ├── troubleshooting.md
│   └── advanced-concepts.md
│
└── scripts/
    ├── create-topics.sh
    └── create-topics-cluster.sh
```

---

## What Was Implemented vs Documented

### Implemented (Working Code)

| Feature | Location |
|---------|----------|
| Order REST API | `services/order-service` |
| Outbox Pattern | `services/order-service/internal/outbox` |
| Kafka Producer | `pkg/kafka/producer.go` |
| Kafka Consumer | `pkg/kafka/consumer.go` |
| Consumer Groups | Payment & Notification services |
| Manual Offset Commit | All consumers |
| Idempotency | Payment & Notification services |
| Retry Logic | `services/payment-service/internal/consumer` |
| Dead Letter Topic | `services/payment-service/internal/consumer` |
| Event Envelope | `pkg/events/envelope.go` |
| Kafka Headers | `pkg/kafka/publisher.go` |
| Schema Evolution | `pkg/events/order_events.go` (V1/V2) |
| Slow Consumer | Config flag in all consumers |
| Graceful Shutdown | All services |
| Structured Logging | `pkg/observability/logger.go` |

### Documented Only (See `docs/advanced-concepts.md`)

| Concept | Description |
|---------|-------------|
| Kafka Transactions | Atomic Kafka writes (not distributed) |
| Kafka Fencing | Producer epoch and stale instances |
| CDC / Debezium | Change Data Capture from PostgreSQL |
| Kafka Streams | Stream processing (Java ecosystem) |
| Kafka Connect | Connector framework |
| Replication / ISR | Multi-broker cluster concepts |
| Security (TLS/SASL) | Authentication and authorization |

---

## Interview Questions

Based on this project, you should be able to answer:

1. **What is the difference between a topic and a partition?**
2. **How do consumer groups enable parallel processing?**
3. **What happens when there are more consumers than partitions?**
4. **Explain the difference between at-least-once and exactly-once delivery.**
5. **Why is idempotency important in Kafka consumers?**
6. **What is the Outbox pattern and why is it needed?**
7. **How does Kafka ensure ordering?**
8. **What is consumer lag and how do you reduce it?**
9. **Explain the role of message keys in Kafka.**
10. **What is a Dead Letter Topic and when would you use one?**
11. **How do you handle schema evolution in Kafka?**
12. **What is the difference between Kafka headers and payload?**
13. **Explain Kafka's KRaft mode vs ZooKeeper.**
14. **What is rebalancing and when does it occur?**
15. **How do you ensure exactly-once processing across Kafka and a database?**

---

## Learning Roadmap

After completing this project, here's what to study next:

### Beginner → Intermediate
- [x] Topics, partitions, offsets
- [x] Consumer groups
- [x] Basic producer/consumer
- [x] Manual offset commit
- [x] Idempotency

### Intermediate → Advanced
- [ ] Kafka transactions (read `docs/advanced-concepts.md`)
- [ ] Schema Registry with Avro/Protobuf
- [ ] Kafka Streams for stream processing
- [ ] Kafka Connect for integrations
- [ ] Monitoring with Prometheus/Grafana

### Advanced → Expert
- [ ] Multi-datacenter replication (MirrorMaker)
- [ ] Custom partitioners
- [ ] Performance tuning
- [ ] Security hardening (mTLS, SASL, ACLs)
- [ ] Production deployment patterns

### Recommended Resources
- [Kafka: The Definitive Guide](https://www.confluent.io/product/kafka-definitive-guide/)
- [Confluent Documentation](https://docs.confluent.io/)
- [Kafka Best Practices](https://docs.confluent.io/current/kafka/deployment.html)

---

## Kafka Go Library

This project uses [segmentio/kafka-go](https://github.com/segmentio/kafka-go).

**Why this library?**
- Pure Go (no CGO)
- Clean API
- Good documentation
- Active maintenance
- Supports all features needed for this project

**Alternatives:**
- [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) - librdkafka wrapper
- [sarama](https://github.com/IBM/sarama) - Shopify's library

---

## License

This project is for educational purposes.

---

## Contributing

This is a learning project. If you find issues or have suggestions, please open an issue.

---

## Summary

This project demonstrates that Kafka is not just a message queue—it's a **distributed event streaming platform** that enables:

- **Decoupled services** through events
- **Scalable processing** through partitions
- **Reliable delivery** through consumer groups and offsets
- **Fault tolerance** through replication
- **Real-time processing** through streaming

The patterns learned here (Outbox, idempotency, retry/DLT) are essential for building production microservices.
