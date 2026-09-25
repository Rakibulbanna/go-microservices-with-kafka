# Kafka Microservices Learning Project — Go

You are a senior Go backend engineer and Kafka/microservices architect.

I am learning Apache Kafka for real-world microservices development. I want you to build a small but production-style learning project in **Go** that demonstrates the important Kafka concepts through practical use cases.

## My Environment

* Ubuntu Linux
* Go
* Docker
* Docker Compose
* Air for Go hot reload
* Apache Kafka in KRaft mode
* PostgreSQL
* Redis if genuinely useful
* REST API between services where synchronous communication is appropriate

Do NOT use Kubernetes for this project.

Keep the project small enough that I can understand every part.

---

# Main Goal

Build a practical **Order / Payment / Notification** microservices system using Kafka.

Use exactly 3 Go services:

1. `order-service`
2. `payment-service`
3. `notification-service`

The project must demonstrate Kafka concepts through actual working code, not just comments or theoretical examples.

The code should be simple enough for a developer learning Kafka to understand, while following professional Go backend practices.

---

# Business Scenario

An order is created through the Order Service.

Example:

```text
POST /orders
```

Order Service creates the order and publishes:

```text
order.created
```

Payment Service consumes the event.

If payment succeeds:

```text
payment.completed
```

If payment fails:

```text
payment.failed
```

Notification Service consumes relevant events and simulates sending notifications.

The system should demonstrate both synchronous REST communication and asynchronous Kafka communication.

---

# Architecture

Use this basic architecture:

```text
                    HTTP
                     │
                     ▼
              ┌──────────────┐
              │ Order Service│
              └──────┬───────┘
                     │
                     │ order.created
                     ▼
                  Kafka
             ┌───────┴────────┐
             │                │
             ▼                ▼
       Payment Group     Notification Group
             │                │
             ▼                ▼
      Payment Service   Notification Service
             │
             │ payment.completed / failed
             ▼
           Kafka
             │
             ▼
      Notification Service
```

Important:

* Payment Service and Notification Service must use DIFFERENT consumer groups.
* Both groups should independently receive the same events.
* Within one consumer group, a partition must be assigned to only one consumer at a time.

---

# Kafka Topics

Create meaningful topic names.

At minimum:

```text
orders.v1
payments.v1
notifications.v1
orders.retry.v1
orders.dlt.v1
payments.retry.v1
payments.dlt.v1
```

Explain in README why these names are used.

Use versioning such as:

```text
orders.v1
```

rather than vague names such as:

```text
data
events
kafka-topic
```

---

# Partitions

Create topics with multiple partitions.

For example:

```text
orders.v1 -> 3 partitions
payments.v1 -> 3 partitions
notifications.v1 -> 3 partitions
```

Use an appropriate Kafka message key.

For order events:

```text
key = order_id
```

Explain and demonstrate that the same `order_id` is routed to the same partition, providing ordering for that order.

Demonstrate:

```text
order-101
order-101
order-101
```

being processed in partition order.

Explain that Kafka ordering is per partition, NOT global topic ordering.

---

# Consumer Groups

Use explicit consumer group names:

```text
payment-service
notification-service
```

Demonstrate:

```text
orders.v1
   │
   ├── payment-service group
   │      └── Payment Service
   │
   └── notification-service group
          └── Notification Service
```

Both groups must independently consume the events.

Also demonstrate what happens when:

```text
3 partitions
5 consumers
```

exist inside one consumer group.

Explain why 2 consumers become idle.

---

# Consumer Assignment

Implement enough logging to clearly show:

```text
consumer started
consumer group
partition assigned
partition revoked
partition processing
consumer stopped
```

When rebalancing happens, the logs should make it obvious.

Example:

```text
[consumer=payment-1] assigned partition=0
[consumer=payment-2] assigned partition=1
[consumer=payment-3] assigned partition=2
```

If there are more consumers than partitions, explain:

```text
consumer-4 -> idle
consumer-5 -> idle
```

---

# Offset

Demonstrate Kafka offsets.

Logs should include:

```text
topic
partition
offset
key
event_id
```

Example:

```text
topic=orders.v1 partition=1 offset=42 event_id=...
```

Explain:

* offset is partition-specific
* Kafka does not use one global offset for a topic
* consumer groups maintain their own progress

---

# Offset Commit

Demonstrate manual/explicit offset handling.

Do NOT blindly commit before successful processing.

Use the flow:

```text
consume
   ↓
validate
   ↓
process
   ↓
success
   ↓
commit offset
```

If processing fails:

```text
consume
   ↓
process
   ↓
failure
   ↓
retry / DLT strategy
```

Explain the consequences of committing before processing.

---

# Delivery Semantics

Demonstrate:

## At-most-once

Explain with a small example.

## At-least-once

Use this as the main processing model.

Show why duplicate processing can happen.

## Exactly-once

Explain Kafka's exactly-once capabilities and limitations.

IMPORTANT:

Do NOT claim that Kafka transactions magically make PostgreSQL + Kafka + external APIs one atomic transaction.

Explain the boundary clearly.

---

# Idempotency

Payment processing MUST be idempotent.

Use:

```text
event_id
```

or another appropriate idempotency key.

Demonstrate:

```text
payment event received
payment processed
offset not committed
same event received again
idempotency check detects duplicate
payment is NOT charged twice
```

Explain why idempotency is essential in real microservices.

---

# Event Structure

Create a common event envelope.

Example:

```json
{
  "event_id": "uuid",
  "event_type": "order.created",
  "version": 1,
  "occurred_at": "timestamp",
  "correlation_id": "uuid",
  "causation_id": "uuid",
  "producer": "order-service",
  "data": {}
}
```

Use Go structs.

Keep event payloads explicit.

Do not use:

```go
map[string]interface{}
```

everywhere.

Prefer typed structures.

---

# Headers

Use Kafka headers for metadata such as:

```text
event_type
correlation_id
causation_id
trace_id
```

Explain why metadata can belong in headers instead of the event payload.

---

# Serialization

Use JSON initially because this is a learning project.

Create a clear serialization/deserialization layer.

Explain that production systems may use:

* Avro
* Protobuf
* JSON Schema

---

# Schema Evolution

Demonstrate a simple version change.

For example:

```text
OrderCreatedV1
```

then:

```text
OrderCreatedV2
```

Add a backward-compatible field.

Explain:

* backward compatibility
* forward compatibility
* breaking changes
* why event contracts matter

Do not introduce a complicated Schema Registry implementation unless it genuinely improves the learning project.

If Schema Registry is added, keep it optional and clearly separated from the core Kafka implementation.

---

# Retry

Implement retry handling.

Example:

```text
orders.v1
   ↓
Payment Service
   ↓
processing failure
   ↓
orders.retry.v1
   ↓
retry
```

Use a reasonable retry strategy.

Explain:

* retry count
* retry delay
* transient errors
* permanent errors

Do not create infinite retry loops.

---

# Dead Letter Topic

Implement DLT behavior.

Example:

```text
payments.v1
     ↓
consumer fails
     ↓
retry
     ↓
retry
     ↓
maximum attempts exceeded
     ↓
payments.dlt.v1
```

DLT message should contain enough information to debug the original event.

Include:

```text
original topic
partition
offset
event_id
error
timestamp
retry count
original payload
```

---

# Consumer Lag

Make the project easy to use for learning consumer lag.

Create a deliberately slow consumer option.

For example:

```text
SLOW_CONSUMER=true
```

When enabled, processing should sleep for a configurable amount of time.

Explain:

```text
producer rate > consumer processing rate
```

causing lag.

Add documentation explaining how to observe lag.

---

# Backpressure

Demonstrate what happens when consumers cannot keep up.

Explain:

```text
high producer throughput
+
slow consumer
=
increasing consumer lag
```

Discuss practical approaches:

* increase partitions
* increase consumers
* optimize processing
* batch processing
* reduce unnecessary work
* scale consumers

Also explain why simply adding consumers does not help once consumer count exceeds partition count.

---

# Rebalancing

Demonstrate consumer group rebalancing.

Provide instructions:

1. Start one consumer.
2. Start another consumer.
3. Stop one consumer.
4. Observe partition reassignment.

Logs must make the rebalance visible.

Explain:

```text
consumer joins
consumer leaves
consumer crashes
partition assignment changes
```

---

# Replication

Configure Kafka with replication where practical.

If running a multi-broker Kafka cluster in Docker is reasonable, create:

```text
broker-1
broker-2
broker-3
```

using KRaft.

Use replication factor where supported by the cluster.

If a 3-broker setup makes the project unnecessarily difficult on my Ubuntu machine, provide a simpler single-broker development profile AND a 3-broker learning profile.

Do not hide this complexity.

Explain:

```text
leader
follower
replication factor
ISR
```

---

# ACK

Explain producer acknowledgements.

Demonstrate the configuration conceptually and, where supported by the chosen Go Kafka library, configure:

```text
acks=all
```

Explain the difference between:

```text
acks=0
acks=1
acks=all
```

Do not oversimplify durability guarantees.

---

# Producer Reliability

Configure appropriate producer reliability.

Discuss:

* retries
* idempotent producer
* acknowledgements
* batching
* compression

Use safe defaults for this learning project.

---

# KRaft

Kafka MUST run in KRaft mode.

Do NOT use ZooKeeper.

Explain:

```text
Kafka + ZooKeeper
```

versus:

```text
Kafka KRaft
```

Explain the role of Kafka controllers and metadata quorum at a high level.

Keep the explanation practical.

---

# Kafka Fencing

Include a dedicated learning section for Kafka fencing.

Explain:

* stale producer instance
* producer identity / epoch
* transactional producer
* why an old producer should not continue writing when a newer valid producer instance takes over

Do NOT add unnecessary complexity to the main business flow just to demonstrate fencing.

Create a small optional example or explanation if implementing it directly would make the project harder to understand.

---

# Kafka Transactions

Create an optional transaction example.

Explain:

```text
producer sends multiple Kafka records
```

and how Kafka transactions can atomically commit Kafka writes.

IMPORTANT:

Explicitly explain the difference between:

```text
Kafka transaction
```

and:

```text
PostgreSQL transaction + Kafka transaction
```

Do not falsely claim distributed atomicity.

---

# Outbox Pattern

Implement an Outbox example in Order Service.

Problem:

```text
DB INSERT succeeds
Kafka publish fails
```

or:

```text
Kafka publish succeeds
DB INSERT fails
```

Solution:

```text
PostgreSQL transaction
       │
       ├── orders table
       │
       └── outbox_events table
```

Then a publisher reads the outbox and publishes to Kafka.

Implement a simple polling outbox worker.

Explain:

```text
business data
+
outbox event
```

being written in the same PostgreSQL transaction.

Make the outbox publisher idempotent.

---

# CDC / Debezium

Do NOT make Debezium mandatory for the core project.

Create an optional section explaining:

```text
PostgreSQL
   ↓
Debezium
   ↓
Kafka
```

Explain when CDC is useful and how it differs from an application-level Outbox publisher.

If adding a Docker Compose profile for Debezium is reasonable, keep it optional.

---

# Event vs Command

Explain the difference.

Examples:

Event:

```text
order.created
```

Command:

```text
process.payment
```

Use event-oriented naming for the main Kafka flow.

Explain why naming matters.

---

# REST vs Kafka

Use REST where synchronous request/response is appropriate.

Use Kafka where asynchronous event-driven communication is appropriate.

Document examples:

REST:

```text
Client → Order Service
```

Kafka:

```text
Order Service → Payment Service
```

Do not turn everything into Kafka.

---

# Database

Use PostgreSQL.

Each service should have clear ownership of its data.

Prefer:

```text
Order Service → orders DB
Payment Service → payments DB
Notification Service → notifications DB
```

Do not create one shared database schema for all services.

Use migrations.

Keep database code simple.

---

# Redis

Redis is optional.

Only add Redis if it demonstrates something useful such as:

* idempotency cache
* distributed lock example
* temporary state

Do not add Redis just because it is commonly used with Kafka.

---

# Go Project Structure

Use a clean but not over-engineered structure.

Example:

```text
kafka-microservices/
│
├── docker-compose.yml
├── Makefile
├── README.md
├── .env.example
│
├── services/
│   ├── order-service/
│   │   ├── cmd/
│   │   ├── internal/
│   │   ├── migrations/
│   │   ├── Dockerfile
│   │   └── .air.toml
│   │
│   ├── payment-service/
│   │   ├── cmd/
│   │   ├── internal/
│   │   ├── migrations/
│   │   ├── Dockerfile
│   │   └── .air.toml
│   │
│   └── notification-service/
│       ├── cmd/
│       ├── internal/
│       ├── migrations/
│       ├── Dockerfile
│       └── .air.toml
│
├── pkg/
│   ├── events/
│   ├── kafka/
│   └── observability/
│
└── docs/
    ├── architecture.md
    ├── kafka-concepts.md
    ├── topics.md
    ├── failure-scenarios.md
    └── troubleshooting.md
```

Do not create unnecessary abstractions.

I am learning Go and Kafka, so readability is more important than abstraction for abstraction's sake.

---

# Kafka Go Library

Choose one mature Go Kafka client.

Prefer:

```text
github.com/segmentio/kafka-go
```

unless you have a strong technical reason to use another library.

Explain the choice in the README.

Keep Kafka-specific code isolated so the business logic is not tightly coupled to the library.

---

# Docker

Create Docker Compose for:

* Kafka
* PostgreSQL
* optional Redis
* Order Service
* Payment Service
* Notification Service

Kafka must use KRaft.

Provide:

```bash
docker compose up -d
```

for infrastructure.

Also support local Go development:

```bash
air
```

for each service.

---

# Development Workflow

I want to be able to run:

```bash
docker compose up -d
```

then:

```bash
cd services/order-service
air
```

and similarly for the other services.

Provide a Makefile with useful commands:

```bash
make up
make down
make logs
make topics
make test
make lint
make build
```

---

# REST API

Implement at minimum:

```http
POST /orders
GET /orders/:id
GET /health
```

Example request:

```json
{
  "customer_id": "customer-1",
  "items": [
    {
      "product_id": "product-1",
      "quantity": 2,
      "price": 100
    }
  ]
}
```

Creating the order should result in an event being published.

---

# Observability

Use structured logging.

Every important log should include useful context.

Example:

```text
service
event_id
event_type
correlation_id
topic
partition
offset
consumer_group
order_id
```

Provide a simple way to follow one request/event across services using:

```text
correlation_id
```

---

# Graceful Shutdown

All Go services must properly handle:

```text
SIGTERM
SIGINT
```

Close:

* HTTP server
* Kafka consumers
* Kafka producers
* DB connections

Use context cancellation.

Do not leave goroutines running unnecessarily.

---

# Error Handling

Use idiomatic Go error handling.

Avoid:

```go
panic()
```

for normal application errors.

Do not silently ignore errors.

Return meaningful errors.

Keep infrastructure errors separate from business errors where reasonable.

---

# Testing

Write tests for important behavior.

At minimum:

* event serialization
* event deserialization
* idempotency
* order creation
* payment processing
* retry logic
* DLT behavior

If practical, add Kafka integration tests.

Do not make the test suite unnecessarily complicated.

---

# Security

For local development, keep Kafka security simple.

Create a documentation section explaining:

* TLS
* SASL
* ACL
* authentication
* authorization

Do not make production Kafka security mandatory for the local learning setup unless it can be done cleanly.

---

# Kafka UI

Add a Kafka UI tool to Docker Compose if possible.

Use it to inspect:

* topics
* partitions
* messages
* consumer groups
* offsets
* consumer lag

Explain how I can use it for learning.

---

# Failure Scenarios

The README must contain hands-on experiments.

At minimum:

## Experiment 1 — Basic flow

Create an order.

Observe:

```text
order.created
payment.completed
notification
```

## Experiment 2 — Multiple consumer groups

Show that both:

```text
payment-service
notification-service
```

receive the same event.

## Experiment 3 — More consumers than partitions

Run 5 consumers against 3 partitions.

Observe idle consumers.

## Experiment 4 — Rebalance

Start and stop consumers.

Observe partition reassignment.

## Experiment 5 — Duplicate event

Force processing before offset commit.

Observe duplicate delivery.

Verify idempotency prevents duplicate payment.

## Experiment 6 — Slow consumer

Enable:

```text
SLOW_CONSUMER=true
```

Observe consumer lag.

## Experiment 7 — Retry

Force payment failure.

Observe retry topic.

## Experiment 8 — DLT

Force permanent failure.

Observe DLT.

## Experiment 9 — Ordering

Send multiple events for the same order.

Verify same key routes them consistently.

## Experiment 10 — Outbox

Temporarily simulate Kafka failure.

Verify business data and outbox event remain consistent.

## Experiment 11 — Consumer scaling

Compare:

```text
3 partitions / 1 consumer
3 partitions / 3 consumers
3 partitions / 5 consumers
```

Explain throughput implications.

## Experiment 12 — Broker failure

If using the 3-broker profile, stop one broker and observe replication/failover behavior.

---

# Documentation Requirements

The project must include a beginner-friendly README.

Do NOT just explain what commands to run.

For every major Kafka concept, explain:

```text
Problem
↓
Kafka concept
↓
Implementation
↓
What to observe
↓
Why it matters in production
```

Example:

```text
Problem:
Payment Service is slow.

Solution:
Use asynchronous Kafka communication.

Implementation:
Order Service publishes order.created.

Result:
Order creation does not have to wait for payment processing.
```

---

# Important Kafka Concepts Checklist

Make sure the implementation/documentation covers all of these:

* Kafka
* Broker
* Cluster
* Topic
* Topic naming
* Partition
* Record/message/event
* Producer
* Consumer
* Consumer Group
* Consumer assignment
* Idle consumers
* Offset
* Commit
* Key
* Value
* Timestamp
* Headers
* Leader
* Follower
* Replica
* Replication Factor
* ISR
* Retention
* Kafka log
* ACK
* Producer retry
* Idempotent producer
* Consumer lag
* Backpressure
* Rebalance
* Serialization
* Schema evolution
* Event contract
* Event vs command
* Event-driven architecture
* REST vs Kafka
* Retry topic
* DLT
* Idempotent consumer
* Outbox pattern
* CDC/Debezium
* Kafka transactions
* KRaft
* Kafka controller
* Kafka fencing
* Kafka Connect
* Kafka Streams
* Security
* Monitoring
* Kafka UI

For concepts that are not directly implemented, create an optional `docs/advanced-concepts.md` section with a practical explanation.

Do NOT add unnecessary infrastructure just to claim every concept is implemented.

---

# Kafka Streams and Kafka Connect

These are not required for the core business flow.

Create documentation explaining:

## Kafka Connect

What it is and when to use it.

Example:

```text
PostgreSQL → Debezium → Kafka
```

## Kafka Streams

Explain stream processing and when it would be useful.

Do not introduce Kafka Streams into the core Go services because this project is focused on learning Kafka fundamentals with Go.

---

# Code Quality Rules

Follow these rules:

1. Idiomatic Go.
2. Small functions.
3. Clear names.
4. Context-aware operations.
5. Proper error handling.
6. No unnecessary abstraction.
7. No giant files.
8. No duplicated Kafka configuration.
9. Keep business logic independent from Kafka where practical.
10. Use interfaces only where they provide real value.
11. Do not use `interface{}` everywhere.
12. Prefer typed event structures.
13. Avoid global mutable state.
14. Graceful shutdown.
15. Structured logging.
16. Configuration through environment variables.
17. Never hardcode passwords/secrets.
18. Provide `.env.example`.
19. Never read or expose my existing `.env` files.
20. Never modify unrelated files on my machine.

---

# Important AI Coding Rule

Before implementing, first inspect the project directory.

Do not immediately generate hundreds of files.

First:

1. Inspect the environment.
2. Check installed Go version.
3. Check Docker.
4. Check Docker Compose.
5. Check Air.
6. Decide the project structure.
7. Create a short implementation plan.
8. Then implement incrementally.

After each major phase:

```text
implement
→ run
→ test
→ fix
→ explain
```

Do not assume code works without running it.

---

# Implementation Phases

Implement in this order.

## Phase 1

Infrastructure:

* Docker Compose
* Kafka KRaft
* PostgreSQL
* Kafka UI
* topics

## Phase 2

Order Service:

* REST API
* PostgreSQL
* order creation
* basic Kafka producer

## Phase 3

Payment Service:

* Kafka consumer
* consumer group
* payment processing
* payment event

## Phase 4

Notification Service:

* separate consumer group
* event consumption

## Phase 5

Reliability:

* manual commits
* idempotency
* retry
* DLT
* error handling

## Phase 6

Scaling:

* partitions
* multiple consumers
* consumer groups
* rebalance
* lag
* backpressure

## Phase 7

Production patterns:

* event envelope
* headers
* correlation ID
* outbox
* schema evolution

## Phase 8

Advanced:

* replication
* ISR
* ACK
* idempotent producer
* transactions
* fencing
* CDC/Debezium explanation

---

# Final Requirement

When you finish, I should be able to clone/open the project and learn Kafka by actually running experiments.

The project is primarily a **learning laboratory**, not a production SaaS application.

Optimize for:

```text
understanding > complexity
```

and:

```text
real Kafka behavior > fake demonstrations
```

Do not hide important Kafka behavior behind excessive abstractions.

At the end, provide:

1. Complete project.
2. README.
3. Architecture diagram.
4. Kafka concept guide.
5. Commands to run everything.
6. Troubleshooting guide.
7. Hands-on experiments.
8. Interview questions based on this project.
9. Explanation of what was implemented versus what is documented only.
10. A final learning roadmap showing what I should study next after completing this project.
