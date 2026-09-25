# Advanced Kafka Concepts

This document covers advanced concepts that are documented but not fully implemented in the core project.

---

## Kafka Transactions

### What Are Kafka Transactions?

Kafka transactions allow atomic writes across multiple partitions and topics.

```java
producer.beginTransaction();
producer.send(new ProducerRecord("topic-a", key, value1));
producer.send(new ProducerRecord("topic-b", key, value2));
producer.commitTransaction();
```

Either both messages are written, or neither is.

### What Kafka Transactions Do NOT Do

**Important:** Kafka transactions only make Kafka writes atomic. They do NOT provide distributed transactions across:
- Kafka + PostgreSQL
- Kafka + external APIs
- Kafka + other systems

### Read-Process-Write Pattern

With Kafka transactions, you can achieve exactly-once processing:

```
consumer.subscribe("input-topic")
producer.initTransactions()

while (true) {
    records = consumer.poll()
    producer.beginTransaction()
    
    for (record : records) {
        result = process(record)
        producer.send(new ProducerRecord("output-topic", result))
    }
    
    consumer.commitSync()  // Commit offsets in same transaction
    producer.commitTransaction()
}
```

### Limitations

- Only works within Kafka (not external systems)
- Requires `isolation.level=read_committed` on consumers
- Performance overhead
- Complex error handling

---

## Kafka Fencing

### The Problem

Consider this scenario:

```
Time 1: Producer A (epoch=1) starts writing
Time 2: Network issue - Producer A appears dead
Time 3: Producer B (epoch=2) takes over
Time 4: Producer A recovers and continues writing
```

Now we have two producers writing with different epochs. This can cause:
- Duplicate messages
- Out-of-order messages
- Data corruption

### The Solution: Fencing

Kafka uses **epochs** (monotonically increasing numbers) to fence off stale producers.

```
Producer A: producer.id=100, epoch=1
Producer B: producer.id=100, epoch=2  (same ID, higher epoch)
```

When the broker sees a message from epoch=1 after epoch=2 has been established, it rejects it as stale.

### How It Works

1. Idempotent producer gets a `producer.id` and `epoch`
2. On restart/reconnect, epoch increments
3. Broker tracks the highest epoch per producer.id
4. Messages from lower epochs are rejected (fenced)

### Configuration

```go
writer := &kafka.Writer{
    // Enable idempotent producer
    // This automatically handles fencing
}
```

In segmentio/kafka-go, idempotent production is enabled by default when using `RequiredAcks: RequireAll`.

---

## CDC / Debezium

### What is CDC?

Change Data Capture (CDC) captures changes to database tables and streams them as events.

```
PostgreSQL → Debezium → Kafka → Consumers
   (WAL)     (connector)  (topic)
```

### How Debezium Works

1. Debezium reads PostgreSQL's Write-Ahead Log (WAL)
2. Converts changes to Kafka events
3. Publishes to topics like `postgres.orders.v1`

### Event Example

```json
{
  "before": null,
  "after": {
    "id": "order-123",
    "status": "CREATED"
  },
  "source": {
    "connector": "postgresql",
    "ts_ms": 1705312200000
  },
  "op": "c"
}
```

### CDC vs Application-Level Outbox

| Aspect | CDC (Debezium) | Application Outbox |
|--------|---------------|-------------------|
| Implementation | Infrastructure-level | Application-level |
| Reliability | Database WAL guarantees | Transaction guarantees |
| Schema control | Full table changes | Curated events |
| Complexity | Additional infrastructure | Application code |
| Latency | Very low (WAL) | Polling interval |

### When to Use CDC

- Legacy systems where you can't modify application code
- Need to capture ALL database changes
- Building a data lake or analytics pipeline
- Synchronizing databases

### When to Use Outbox

- You control the application code
- You want curated, meaningful events
- You need domain-specific event structure
- You want to include business context

---

## Kafka Streams

### What is Kafka Streams?

A Java library for stream processing built on top of Kafka.

```java
KStream<String, Order> orders = builder.stream("orders.v1");

KStream<String, Payment> payments = orders
    .filter((key, order) -> order.getTotalAmount() > 0)
    .mapValues(order -> processPayment(order));

payments.to("payments.v1");
```

### Key Features

- **Stateful processing**: Windowed aggregations, joins
- **Exactly-once semantics**: Built-in transactional support
- **Scalability**: Same consumer group mechanism
- **Fault tolerance**: State stored in Kafka topics

### Use Cases

- Real-time aggregations
- Stream joins
- Event sourcing
- Complex event processing

### Why Not in This Project?

This project focuses on Go fundamentals. Kafka Streams is Java-only. For Go stream processing, consider:
- Custom consumers with state stores
- External stream processing engines

---

## Kafka Connect

### What is Kafka Connect?

A framework for connecting Kafka with external systems.

```
Source Connectors:
  PostgreSQL → Kafka
  S3 → Kafka
  MongoDB → Kafka

Sink Connectors:
  Kafka → Elasticsearch
  Kafka → S3
  Kafka → JDBC
```

### Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Source     │────▶│    Kafka     │────▶│    Sink      │
│  Connector   │     │   Connect    │     │  Connector   │
└──────────────┘     └──────────────┘     └──────────────┘
     PostgreSQL         Kafka Cluster        Elasticsearch
```

### Use Cases

- Database replication
- Data integration
- ETL pipelines
- Log aggregation

### Why Not in This Project?

The Outbox pattern handles the primary use case (database → Kafka) at the application level. Kafka Connect would be overkill for this learning project.

---

## Replication and ISR

### Replication Factor

Each partition can be replicated across multiple brokers:

```
Partition 0:
  Leader: Broker 1
  Follower: Broker 2
  Follower: Broker 3
  
Replication Factor = 3
```

### In-Sync Replicas (ISR)

Followers that are caught up with the leader:

```
ISR = [Broker 1, Broker 2, Broker 3]
```

If Broker 3 falls behind:
```
ISR = [Broker 1, Broker 2]
```

### min.insync.replicas

Minimum replicas that must acknowledge a write:

```
acks=all + min.insync.replicas=2
→ At least 2 replicas must confirm write
```

### Leader Election

When leader fails:
1. Controller detects failure
2. Selects new leader from ISR
3. Producers/consumers update metadata

### Observing Replication

```bash
# Describe topic
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic orders.v1

# Output:
Topic: orders.v1  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```

---

## Producer Acknowledgments (ACKs)

### acks=0
Producer doesn't wait for any acknowledgment.
- Fastest
- Messages can be lost

### acks=1
Leader writes message and responds.
- Moderate speed
- Message lost if leader fails before replication

### acks=all (This Project)
All ISR replicas must acknowledge.
- Slowest
- Most durable
- No message loss (with sufficient replicas)

```go
writer := &kafka.Writer{
    RequiredAcks: kafka.RequireAll,  // acks=all
}
```

---

## Security

### TLS Encryption

Encrypt data in transit:

```
Producer ←──TLS──→ Broker ←──TLS──→ Consumer
```

### SASL Authentication

Authenticate clients:

```
SASL/PLAIN: Username/password
SASL/SCRAM: Challenge-response
SASL/GSSAPI: Kerberos
SASL/OAUTHBEARER: OAuth 2.0
```

### ACLs (Access Control Lists)

Control who can do what:

```
User:payment-service can:
  READ topic:orders.v1
  WRITE topic:payments.v1
  READ group:payment-service
```

### Why Not in This Project?

Security adds complexity that's not needed for local development. For production:
- Use TLS for encryption
- Use SASL for authentication
- Use ACLs for authorization
- Consider mTLS for service-to-service auth

---

## Monitoring

### Key Metrics

**Producer:**
- Record send rate
- Record error rate
- Batch size
- Compression ratio

**Consumer:**
- Consumer lag
- Rebalance rate
- Commit rate
- Poll time

**Broker:**
- Request rate
- Under-replicated partitions
- Active controller count
- Disk usage

### Tools

- **Prometheus + Grafana**: Metrics collection and visualization
- **JMX Exporter**: Kafka JMX metrics to Prometheus
- **Burrow**: Consumer lag monitoring
- **Kafka UI**: Visual monitoring (included in this project)

---

## Performance Tuning

### Producer

```go
writer := &kafka.Writer{
    BatchSize:    100,              // Messages per batch
    BatchTimeout: time.Second,      // Max wait before sending
    Compression:  kafka.Snappy,     // Compression codec
    RequiredAcks: kafka.RequireAll, // Durability
}
```

### Consumer

```go
reader := kafka.NewReader(kafka.ReaderConfig{
    MinBytes:   10e3,   // Min bytes per fetch
    MaxBytes:   10e6,   // Max bytes per fetch
    MaxWait:    5 * time.Second,
})
```

### Broker

```
num.partitions=3
default.replication.factor=3
log.segment.bytes=1073741824
log.retention.hours=168
```

---

## Summary

These advanced concepts are important for production Kafka deployments:

| Concept | Purpose |
|---------|---------|
| Transactions | Atomic Kafka writes |
| Fencing | Prevent stale producer writes |
| CDC/Debezium | Database change streaming |
| Kafka Streams | Stream processing |
| Kafka Connect | System integration |
| Replication/ISR | Fault tolerance |
| ACKs | Durability guarantees |
| Security | Encryption and authentication |
| Monitoring | Observability |
| Tuning | Performance optimization |

For most applications, the patterns implemented in this project (Outbox, idempotency, retry/DLT) are sufficient. Advanced features should be added based on specific requirements.
