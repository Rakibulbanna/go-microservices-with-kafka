# Kafka Concepts Guide

## Core Concepts

### Broker
A Kafka server that stores topics and handles client requests.

### Cluster
A set of brokers working together. In this project:
- Single broker (development)
- 3-broker cluster (optional, for learning replication)

### Topic
A named feed of messages. Examples: `orders.v1`, `payments.v1`

### Partition
A topic is split into partitions for parallelism. Each partition is:
- An ordered, immutable sequence of messages
- Identified by a unique offset
- Assigned to one consumer per group

### Record (Message/Event)
A single entry in a partition containing:
- Key (optional) - determines partition
- Value - the message payload
- Timestamp
- Headers (optional metadata)

### Producer
Publishes messages to topics. In this project:
- Order Service produces to `orders.v1`
- Payment Service produces to `payments.v1`

### Consumer
Subscribes to topics and processes messages.

### Consumer Group
A set of consumers that cooperate to consume a topic.
- Each partition is assigned to exactly ONE consumer per group
- Different groups independently receive all messages

---

## Key Mechanics

### Partitioning

Messages are distributed across partitions using the key:

```
partition = hash(key) % num_partitions
```

Same key → Same partition → Ordering guaranteed for that key.

**Example:**
```
order-101 → partition 0
order-101 → partition 0 (always)
order-102 → partition 2
```

### Offsets

Each message in a partition has a unique offset:

```
Partition 0:
  offset 0: msg_a
  offset 1: msg_b
  offset 2: msg_c
              ↑
         consumer committed here
```

Each consumer group maintains its own offset per partition.

### Consumer Assignment

When a consumer joins a group:
1. Group coordinator assigns partitions
2. Partitions are distributed evenly
3. If consumers > partitions, some consumers are idle

**Example:**
```
3 partitions, 1 consumer:
  consumer-1: [p0, p1, p2]

3 partitions, 3 consumers:
  consumer-1: [p0]
  consumer-2: [p1]
  consumer-3: [p2]

3 partitions, 5 consumers:
  consumer-1: [p0]
  consumer-2: [p1]
  consumer-3: [p2]
  consumer-4: [idle]
  consumer-5: [idle]
```

### Rebalancing

When consumers join/leave a group, partitions are reassigned:

```
Before:
  consumer-1: [p0, p1, p2]

consumer-2 joins:
  consumer-1: [p0, p1]
  consumer-2: [p2]

consumer-1 leaves:
  consumer-2: [p0, p1, p2]
```

---

## Delivery Semantics

### At-Most-Once
```
consume → commit → process
```
- If process fails after commit, message is lost
- Fast but unreliable

### At-Least-Once (This Project)
```
consume → process → commit
```
- If commit fails after process, message is reprocessed
- No message loss, but duplicates possible
- Requires idempotency

### Exactly-Once
- Requires Kafka transactions
- Complex to implement correctly
- Only makes Kafka writes atomic, not Kafka + external systems

---

## Idempotency

**Problem:** At-least-once delivery can cause duplicates.

**Solution:** Use a unique identifier (event_id) to detect duplicates.

```
1. Receive event (event_id=abc123)
2. Check: Has abc123 been processed?
   - Yes → Skip (already processed)
   - No  → Process and record abc123
```

In this project:
- Payment Service checks `event_id` before processing
- Notification Service checks `event_id` before sending

---

## Retry Pattern

When processing fails:

```
orders.v1
    ↓
Payment Service
    ↓
Processing fails (transient error)
    ↓
Send to payments.retry.v1
    ↓
Retry consumer picks up
    ↓
Wait (exponential backoff)
    ↓
Retry processing
    ↓
Success → Done
Failure → Retry again (up to max)
    ↓
Max retries exceeded
    ↓
Send to payments.dlt.v1
```

---

## Dead Letter Topic (DLT)

Messages that fail after all retries go to DLT:

```json
{
  "event_id": "original-event-id",
  "event_type": "order.created.dlt",
  "data": {
    "original_topic": "orders.v1",
    "partition": 0,
    "offset": 42,
    "error": "payment gateway timeout",
    "retry_count": 3,
    "original_payload": "{...}",
    "timestamp": "2024-01-15T10:30:00Z"
  }
}
```

**Why DLT?**
- Isolate problematic messages
- Debug failures
- Manual intervention possible
- Different retention policy

---

## Consumer Lag

```
Producer rate: 100 msg/sec
Consumer rate: 80 msg/sec
Lag: 20 msg/sec (growing)
```

**Causes:**
- Slow processing logic
- Insufficient consumers
- Insufficient partitions

**Solutions:**
1. Optimize processing
2. Add consumers (up to partition count)
3. Add partitions (requires topic recreation)
4. Batch processing

---

## Ordering

Kafka guarantees ordering **within a partition**, not globally.

```
Topic: orders.v1 (3 partitions)

Partition 0: [order-101:create] [order-101:update] [order-101:cancel]
Partition 1: [order-102:create] [order-102:update]
Partition 2: [order-103:create]
```

- order-101 events are ordered (same partition)
- order-101 and order-102 may be processed in any order (different partitions)

---

## Headers vs Payload

**Headers** (metadata):
- event_type
- correlation_id
- event_id
- producer

**Payload** (data):
- Business data (order details, payment info)

**Why separate?**
- Read metadata without deserializing payload
- Routing/filtering based on headers
- Schema evolution without breaking header consumers

---

## The Outbox Pattern

**Problem:** Atomic write to database AND publish to Kafka.

**Solution:**
```
BEGIN TRANSACTION
  INSERT INTO orders (...)
  INSERT INTO outbox_events (event_data)
COMMIT

Outbox Publisher (async):
  LOOP:
    SELECT * FROM outbox_events WHERE processed = false
    FOR EACH event:
      Publish to Kafka
      UPDATE outbox_events SET processed = true
```

**Benefits:**
- Atomic: Both succeed or both fail
- Reliable: Publisher retries on failure
- Decoupled: Business logic doesn't depend on Kafka

---

## Schema Evolution

**Backward Compatible:**
- Adding optional fields
- V2 consumer can read V1 events

**Forward Compatible:**
- V1 consumer can read V2 events (ignores new fields)

**Breaking Changes:**
- Removing fields
- Changing field types
- Renaming fields

**Solution:** Version topics (`orders.v1`, `orders.v2`)

---

## KRaft vs ZooKeeper

### ZooKeeper Mode (Legacy)
```
Broker ←→ ZooKeeper (metadata, leadership)
```
- Separate ZooKeeper cluster required
- Slower leadership changes
- Limited scalability

### KRaft Mode (Modern)
```
Broker/Controller (combined)
```
- No ZooKeeper needed
- Faster leadership changes
- Better scalability
- Simpler operations

This project uses KRaft mode.
