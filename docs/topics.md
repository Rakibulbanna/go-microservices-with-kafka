# Topics

## Topic List

| Topic | Partitions | Purpose |
|-------|-----------|---------|
| `orders.v1` | 3 | Order events (created, cancelled) |
| `payments.v1` | 3 | Payment events (completed, failed) |
| `notifications.v1` | 3 | Notification events (sent) |
| `orders.retry.v1` | 3 | Order processing retries |
| `orders.dlt.v1` | 3 | Order dead letter topic |
| `payments.retry.v1` | 3 | Payment processing retries |
| `payments.dlt.v1` | 3 | Payment dead letter topic |

---

## Topic Naming Convention

### Pattern
```
<domain>.<version>
<domain>.retry.<version>
<domain>.dlt.<version>
```

### Examples
```
orders.v1         → Main topic, version 1
orders.retry.v1   → Retry topic for orders
orders.dlt.v1     → Dead letter topic for orders
```

### Why Version Topics?

**Schema Evolution:**
- `orders.v1` uses schema version 1
- `orders.v2` can introduce breaking changes
- Consumers migrate at their own pace
- Both versions can coexist

**Why Not Just `orders`?**
- No way to introduce breaking changes
- All consumers must update simultaneously
- Risk of incompatibility

---

## Why Separate Retry and DLT Topics?

### Retry Topics

**Purpose:** Handle transient failures with delayed retry.

**Benefits:**
- Separate retention policy (shorter)
- Different monitoring/alerting
- Isolate problematic messages
- Exponential backoff possible

**Example Flow:**
```
orders.v1 → Payment Service fails → orders.retry.v1
orders.retry.v1 → Wait → Retry → Success → Done
orders.retry.v1 → Wait → Retry → Fail → orders.retry.v1 (again)
```

### Dead Letter Topics (DLT)

**Purpose:** Isolate messages that fail permanently.

**Benefits:**
- Long retention for debugging
- Manual intervention possible
- Separate alerting
- Don't block main processing

**Example Flow:**
```
orders.retry.v1 → Max retries exceeded → orders.dlt.v1
orders.dlt.v1 → Manual review → Fix and reprocess or discard
```

---

## Partition Strategy

### Why 3 Partitions?

**Balance between:**
- Parallelism (more partitions = more consumers)
- Overhead (more partitions = more broker resources)
- Ordering (more partitions = less global ordering)

**Rule of Thumb:**
- Start with partitions = expected peak consumers
- Can increase later (requires topic recreation)
- Cannot decrease without data loss

### Message Key

**Orders:** `order_id`
- All events for same order go to same partition
- Guarantees ordering per order
- Enables efficient querying

**Payments:** `order_id`
- Payment events correlated with orders
- Same partition as corresponding order

**Why Not Random Keys?**
- No ordering guarantee
- Related events scattered across partitions
- Harder to debug and trace

---

## Topic Configuration

### Retention

```
Main topics: 7 days (168 hours)
Retry topics: 1 day
DLT topics: 30 days (for debugging)
```

### Compression

```
Producer: Snappy compression
- Good balance of speed and compression ratio
- Reduces network bandwidth
- Reduces storage
```

### Batch Size

```
Producer batch size: 100 messages
Batch timeout: 1 second
```

---

## Inspecting Topics

### List Topics
```bash
make topics
```

### Describe Topic
```bash
make topic-describe TOPIC=orders.v1
```

Output:
```
Topic: orders.v1  PartitionCount: 3
Topic: orders.v1  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
Topic: orders.v1  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
Topic: orders.v1  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

### Consume Messages
```bash
docker exec kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.v1 \
  --from-beginning \
  --max-messages 10 \
  --property print.key=true \
  --property print.headers=true \
  --property print.timestamp=true
```

### Check Consumer Lag
```bash
make consumer-group-describe GROUP=payment-service
```

---

## Topic Creation

Topics are created automatically on startup by the `topic-creator` service.

**Manual Creation:**
```bash
docker exec kafka-broker kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders.v1 \
  --partitions 3 \
  --replication-factor 1
```

**Cluster Mode (3 brokers):**
```bash
docker exec kafka-broker-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders.v1 \
  --partitions 3 \
  --replication-factor 3
```

---

## Monitoring Topics

### Kafka UI

Visit http://localhost:8080 to see:
- Topic list
- Partition details
- Message browser
- Consumer group offsets
- Consumer lag

### CLI

```bash
# List topics
make topics

# Describe topic
make topic-describe TOPIC=orders.v1

# List consumer groups
make consumer-groups

# Describe consumer group
make consumer-group-describe GROUP=payment-service
```

---

## Best Practices

1. **Version your topics** - Enable schema evolution
2. **Use meaningful names** - `orders.v1` not `topic1`
3. **Separate retry/DLT** - Isolate problematic messages
4. **Choose keys carefully** - Affects ordering and partitioning
5. **Monitor lag** - Detect processing issues early
6. **Set appropriate retention** - Balance storage and debugging needs
7. **Use compression** - Reduce bandwidth and storage
8. **Plan partition count** - Hard to change later
