# Failure Scenarios and Experiments

This document provides hands-on experiments to understand how the system behaves under various failure conditions.

---

## Experiment 1: Basic Flow

**Goal:** Observe the normal event flow.

**Steps:**
```bash
# 1. Start infrastructure
docker compose up -d

# 2. Start all services (in separate terminals)
make order
make payment
make notification

# 3. Create an order
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer-1",
    "items": [
      {"product_id": "product-1", "quantity": 2, "price": 100.00}
    ]
  }'
```

**Expected Flow:**
1. Order Service: Order created, outbox event inserted
2. Outbox Publisher: Event published to `orders.v1`
3. Payment Service: Consumed `order.created`, processed payment
4. Payment Service: Published `payment.completed` to `payments.v1`
5. Notification Service: Consumed both events, sent notifications

**Observe:**
- Logs in each service terminal
- Messages in Kafka UI (http://localhost:8080)
- Database records in each PostgreSQL instance

---

## Experiment 2: Multiple Consumer Groups

**Goal:** Verify both consumer groups receive the same event.

**Steps:**
```bash
# 1. Create an order (same as Experiment 1)

# 2. Check consumer groups
make consumer-groups

# 3. Describe each group
make consumer-group-describe GROUP=payment-service
make consumer-group-describe GROUP=notification-service
```

**Expected:**
- Both `payment-service` and `notification-service` groups exist
- Both groups show consumption from `orders.v1`
- Each group has independent offsets

**Observe in Kafka UI:**
- Consumer Groups section
- Both groups consuming from `orders.v1`
- Different offsets per group

---

## Experiment 3: More Consumers Than Partitions

**Goal:** Understand idle consumers.

**Setup:**
With 3 partitions and 5 consumers in one group.

**Steps:**
```bash
# 1. Start 3 payment service instances (modify ports in .env)
# Instance 1: PAYMENT_SERVICE_PORT=8082
# Instance 2: PAYMENT_SERVICE_PORT=8084
# Instance 3: PAYMENT_SERVICE_PORT=8085
# Instance 4: PAYMENT_SERVICE_PORT=8086
# Instance 5: PAYMENT_SERVICE_PORT=8087

# 2. Check consumer group
make consumer-group-describe GROUP=payment-service
```

**Expected:**
```
TOPIC      PARTITION  ASSIGNMENT
orders.v1  0          consumer-1
orders.v1  1          consumer-2
orders.v1  2          consumer-3
```

- 3 consumers assigned partitions
- 2 consumers idle (no partitions)

**Observe:**
- Logs showing partition assignments
- Only 3 consumers processing messages
- Idle consumers not receiving any messages

**Key Insight:**
Adding consumers beyond partition count provides no benefit.

---

## Experiment 4: Rebalancing

**Goal:** Observe partition reassignment.

**Steps:**
```bash
# 1. Start one payment service instance
# Observe logs:
# [consumer] assigned partition=0
# [consumer] assigned partition=1
# [consumer] assigned partition=2

# 2. Start second payment service instance
# Observe logs:
# [consumer-1] revoked partition=2
# [consumer-2] assigned partition=2

# 3. Stop first payment service
# Observe logs:
# [consumer-2] assigned partition=0
# [consumer-2] assigned partition=1
# [consumer-2] assigned partition=2
```

**Expected:**
- Partitions redistributed when consumers join/leave
- Brief pause in processing during rebalance
- All partitions eventually assigned

**Observe:**
- "Partition assigned" and "partition revoked" logs
- Consumer group state changes in Kafka UI

---

## Experiment 5: Duplicate Event (Idempotency)

**Goal:** Verify idempotency prevents duplicate processing.

**Steps:**
```bash
# 1. Create an order
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer-1",
    "items": [{"product_id": "product-1", "quantity": 1, "price": 100.00}]
  }'

# Note the order_id from response

# 2. Check payment was created
docker exec postgres-payment psql -U postgres -d payments_db \
  -c "SELECT * FROM payments WHERE order_id = '<order_id>';"

# Should show 1 payment record

# 3. Manually reset consumer offset to reprocess the message
make consumer-group-describe GROUP=payment-service
# Note the current offset for the partition

# Reset to earlier offset
docker exec kafka-broker kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group payment-service \
  --reset-offsets --to-offset <earlier-offset> \
  --topic orders.v1 \
  --execute

# 4. Observe logs
# Should see: "duplicate payment event detected - idempotency check"

# 5. Verify no duplicate payment
docker exec postgres-payment psql -U postgres -d payments_db \
  -c "SELECT COUNT(*) FROM payments WHERE order_id = '<order_id>';"

# Should still be 1
```

**Expected:**
- Same event processed twice
- Idempotency check detects duplicate
- Only ONE payment record in database

**Key Insight:**
At-least-once delivery requires idempotency to prevent duplicates.

---

## Experiment 6: Slow Consumer

**Goal:** Observe consumer lag.

**Steps:**
```bash
# 1. Enable slow consumer in payment service .env
SLOW_CONSUMER=true
SLOW_CONSUMER_DELAY=5s

# 2. Restart payment service

# 3. Create multiple orders rapidly
for i in {1..20}; do
  curl -X POST http://localhost:8081/orders \
    -H "Content-Type: application/json" \
    -d "{
      \"customer_id\": \"customer-$i\",
      \"items\": [{\"product_id\": \"product-1\", \"quantity\": 1, \"price\": 100.00}]
    }"
done

# 4. Check consumer lag
make consumer-group-describe GROUP=payment-service
```

**Expected:**
```
TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders.v1  0          5               10              5
orders.v1  1          4               8               4
orders.v1  2          3               9               6
```

- Lag increasing because consumer is slow
- Producer rate > consumer processing rate

**Observe:**
- Lag in Kafka UI
- Slow processing logs (5s delay per message)
- Messages queuing in partitions

---

## Experiment 7: Retry

**Goal:** Observe retry behavior.

**Steps:**
```bash
# 1. Create an order with amount > 10000 (will fail payment)
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer-1",
    "items": [{"product_id": "product-1", "quantity": 1, "price": 15000.00}]
  }'

# 2. Observe payment service logs
# Should see:
# "payment processing error"
# "sending to retry topic"

# 3. Check retry topic
docker exec kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic payments.retry.v1 \
  --from-beginning \
  --max-messages 5

# 4. Observe retry consumer logs
# Should see retry attempts with delays
```

**Expected:**
- Payment fails (amount > 10000)
- Event sent to `payments.retry.v1`
- Retry consumer picks up event
- Exponential backoff between retries
- After max retries, sent to DLT

---

## Experiment 8: Dead Letter Topic (DLT)

**Goal:** Observe DLT behavior after max retries.

**Steps:**
```bash
# 1. Create order with failing payment (same as Experiment 7)

# 2. Wait for max retries (default 3)

# 3. Check DLT topic
docker exec kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic payments.dlt.v1 \
  --from-beginning \
  --max-messages 5 \
  --property print.key=true \
  --property print.headers=true

# 4. Observe DLT message structure
```

**Expected:**
DLT message contains:
```json
{
  "event_id": "original-event-id",
  "event_type": "order.created.dlt",
  "data": {
    "original_topic": "orders.v1",
    "partition": 0,
    "offset": 42,
    "error": "payment processing failed",
    "retry_count": 3,
    "original_payload": "{...}",
    "timestamp": "2024-01-15T10:30:00Z"
  }
}
```

**Observe:**
- Message in DLT after max retries
- Contains debugging information
- Original payload preserved

---

## Experiment 9: Ordering

**Goal:** Verify ordering within partition.

**Steps:**
```bash
# 1. Create an order
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer-1",
    "items": [{"product_id": "product-1", "quantity": 1, "price": 100.00}]
  }'

# Note the order_id

# 2. Check which partition the order went to
docker exec kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.v1 \
  --property print.key=true \
  --property print.partition=true \
  --max-messages 10

# 3. Create multiple events for same order_id
# (Would need update/cancel endpoints for full demo)

# 4. Verify all events for same order_id go to same partition
```

**Expected:**
- Same `order_id` key → same partition
- Events processed in order within that partition

**Key Insight:**
Kafka ordering is per-partition, not global.

---

## Experiment 10: Outbox Pattern

**Goal:** Verify outbox ensures consistency.

**Steps:**
```bash
# 1. Check outbox table before creating order
docker exec postgres-order psql -U postgres -d orders_db \
  -c "SELECT * FROM outbox_events ORDER BY created_at DESC LIMIT 5;"

# 2. Create an order
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer-1",
    "items": [{"product_id": "product-1", "quantity": 1, "price": 100.00}]
  }'

# 3. Check outbox table immediately after
docker exec postgres-order psql -U postgres -d orders_db \
  -c "SELECT * FROM outbox_events ORDER BY created_at DESC LIMIT 5;"

# Should see new unprocessed event

# 4. Wait for outbox publisher
# Check again - event should be marked processed

# 5. Verify message in Kafka
docker exec kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.v1 \
  --from-beginning \
  --max-messages 1
```

**Expected:**
- Order and outbox event created in same transaction
- Outbox publisher sends to Kafka
- Event marked as processed
- Message appears in Kafka topic

**Key Insight:**
Outbox pattern ensures database and Kafka are eventually consistent.

---

## Experiment 11: Consumer Scaling

**Goal:** Compare throughput with different configurations.

**Configurations:**
1. 3 partitions / 1 consumer
2. 3 partitions / 3 consumers
3. 3 partitions / 5 consumers

**Steps:**
```bash
# For each configuration:

# 1. Start appropriate number of consumers

# 2. Create 100 orders rapidly
for i in {1..100}; do
  curl -X POST http://localhost:8081/orders \
    -H "Content-Type: application/json" \
    -d "{
      \"customer_id\": \"customer-$i\",
      \"items\": [{\"product_id\": \"product-1\", \"quantity\": 1, \"price\": 100.00}]
    }" &
done
wait

# 3. Measure time to process all messages
# 4. Check consumer lag
make consumer-group-describe GROUP=payment-service
```

**Expected Results:**

| Configuration | Throughput | Notes |
|---------------|-----------|-------|
| 3 parts / 1 consumer | Baseline | Sequential processing |
| 3 parts / 3 consumers | ~3x faster | Parallel processing |
| 3 parts / 5 consumers | Same as 3/3 | 2 consumers idle |

**Key Insight:**
More consumers only help up to partition count.

---

## Experiment 12: Broker Failure (Cluster Mode)

**Goal:** Observe replication and failover.

**Setup:**
```bash
# Use 3-broker cluster
docker compose -f docker-compose.cluster.yml up -d
```

**Steps:**
```bash
# 1. Check topic replication
docker exec kafka-broker-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic orders.v1

# Should show:
# Leader: 1  Replicas: 1,2,3  Isr: 1,2,3

# 2. Create some orders

# 3. Stop one broker
docker stop kafka-broker-2

# 4. Check topic status
docker exec kafka-broker-1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic orders.v1

# Should show:
# Leader: 1  Replicas: 1,2,3  Isr: 1,3

# 5. Verify services still work
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer-1",
    "items": [{"product_id": "product-1", "quantity": 1, "price": 100.00}]
  }'

# 6. Restart broker
docker start kafka-broker-2

# 7. Wait for ISR to recover
# Check topic status again
```

**Expected:**
- Leader election occurs when broker stops
- Service continues with remaining brokers
- ISR shrinks, then recovers when broker returns

**Key Insight:**
Replication provides fault tolerance.

---

## Summary

These experiments demonstrate:

1. **Basic flow** - Event-driven architecture in action
2. **Consumer groups** - Independent processing
3. **Partition assignment** - How consumers get work
4. **Rebalancing** - Dynamic partition assignment
5. **Idempotency** - Handling duplicates
6. **Consumer lag** - Backpressure visualization
7. **Retry** - Handling transient failures
8. **DLT** - Handling permanent failures
9. **Ordering** - Per-partition guarantees
10. **Outbox** - Reliable event publishing
11. **Scaling** - Partition-consumer relationship
12. **Replication** - Fault tolerance

Each experiment builds understanding of Kafka concepts through real behavior.
