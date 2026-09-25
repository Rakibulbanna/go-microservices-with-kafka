# Troubleshooting Guide

## Common Issues

### Infrastructure

#### Kafka Won't Start

**Symptom:**
```
kafka-broker | ERROR Fatal error during KafkaServer startup
```

**Solution:**
```bash
# Check if port is already in use
lsof -i :9092
lsof -i :9093

# Kill existing process or change port
docker compose down
docker compose up -d
```

#### Topics Not Created

**Symptom:**
```bash
make topics
# Returns empty list
```

**Solution:**
```bash
# Check topic-creator logs
docker compose logs topic-creator

# Manually create topics
docker exec kafka-broker kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders.v1 --partitions 3 --replication-factor 1
```

#### PostgreSQL Connection Refused

**Symptom:**
```
failed to connect to database: dial tcp 127.0.0.1:5432: connect: connection refused
```

**Solution:**
```bash
# Check PostgreSQL is running
docker compose ps postgres-order

# Check logs
docker compose logs postgres-order

# Restart
docker compose restart postgres-order
```

### Services

#### Service Won't Start

**Symptom:**
```
panic: failed to connect to database
```

**Solution:**
1. Ensure infrastructure is running: `docker compose ps`
2. Check environment variables match `.env.example`
3. Verify database is healthy: `docker compose logs postgres-order`

#### Messages Not Being Consumed

**Symptom:**
Messages appear in topic but consumer doesn't process them.

**Diagnosis:**
```bash
# Check consumer group status
make consumer-group-describe GROUP=payment-service

# Check consumer logs
docker compose logs payment-service | grep "consumer started"
```

**Possible Causes:**
1. Consumer not started
2. Consumer crashed
3. Consumer stuck (check for errors in logs)
4. Wrong consumer group ID

#### Duplicate Processing

**Symptom:**
Same payment processed multiple times.

**Expected Behavior:**
This is normal with at-least-once delivery. The idempotency check should prevent duplicate charges.

**Check:**
```bash
# Look for idempotency logs
docker compose logs payment-service | grep "duplicate"
```

If duplicates are still being charged, check the idempotency implementation.

#### High Consumer Lag

**Symptom:**
Consumer lag keeps increasing in Kafka UI.

**Diagnosis:**
```bash
make consumer-group-describe GROUP=payment-service
```

**Solutions:**
1. **Add consumers** (up to partition count)
2. **Optimize processing** - reduce per-message time
3. **Increase partitions** - requires topic recreation
4. **Enable batching** - process multiple messages at once
5. **Reduce unnecessary work** - skip non-essential operations

**Note:** Adding consumers beyond partition count won't help.

### Kafka UI

#### Can't Connect to Kafka

**Symptom:**
Kafka UI shows "Connection refused" or no clusters.

**Solution:**
```bash
# Check Kafka is healthy
docker compose ps kafka

# Restart Kafka UI
docker compose restart kafka-ui

# Check environment variables
docker compose logs kafka-ui | grep KAFKA
```

#### Messages Not Showing

**Symptom:**
Topic exists but messages don't appear in UI.

**Solution:**
1. Refresh the page
2. Check message retention (default 7 days)
3. Verify messages are being produced (check service logs)

---

## Debugging Techniques

### Enable Debug Logging

Set in `.env`:
```
LOG_LEVEL=debug
```

### Follow Messages Across Services

Use `correlation_id` to trace an event:

```bash
# Create order
curl -X POST http://localhost:8081/orders -d '{...}'

# Note the correlation_id from response

# Search logs
docker compose logs -f | grep <correlation_id>
```

### Inspect Kafka Messages

```bash
# Consume from topic
docker exec kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.v1 \
  --from-beginning \
  --max-messages 10

# With headers
docker exec kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.v1 \
  --property print.headers=true \
  --property print.key=true \
  --property print.timestamp=true
```

### Check Consumer Group Offsets

```bash
make consumer-group-describe GROUP=payment-service
```

Output:
```
TOPIC      PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
orders.v1  0          42              45              3
orders.v1  1          38              40              2
orders.v1  2          40              40              0
```

### Reset Consumer Group Offset

**Warning:** This will cause reprocessing of messages.

```bash
# Reset to earliest
docker exec kafka-broker kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group payment-service \
  --reset-offsets --to-earliest \
  --topic orders.v1 \
  --execute

# Reset to latest
docker exec kafka-broker kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group payment-service \
  --reset-offsets --to-latest \
  --topic orders.v1 \
  --execute
```

---

## Performance Issues

### Slow Producer

**Symptoms:**
- High latency on POST /orders
- Messages queuing up in producer

**Solutions:**
1. Increase batch size
2. Reduce acks (acks=1 instead of acks=all)
3. Enable compression
4. Check network latency

### Slow Consumer

**Symptoms:**
- Consumer lag increasing
- Messages backing up in partitions

**Solutions:**
1. Optimize processing logic
2. Add more consumers (up to partition count)
3. Increase partitions
4. Enable batching
5. Reduce database query time

### High Memory Usage

**Symptoms:**
- Service using excessive memory
- OOM kills

**Solutions:**
1. Reduce consumer fetch size (MaxBytes)
2. Reduce batch size
3. Check for memory leaks
4. Increase container memory limits

---

## Data Issues

### Messages in Wrong Order

**Symptom:**
Events for same order processed out of order.

**Expected:**
Kafka guarantees ordering within a partition, not globally.

**Check:**
```bash
# Verify same key goes to same partition
docker exec kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.v1 \
  --property print.key=true \
  --property print.partition=true
```

### Missing Messages

**Symptom:**
Expected messages not found in topic.

**Possible Causes:**
1. Producer failed to send (check logs)
2. Message retention expired (default 7 days)
3. Wrong topic name
4. Outbox publisher not running

**Check:**
```bash
# Check outbox events
docker exec postgres-order psql -U postgres -d orders_db \
  -c "SELECT * FROM outbox_events ORDER BY created_at DESC LIMIT 10;"
```

### Duplicate Messages in Database

**Symptom:**
Multiple payment records for same event.

**Check:**
```bash
docker exec postgres-payment psql -U postgres -d payments_db \
  -c "SELECT event_id, COUNT(*) FROM payments GROUP BY event_id HAVING COUNT(*) > 1;"
```

If duplicates exist, the idempotency check is not working correctly.

---

## Recovery Procedures

### Service Crash Recovery

1. Service automatically restarts (if using Docker)
2. Consumer resumes from last committed offset
3. No messages lost (at-least-once delivery)

### Kafka Broker Failure

**Single Broker:**
- Service unavailable until broker recovers
- Producers/consumers will retry

**Cluster (3-broker):**
- Leader election occurs automatically
- Service continues with remaining brokers
- Check ISR status after recovery

### Database Failure

1. Service will fail to start
2. Fix database issue
3. Restart service
4. Outbox events will be published on recovery

### Consumer Group Rebalance Storm

**Symptom:**
Constant rebalancing in logs.

**Causes:**
- Consumers crashing repeatedly
- Network instability
- Session timeout too low

**Solutions:**
1. Increase `session.timeout.ms`
2. Increase `heartbeat.interval.ms`
3. Fix underlying consumer crashes
4. Check network connectivity

---

## Getting Help

1. Check logs: `docker compose logs -f <service>`
2. Check Kafka UI: http://localhost:8080
3. Review this troubleshooting guide
4. Check `docs/kafka-concepts.md` for concept explanations
5. Review service code for implementation details
