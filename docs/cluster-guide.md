# 3-Broker Kafka Cluster Guide

This document explains what happens inside the 3-broker Kafka cluster and how to observe it.

---

## When to Use the Cluster

| Setup | When to Use |
|-------|------------|
| `docker compose up -d` (single broker) | Learning basics: consumer groups, offsets, partitions, retry/DLT, outbox |
| `make cluster-up` (3 brokers) | Learning: replication, ISR, leader election, fault tolerance, broker failure |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│              KRaft Controller Quorum                     │
│         (Brokers 1, 2, 3 vote on metadata)              │
└─────────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
   ┌─────────┐      ┌─────────┐      ┌─────────┐
   │Broker 1 │      │Broker 2 │      │Broker 3 │
   │ :9092   │      │ :9082   │      │ :9072   │
   └─────────┘      └─────────┘      └─────────┘
```

Each broker acts as both:
- **Broker**: Handles produce/consume requests, stores data
- **Controller**: Participates in KRaft metadata quorum (no ZooKeeper needed)

---

## Key Concepts

### Leader

The broker that handles all reads and writes for a given partition.

```
orders.v1 Partition 0 → Leader: Broker 3
orders.v1 Partition 1 → Leader: Broker 1
orders.v1 Partition 2 → Leader: Broker 2
```

Producers and consumers talk to the **leader** for each partition.

### Replicas

All brokers that hold a copy of a partition's data.

```
orders.v1 Partition 0 → Replicas: 3,1,2
```

This means Broker 3, 1, and 2 all have a copy of partition 0.

### ISR (In-Sync Replicas)

The subset of replicas that are **up-to-date** with the leader.

```
Normal:    ISR = [3,1,2] → all 3 brokers are in sync
Failure:   ISR = [1,2]   → Broker 3 is down or behind
```

If a broker falls behind (slow disk, network issue), it is removed from ISR.

### min.insync.replicas

Configured as `2` in this cluster.

```
acks=all + min.insync.replicas=2

→ At least 2 replicas must acknowledge a write
→ If only 1 broker is in ISR, writes are REJECTED
→ Prevents data loss
```

| ISR Size | Write Succeeds? | Why |
|----------|----------------|-----|
| 3 | Yes | 3 >= 2 |
| 2 | Yes | 2 >= 2 |
| 1 | No | 1 < 2, too risky |

---

## How Data Flows

### Producing a Message

```
1. Producer sends message to Leader (Broker 3, partition 0)
         │
         ▼
2. Broker 3 writes to local disk
         │
         ▼
3. Broker 3 replicates to Broker 1 and Broker 2 (followers)
         │
         ▼
4. All ISR replicas acknowledge
         │
         ▼
5. Producer receives success response
```

### What Happens When a Broker Fails

```
Before:
  Partition 0: Leader=3  ISR=[3,1,2]

Broker 3 crashes:
  Partition 0: Leader=1  ISR=[1,2]
  → Broker 1 becomes new leader
  → No data loss (Brokers 1 and 2 have copies)
  → Clients automatically reconnect to Broker 1

Broker 3 recovers:
  Partition 0: Leader=1  ISR=[1,2,3]
  → Broker 3 catches up and rejoins ISR
```

---

## Observing the Cluster

### 1. Topic Description

```bash
make cluster-topic-describe TOPIC=orders.v1
```

Output:
```
Topic: orders.v1  PartitionCount:3  ReplicationFactor:3  min.insync.replicas=2
  Partition: 0  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  Partition: 1  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 2  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
```

**Read this as:**
- 3 partitions, each replicated to all 3 brokers
- Leaders distributed evenly (one per broker)
- All replicas in sync

### 2. List All Topics

```bash
make cluster-topics
```

### 3. Consumer Group Status

```bash
make cluster-consumer-group-describe GROUP=payment-service
```

### 4. Log Dirs (See What Each Broker Stores)

```bash
docker exec kafka-broker-1 /opt/kafka/bin/kafka-log-dirs.sh \
  --bootstrap-server localhost:9092 \
  --describe --broker-list 1,2,3
```

You'll see every broker holds **all 21 partitions** (7 topics x 3 partitions).

### 5. Broker API Versions

```bash
docker exec kafka-broker-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

---

## Hands-On Experiments

### Experiment 1: Normal State

```bash
# Check all topics
make cluster-topics

# Describe a topic
make cluster-topic-describe TOPIC=orders.v1

# Note: all ISR = [3,1,2], leaders distributed
```

### Experiment 2: Broker Failure

```bash
# Step 1: Check current state
make cluster-topic-describe TOPIC=orders.v1

# Step 2: Kill Broker 3
docker stop kafka-broker-3

# Step 3: Wait a few seconds, then check again
make cluster-topic-describe TOPIC=orders.v1
```

**What you'll see:**
```
Partition 0: Leader=1  Replicas=3,1,2  Isr=1,2
Partition 1: Leader=1  Replicas=1,2,3  Isr=1,2
Partition 2: Leader=2  Replicas=2,3,1  Isr=2,1
```

**Observations:**
- Broker 3 removed from ISR
- Leaders reassigned to surviving brokers
- Data still available (Brokers 1 and 2 have copies)

### Experiment 3: Produce During Failure

```bash
# Broker 3 is still stopped

# Start your services (point to broker 1)
# Create an order
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"c1","items":[{"product_id":"p1","quantity":1,"price":100}]}'

# It works! Because 2 brokers are still in ISR (>= min.insync.replicas=2)
```

### Experiment 4: Two Brokers Fail

```bash
# Stop Broker 2 and 3
docker stop kafka-broker-2
docker stop kafka-broker-3

# Try to create an order
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"c1","items":[{"product_id":"p1","quantity":1,"price":100}]}'

# It FAILS! Only 1 broker in ISR (< min.insync.replicas=2)
# Kafka rejects writes to prevent data loss
```

### Experiment 5: Recovery

```bash
# Restart all brokers
docker start kafka-broker-2
docker start kafka-broker-3

# Wait 30 seconds for recovery

# Check ISR is back to full
make cluster-topic-describe TOPIC=orders.v1

# All ISR = [3,1,2] again
# Writes succeed again
```

### Experiment 6: Leader Re-election

```bash
# Check current leaders
make cluster-topic-describe TOPIC=orders.v1

# Note which broker leads partition 0 (e.g., Broker 3)

# Stop that broker
docker stop kafka-broker-3

# Check again
make cluster-topic-describe TOPIC=orders.v1

# Partition 0 now has a different leader (e.g., Broker 1)
# This is automatic leader election
```

---

## What Each Broker Stores

All 3 brokers store **all partitions** (replication factor = 3):

```
Broker 1: orders.v1-[0,1,2], payments.v1-[0,1,2], notifications.v1-[0,1,2], ...
Broker 2: orders.v1-[0,1,2], payments.v1-[0,1,2], notifications.v1-[0,1,2], ...
Broker 3: orders.v1-[0,1,2], payments.v1-[0,1,2], notifications.v1-[0,1,2], ...
```

This means:
- **21 partitions per broker** (7 topics x 3 partitions)
- Any single broker failure = no data loss
- Load distributed across all brokers

---

## KRaft Quorum

In KRaft mode, the 3 brokers form a **metadata quorum**:

```
Broker 1 = voter
Broker 2 = voter
Broker 3 = voter

Quorum = majority = 2 out of 3
```

**What the quorum does:**
- Manages cluster metadata (topics, partitions, leaders)
- Elects leaders when brokers fail
- Requires majority agreement for metadata changes

**Why 3 brokers?**
- Tolerates 1 failure (2 out of 3 = majority)
- Cannot tolerate 2 failures (1 out of 3 = no majority)

---

## Comparison: Single Broker vs Cluster

| Feature | Single Broker | 3-Broker Cluster |
|---------|--------------|-----------------|
| Replication | None (factor=1) | Full (factor=3) |
| Fault tolerance | None | Survives 1 broker failure |
| ISR | N/A | Tracks in-sync replicas |
| Leader election | N/A | Automatic on failure |
| min.insync.replicas | 1 | 2 |
| Data safety | Low | High |
| Resource usage | Low | High (3 JVMs) |
| Use case | Development | Production learning |

---

## Troubleshooting

### Broker Shows Unhealthy

```bash
# Check broker logs
docker logs kafka-broker-1 --tail 50

# Check health status
docker inspect kafka-broker-1 --format='{{json .State.Health}}' | jq
```

### ISR Shrinks

If a broker drops out of ISR:
1. Check if the broker is running: `docker ps`
2. Check broker logs: `docker logs kafka-broker-X`
3. Wait for it to catch up (ISR will grow back)

### Can't Connect to Cluster

```bash
# Test connectivity to each broker
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka-broker-2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka-broker-3 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

## Summary

The 3-broker cluster demonstrates:

1. **Replication**: Every partition exists on all 3 brokers
2. **Leader election**: Automatic failover when a broker dies
3. **ISR**: Tracks which brokers are up-to-date
4. **Quorum**: KRaft metadata management without ZooKeeper
5. **min.insync.replicas**: Prevents writes when too many brokers are down

This is how production Kafka ensures **durability** and **availability**.
