# Kafka Implementation Patterns

This guide compares all the Kafka patterns implemented in this project.

---

## Overview

| Solution | Pattern | Database | Use Case | Services |
|----------|---------|----------|----------|----------|
| **A** | Pure Kafka | None | Event forwarding, analytics | Analytics Service |
| **B** | Outbox Pattern | PostgreSQL | Business transactions | Order Service |
| **C** | Consumer with DB | PostgreSQL | Event processing | Payment, Notification |

---

## Solution A: Pure Kafka (No Database)

### Architecture

```
Producer → Kafka → Consumer → Kafka
   (no DB)          (no DB)
```

### Implementation

**Analytics Service:**
- Consumes from `orders.v1`
- Transforms events in memory
- Publishes to `analytics.v1`
- **NO database at all**

### Code Example

```go
// Consume
msg, _ := reader.FetchMessage(ctx)

// Transform (in memory, no DB)
analyticsEvent := transformer.TransformOrderCreated(envelope, orderData)

// Publish
writer.WriteMessages(ctx, kafkago.Message{
    Topic: "analytics.v1",
    Key:   []byte(orderData.OrderID),
    Value: marshal(analyticsEvent),
})

// Commit
reader.CommitMessages(ctx, msg)
```

### When to Use

✅ Event forwarding/transformation  
✅ Stateless processing  
✅ Real-time analytics  
✅ Maximum simplicity  

❌ Business transactions  
❌ Data persistence needed  
❌ Idempotency required  

**Documentation:** [Solution A: Pure Kafka](./solution-a-pure-kafka.md)

---

## Solution B: Outbox Pattern

### Architecture

```
┌─────────────────────────────┐
│    Single DB Transaction    │
│                             │
│  1. INSERT INTO orders      │
│  2. INSERT INTO outbox      │
│  3. COMMIT (atomic)         │
└─────────────────────────────┘
           ↓
   Outbox Publisher (async)
           ↓
        Kafka Topic
```

### Implementation

**Order Service:**
- Receives HTTP request
- Saves order + outbox event in **same transaction**
- Outbox publisher polls and publishes to Kafka
- **Guaranteed delivery**

### Code Example

```go
// Start transaction
tx, _ := db.BeginTx(ctx, nil)

// Save business data
tx.Exec("INSERT INTO orders ...")

// Save outbox event
tx.Exec("INSERT INTO outbox_events ...")

// Commit both atomically
tx.Commit()

// Later: Outbox publisher sends to Kafka
```

### When to Use

✅ Business transactions  
✅ Need atomicity (DB + Kafka)  
✅ Guaranteed delivery  
✅ Data consistency critical  

❌ Simple event forwarding  
❌ Maximum performance needed  
❌ Stateless processing  

**Documentation:** [Outbox Pattern](./outbox-pattern.md)

---

## Solution C: Consumer with Database

### Architecture

```
Kafka → Consumer → Process → DB
                  ↓
              Commit Offset
```

### Implementation

**Payment Service:**
- Consumes from `orders.v1`
- Processes payment
- Saves to `payments` table
- Commits offset

**Notification Service:**
- Consumes from `orders.v1` and `payments.v1`
- Sends notifications
- Saves to `notifications` table
- Commits offset

### Code Example

```go
// Consume
msg, _ := reader.FetchMessage(ctx)

// Process
payment := processPayment(msg)

// Save to DB
db.Insert(payment)

// Commit offset
reader.CommitMessages(ctx, msg)
```

### When to Use

✅ Need to save processed state  
✅ Event processing with persistence  
✅ Query historical data  
✅ Maintain processing state  

❌ Simple forwarding  
❌ Stateless transformations  

---

## Comparison Table

| Aspect | Solution A | Solution B | Solution C |
|--------|-----------|-----------|-----------|
| **Database** | None | PostgreSQL | PostgreSQL |
| **Complexity** | Very Low | Medium | Low |
| **Reliability** | At-least-once | Guaranteed | At-least-once |
| **Idempotency** | Not needed | Required | Required |
| **Atomicity** | N/A | DB + Kafka | DB only |
| **Performance** | Highest | Medium | High |
| **Use Case** | Forwarding | Transactions | Processing |
| **Example** | Analytics | Order | Payment |

---

## Data Flow in Our System

```
┌─────────────────────────────────────────────────────────────┐
│                    SOLUTION B: OUTBOX                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  HTTP Request → Order Service                              │
│         ↓                                                   │
│  BEGIN TRANSACTION                                          │
│    INSERT INTO orders (business data)                      │
│    INSERT INTO outbox_events (event)                       │
│  COMMIT                                                     │
│         ↓                                                   │
│  Outbox Publisher (async)                                  │
│         ↓                                                   │
│  orders.v1 ─────────────────────────────────────┐          │
│                                                 │          │
└─────────────────────────────────────────────────┼──────────┘
                                                  │
                    ┌─────────────────────────────┼─────────┐
                    │                             │         │
                    ▼                             ▼         │
        ┌───────────────────┐         ┌──────────────────┐ │
        │ SOLUTION A: PURE  │         │ SOLUTION C: DB   │ │
        │     KAFKA         │         │                  │ │
        ├───────────────────┤         ├──────────────────┤ │
        │                   │         │                  │ │
        │ Analytics Service │         │ Payment Service  │ │
        │   (no DB)         │         │   (with DB)      │ │
        │         ↓         │         │         ↓        │ │
        │  analytics.v1     │         │  payments.v1     │ │
        │                   │         │                  │ │
        └───────────────────┘         └──────────────────┘ │
                    │                             │         │
                    └─────────────────────────────┼─────────┘
                                                  │
                                                  ▼
                                      ┌──────────────────┐
                                      │ SOLUTION C: DB   │
                                      ├──────────────────┤
                                      │ Notification     │
                                      │ Service (DB)     │
                                      │         ↓        │
                                      │ notifications.v1 │
                                      └──────────────────┘
```

---

## How to Run Each Solution

### Solution A: Analytics Service

```bash
# Terminal 1
make analytics

# Terminal 2 - Create order
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"c1","items":[{"product_id":"p1","quantity":2,"price":100}]}'

# Terminal 3 - Watch analytics.v1
docker exec kafka-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic analytics.v1 \
  --from-beginning
```

### Solution B: Order Service (Outbox)

```bash
# Terminal 1
make order

# Terminal 2 - Create order
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"c1","items":[{"product_id":"p1","quantity":2,"price":100}]}'

# Terminal 3 - Check outbox table
docker exec postgres-order psql -U postgres -d orders_db \
  -c "SELECT * FROM outbox_events ORDER BY created_at DESC LIMIT 5;"

# Terminal 4 - Watch orders.v1
docker exec kafka-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.v1 \
  --from-beginning
```

### Solution C: Payment Service

```bash
# Terminal 1
make payment

# Terminal 2 - Create order (triggers payment processing)
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"c1","items":[{"product_id":"p1","quantity":2,"price":100}]}'

# Terminal 3 - Check payments table
docker exec postgres-payment psql -U postgres -d payments_db \
  -c "SELECT * FROM payments ORDER BY created_at DESC LIMIT 5;"
```

---

## Learning Path

1. **Start with Solution A** (Analytics Service)
   - Understand basic Kafka flow
   - Learn consume → transform → produce
   - No database complexity

2. **Move to Solution C** (Payment Service)
   - Add database persistence
   - Learn offset management
   - Understand idempotency

3. **Finally Solution B** (Order Service)
   - Understand dual-write problem
   - Learn Outbox pattern
   - Master atomic transactions

---

## Summary

| Solution | Pattern | Complexity | Reliability | Use When |
|----------|---------|------------|-------------|----------|
| **A** | Pure Kafka | ⭐ | ⭐⭐ | Simple forwarding |
| **B** | Outbox | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Business transactions |
| **C** | Consumer + DB | ⭐⭐ | ⭐⭐⭐⭐ | Event processing |

**Key Takeaway:** Choose the simplest solution that meets your requirements. Start with Solution A, add complexity only when needed.
