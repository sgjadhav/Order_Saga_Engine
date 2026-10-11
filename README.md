# Distributed Event-Driven Order Processing System

> *An asynchronous, production-hardened distributed backend demonstrating event-driven choreography, strict distributed consistency primitives, and concurrency control.*

An enterprise-grade, event-driven microservices architecture built with **FastAPI, PostgreSQL, and Redpanda** (Kafka protocol).

The system addresses fundamental distributed systems challenges: eliminating the **Dual-Write Vulnerability** across all participating services via the **Transactional Outbox Pattern**, mitigating race conditions through **Pessimistic Row-Level Locking**, ensuring message ordering with **Partition Keys**, and guaranteeing eventual consistency using a **Choreographed Saga with Compensating Rollbacks**.

---

## Architectural Blueprint

The platform utilizes three fully isolated microservices with dedicated databases per service, orchestrated asynchronously via Redpanda log streaming.

```text
                           [ Client / API Gateway ]
                                      │
                                      │ POST /orders
                                      ▼
                        ┌───────────────────────────┐
                        │       Order Service       │ (Port: 8000)
                        └─────────────┬─────────────┘
                                      │ ACID Transaction (Single DB Commit)
                        ┌─────────────┴─────────────┐
                        ▼                           ▼
                   Order Record               Outbox Message
                    [PENDING]                    [PENDING]
                        └─────────────┬─────────────┘
                                      ▼ (SELECT ... FOR UPDATE SKIP LOCKED)
                        ┌───────────────────────────┐
                        │   Order Relay Worker      │
                        └─────────────┬─────────────┘
                                      │ key=order_id (aiokafka)
                                      ▼
=========================== [ REDPANDA (KAFKA) ] ===========================
    Topics: order-events (keyed) | inventory-events (keyed) | payment-events (keyed)
============================================================================
              │                                      ▲
              │ Consumes 'OrderCreated'              │ key=order_id
              ▼                                      │
┌───────────────────────────┐          ┌───────────────────────────┐
│     Inventory Service     │          │  Inventory Relay Worker   │
│ • SELECT ... FOR UPDATE   │          │ (SKIP LOCKED batch poller)│
│ • Atomic ProcessedEvent   │          └─────────────▲─────────────┘
│ • Atomic Outbox Message   │────────────────────────┘
└─────────────┬─────────────┘  ACID Commit (Stock + Dedup + Outbox)
              │
              │ Consumes 'InventoryReserved'
              ▼
┌───────────────────────────┐          ┌───────────────────────────┐
│      Payment Service      │          │   Payment Relay Worker    │
│ • Dynamic Amount Eval     │          │ (SKIP LOCKED batch poller)│
│ • Atomic ProcessedEvent   │          └─────────────▲─────────────┘
│ • Atomic Outbox Message   │────────────────────────┘
└─────────────┬─────────────┘  ACID Commit (Payment + Dedup + Outbox)
              │
              ├──► [SUCCESS] ──► Emits 'PaymentCompleted' ──► Order CONFIRMED
              │
              └──► [FAILURE] ──► Emits 'PaymentFailed'
                                        │
                                        ├──► Inventory Restores Stock (+N)
                                        └──► Order CANCELLED
```

---

## Distributed Consistency & Resilience Invariants

### 1. Ubiquitous Transactional Outbox (Zero Dual-Write Window)
In a naive distributed setup, writing to a database and then emitting an event over the network creates a crash window where data persists but events are permanently lost.
* **Implementation:** `order_service`, `inventory_service`, and `payment_service` all write their state changes, deduplication records, and outgoing events into their local `outbox` table within the **exact same ACID database transaction** (`async with session.begin():`).
* **Guarantee:** If a service or container crashes, un-emitted events remain safely committed on disk and are dispatched upon worker restart.

### 2. Multi-Instance Safe Scaling (`SELECT ... FOR UPDATE SKIP LOCKED`)
Outbox relay workers can safely scale out across multiple container replicas without race conditions or duplicate publish storms:
```sql
SELECT * FROM outbox 
WHERE processed = 'PENDING' 
ORDER BY id 
FOR UPDATE SKIP LOCKED 
LIMIT 10;
```
Each worker locks its batch exclusively while allowing concurrent workers to immediately grab subsequent pending records.

### 3. Strict Per-Order Partition Key Ordering
Distributed event logs with multiple partitions can deliver events out-of-order if published without a partition key.
* **Implementation:** Every relay worker explicitly publishes messages using the order identifier as the Kafka partition key:
```python
key = str(payload["order_id"]).encode("utf-8")
await producer.send_and_wait(TOPIC, message_bytes, key=key)
```
* **Guarantee:** All lifecycle events for a specific order (`OrderCreated`, `InventoryReserved`, `PaymentFailed`) land on the exact same topic partition, ensuring deterministic consumer ordering.

### 4. Pessimistic Row-Level Locking (`with_for_update()`)
Under burst concurrency (e.g., flash sales), simultaneous requests could lead to race conditions, inventory overselling, and negative stock balances.
* **Implementation:** Inventory reservations execute row-level locks via SQLAlchemy's `.with_for_update()` before verifying and decrementing stock.
* **Guarantee:** Concurrent events queue deterministically at the database row level, preventing race conditions.

### 5. Atomic Consumer Idempotency & Deduplication
Because Kafka provides **at-least-once delivery guarantees**, network retries or consumer restarts can produce duplicate deliveries.
* **Implementation:** Incoming events are tracked in a dedicated `processed_events` table using unique primary key constraints on `event_id`. Duplicate deliveries trigger a DB-level `IntegrityError` which gracefully rolls back the session without duplicate execution.

### 6. Zero Message Loss (Manual Offset Commit)
Default broker auto-commits (`enable_auto_commit=True`) acknowledge messages before processing completes.
* **Implementation:** Auto-commit is disabled across all consumers. Manual offset acknowledgement (`await consumer.commit()`) is invoked strictly after local DB transactions succeed.

---

## End-to-End Choreographed Saga Flow

1. **Happy Path:**
```text
OrderCreated [PENDING] 
    ──► InventoryReserved [Stock Decremented] 
    ──► PaymentCompleted [Payment Recorded] 
    ──► Order CONFIRMED
```

2. **Compensating Rollback Path (e.g., Payment Limit Exceeded):**
```text
OrderCreated [PENDING] 
    ──► InventoryReserved [Stock Decremented] 
    ──► PaymentFailed [Threshold Exceeded] 
    ──► [Compensation Triggered] 
          ├── Inventory Service releases lock and restores stock (+N)
          └── Order Service updates status to CANCELLED
```

---

## Technology Stack

| Layer | Technology | Responsibility |
| :--- | :--- | :--- |
| **API Framework** | **FastAPI + Uvicorn** | High-performance asynchronous REST endpoints |
| **Concurrency** | **AsyncIO** | Non-blocking I/O across DB operations and event consumption |
| **Persistence** | **PostgreSQL 16 (Dedicated DBs)** | Isolated databases per microservice |
| **Async ORM & Driver** | **SQLAlchemy 2.0 Async + asyncpg** | Asynchronous session management and row locking |
| **Streaming Broker** | **Redpanda** | Ultra-low latency, Kafka-compatible distributed event log |
| **Kafka Client** | **aiokafka** | Asynchronous message production, manual offset management |
| **Containerization** | **Docker & Docker Compose** | Full infrastructure orchestration |

---

## Local Deployment & Verification

### 1. Prerequisites
* Docker & Docker Compose
* Python 3.12+

### 2. Start Infrastructure
Start the isolated PostgreSQL databases and Redpanda streaming broker:
```bash
docker compose up -d
```

### 3. Run Verification Tests
Verify row-level locking concurrency and atomic deduplication under simulated high-contention loads:
```bash
# Concurrency & overselling protection
pytest tests/test_inventory_concurrency.py

# Consumer duplicate delivery idempotency
pytest tests/test_consumer_idempotency.py
```
