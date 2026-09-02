# Distributed Event-Driven Order Processing System

> *Just learned and built something about how things work in backend systems. Yup, in tech, the sky is the limit — but that's what we try, and this is what I learned and built. If you clicked something I didn't, please do suggest, I do give credits😊*

An asynchronous, resilient distributed transaction backend built with **FastAPI, PostgreSQL, and Redpanda** (Kafka-compatible event streaming broker).

The platform implements core distributed consistency primitives — specifically the **Transactional Outbox Pattern** to prevent dual-write data corruption and a **Choreographed Saga with Compensating Transactions** to achieve eventual consistency across decoupled services.

---

## Architectural Blueprint

The system is built using **FastAPI + Uvicorn + AsyncIO** for asynchronous REST-based service execution, **PostgreSQL + SQLAlchemy Async + asyncpg** for service-specific persistence, and **Redpanda + aiokafka** for asynchronous event streaming. The entire distributed environment is containerized using **Docker and Docker Compose**.

```text
                           [ Client / API Gateway ]
                                      │
                                      │ POST /orders
                                      ▼
                        ┌───────────────────────────┐
                        │       Order Service       │
                        │      (Port: 8000)         │
                        │                           │
                        │ FastAPI + Uvicorn         │
                        │ AsyncIO                   │
                        └─────────────┬─────────────┘
                                      │
                         ACID Transaction Boundary
                              [PostgreSQL]
                    ┌─────────────┴─────────────┐
                    │                           │
                    ▼                           ▼
             Order Record                 Outbox Event
              [PENDING]                    [PENDING]
                    │                           │
                    └─────────────┬─────────────┘
                                  │
                                  │ SQLAlchemy Async
                                  │ + asyncpg
                                  ▼
                        ┌───────────────────────────┐
                        │    Outbox Relay Worker    │
                        │   Continuous Poller       │
                        │                           │
                        │ Python + AsyncIO          │
                        └─────────────┬─────────────┘
                                      │
                                      │ aiokafka
                                      │ Emits 'OrderCreated'
                                      ▼
=========================== [ REDPANDA ] ===========================
                         Kafka-Compatible Broker
      Topic: order-events │ inventory-events │ payment-events
====================================================================
            │                                                 ▲
            │ Consumes 'OrderCreated'                         │
            ▼                                                 │
┌───────────────────────────────┐                             │
│       Inventory Service       │                             │
│                               │                             │
│ FastAPI + AsyncIO             │                             │
│ PostgreSQL                    │                             │
│ SQLAlchemy Async + asyncpg    │                             │
│                               │                             │
│ Idempotent Deduplication      │                             │
│ Stock Decremented (-N)        │                             │
│ Emits 'InventoryReserved'     ┼─────────────────────────────┘
└───────────────┬───────────────┘
                │
                │ aiokafka
                │ Emits 'InventoryReserved'
                ▼
┌───────────────────────────────┐
│        Payment Service        │
│                               │
│ FastAPI + AsyncIO             │
│ PostgreSQL                    │
│ SQLAlchemy Async + asyncpg    │
│                               │
│ Validates Limits / Funds      │
└───────────────┬───────────────┘
                │
                ├───► Evaluation: SUCCESS
                │
                │     Emits 'PaymentCompleted'
                │             │
                │             ▼
                │     Order Service
                │     marks Order
                │     [CONFIRMED]
                │
                └───► Evaluation: FAILED
                      (Amount > Threshold)
                              │
                              ▼
                       Emits 'PaymentFailed'
                              │
                    ┌─────────┴──────────┐
                    │                    │
                    ▼                    ▼
            Inventory Service       Order Service
            Restores Stock (+N)    marks Order
                                   [CANCELLED]


       ┌────────────────────────────────────────────────────┐
       │              Infrastructure Layer                  │
       │                                                    │
       │ Docker + Docker Compose                            │
       │                                                    │
       │ Containerizes:                                     │
       │ • Order Service                                    │
       │ • Inventory Service                                │
       │ • Payment Service                                  │
       │ • PostgreSQL databases                             │
       │ • Redpanda broker                                  │
       │ • Outbox Relay Workers                             │
       └────────────────────────────────────────────────────┘
```

### Architecture Components

* **FastAPI + Uvicorn** — Provides the REST API layer and runs each service as an ASGI application.
* **AsyncIO** — Enables asynchronous request handling, database operations, and event consumption.
* **PostgreSQL 16** — Provides persistent, service-specific storage.
* **SQLAlchemy 2.0 Async + asyncpg** — Handles asynchronous ORM/database interaction.
* **Redpanda** — Acts as the Kafka-compatible distributed event streaming broker.
* **aiokafka** — Provides asynchronous Kafka-compatible event production and consumption.
* **Docker + Docker Compose** — Containerizes and orchestrates the distributed services and infrastructure.

### Distributed Consistency Flow

1. **Order Creation**
   The client sends `POST /orders` to the Order Service.

2. **Transactional Outbox**
   Within the same PostgreSQL ACID transaction:

   * The order is created with `PENDING` status.
   * An `OrderCreated` event is written to the Outbox table.

   This prevents the **dual-write problem**, where the database update succeeds but publishing the corresponding event fails.

3. **Outbox Relay**
   A continuously running worker polls pending Outbox events and publishes them to **Redpanda** using `aiokafka`.

4. **Inventory Reservation**
   Inventory Service consumes `OrderCreated`, validates stock, performs an idempotent stock decrement, and publishes `InventoryReserved`.

5. **Payment Processing**
   Payment Service consumes `InventoryReserved` and evaluates whether the payment can be completed.

6. **Successful Saga Path**

   ```text
   OrderCreated
       ↓
   InventoryReserved
       ↓
   PaymentCompleted
       ↓
   Order → CONFIRMED
   ```

7. **Failure & Compensation**

   ```text
   OrderCreated
       ↓
   InventoryReserved
       ↓
   PaymentFailed
       ↓
   ┌─────────────────────┐
   │ Compensation        │
   ├─────────────────────┤
   │ Restore Stock (+N)  │
   │ Order → CANCELLED   │
   └─────────────────────┘
   ```

   Instead of relying on a distributed database transaction, the system uses a **Choreographed Saga**, where services react to events and execute compensating transactions when a later step fails.

---

## Core Distributed-System Concepts Demonstrated

### 1. Transactional Outbox Pattern

The Order Service stores the business state change and the corresponding event in the **same PostgreSQL transaction**.

```text
BEGIN TRANSACTION

    INSERT Order
    INSERT Outbox Event

COMMIT
```

The event is then published asynchronously by the Outbox Relay.

This provides reliable event publication without requiring a distributed transaction between PostgreSQL and Redpanda.

### 2. Choreographed Saga

There is no central transaction coordinator.

Instead, services communicate through events:

```text
Order Service
      │
      │ OrderCreated
      ▼
Inventory Service
      │
      │ InventoryReserved
      ▼
Payment Service
      │
      ├── PaymentCompleted ──► Order CONFIRMED
      │
      └── PaymentFailed
                │
                ├──► Inventory restores stock
                └──► Order CANCELLED
```

### 3. Compensating Transactions

When a later operation fails, previously completed operations are logically reversed.

For example:

```text
Reserve Stock
      ↓
Payment FAILS
      ↓
Restore Stock
```

This provides **eventual consistency** rather than traditional ACID consistency across multiple services.

### 4. Idempotency

Consumers are designed to safely handle duplicate events.

If the same event is delivered more than once, the service checks whether it has already been processed before applying the business operation again.

```text
Event ID
   ↓
Already Processed?
   ├── YES → Ignore
   └── NO  → Process + Record Event ID
```

This is important because event-driven systems generally provide **at-least-once delivery semantics**, meaning consumers must be prepared for duplicate messages.

---

## Technology Stack

The technologies are intentionally shown as part of the architecture because they map directly to specific responsibilities:

| Layer          | Technology               | Responsibility                     |
| -------------- | ------------------------ | ---------------------------------- |
| API            | **FastAPI**              | REST API layer                     |
| ASGI Server    | **Uvicorn**              | Runs FastAPI services              |
| Concurrency    | **AsyncIO**              | Asynchronous execution             |
| Database       | **PostgreSQL 16**        | Persistent service data            |
| ORM            | **SQLAlchemy 2.0 Async** | Database interaction               |
| DB Driver      | **asyncpg**              | Async PostgreSQL connectivity      |
| Event Broker   | **Redpanda**             | Event streaming                    |
| Kafka Client   | **aiokafka**             | Async event production/consumption |
| Infrastructure | **Docker**               | Containerization                   |
| Orchestration  | **Docker Compose**       | Local distributed environment      |

---

## Dependencies

Install the required Python dependencies:

```powershell
pip install fastapi uvicorn sqlalchemy asyncpg aiokafka psycopg2-binary
```

---

## What This Project Demonstrates

This project is primarily a practical implementation of **distributed backend concepts**, rather than simply a CRUD application.

It demonstrates:

* Asynchronous microservices
* Event-driven architecture
* Transactional Outbox Pattern
* Dual-write problem mitigation
* Choreographed Saga
* Compensating transactions
* Eventual consistency
* Idempotent event processing
* Kafka-compatible event streaming
* Service-specific databases
* ACID transaction boundaries
* Asynchronous database operations
* Containerized distributed infrastructure
* Failure handling across distributed services
