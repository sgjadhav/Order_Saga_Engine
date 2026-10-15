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

## Observability Architecture

The saga pipeline is instrumented end-to-end with a **Prometheus + Grafana** observability layer that runs alongside the existing services — added without touching any business logic, row locks, or transaction flow.

### Prometheus — Metrics Scraping & Time-Series Storage

Prometheus runs as a container (`http://localhost:9090`) and scrapes metrics across services every **2s / 5s** (global `scrape_interval: 2s` in `prometheus.yml`; individual jobs can be relaxed to a **5s** interval for heavier targets):

| Scrape Job          | Target                          | Metrics Source                                        |
| ------------------- | ------------------------------- | ------------------------------------------------------ |
| `redpanda`          | `redpanda:9644`                 | Redpanda broker metrics endpoint                      |
| `order_service`     | `host.docker.internal:8000`     | `prometheus-fastapi-instrumentator` (`GET /metrics`)  |
| `inventory_service` | `host.docker.internal:8001`     | `prometheus_client` HTTP server (saga counters)       |
| `payment_service`   | `host.docker.internal:8002`     | `prometheus_client` HTTP server (payment counters)    |

The scrape configuration lives in `prometheus.yml` in the repository root and is mounted into the Prometheus container.

### Grafana — Real-Time Visual Control Plane

Grafana runs as a container (`http://localhost:3000`, login `admin/admin`) and serves as the real-time visual control plane for monitoring:

* **Order throughput** — per-endpoint request rates from `http_requests_total`.
* **p95 latency** — `histogram_quantile(0.95, ...)` over `http_request_duration_seconds_bucket`.
* **Stock decrement counters** — `inventory_reserved_total` and `inventory_failed_total`.
* **Payment settlements** — `payments_successful_total` and `payments_failed_total`.
* **Compensation rollbacks** — `inventory_refunded_total`.

The wiring lives in `docker-compose.yml`: Prometheus mounts `./prometheus.yml`, and Grafana `depends_on` Prometheus.

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
| Monitoring     | **Prometheus**           | Metrics scraping & time-series storage |
| Dashboards     | **Grafana**              | Real-time distributed systems dashboards |
| Metrics        | **prometheus-fastapi-instrumentator & prometheus_client** | Python metrics exposition |

---

## Local Deployment & Verification

### 1. Prerequisites
* Docker & Docker Compose
* Python 3.12+

<<<<<<< HEAD
### 2. Start Infrastructure
Start the isolated PostgreSQL databases and Redpanda streaming broker:
```bash
docker compose up -d
=======
```powershell
pip install fastapi uvicorn sqlalchemy asyncpg aiokafka psycopg2-binary prometheus-fastapi-instrumentator prometheus-client
>>>>>>> 838a34b (feat(observability): integrate prometheus scraping and grafana control plane for saga workflow)
```

### 3. Run Verification Tests
Verify row-level locking concurrency and atomic deduplication under simulated high-contention loads:
```bash
# Concurrency & overselling protection
pytest tests/test_inventory_concurrency.py

<<<<<<< HEAD
# Consumer duplicate delivery idempotency
pytest tests/test_consumer_idempotency.py
```
=======
## Quickstart / Running

1. **Start the infrastructure** (databases, Redpanda, Prometheus, Grafana):

   ```powershell
   docker compose up -d
   ```

2. **Install the Python dependencies** (see the Dependencies section above).

3. **Start each service** in its own terminal, from the repository root:

   ```powershell
   uvicorn order_service.main:app --port 8000
   python -m inventory_service.consumer
   python -m payment_service.consumer
   ```

   Start the outbox relay workers as well (`order_service/relay_worker.py`, `inventory_service/relay_worker.py`, `payment_service/relay_worker.py`) so the saga flows end-to-end. Run the services on the host (not inside containers) — Prometheus reaches them through `host.docker.internal`.

### Access URLs

| Resource              | URL                                 |
| --------------------- | ----------------------------------- |
| Grafana               | http://localhost:3000 (admin/admin) |
| Prometheus Targets    | http://localhost:9090/targets       |
| Order Service Metrics | http://localhost:8000/metrics       |
| Inventory Metrics     | http://localhost:8001/metrics       |
| Payment Metrics       | http://localhost:8002/metrics       |

### Viewing & Verifying the "Distributed Saga Control Plane" Dashboard

1. Open **Grafana** at http://localhost:3000 and sign in with `admin` / `admin` (Grafana prompts for a password change on first login).
2. **Add the Prometheus data source**: *Connections → Data sources → Add data source → Prometheus*, set the URL to `http://prometheus:9090` (Prometheus's container name on the Docker network), then **Save & test** — it should report *data source is working*.
3. **Verify scraping**: open http://localhost:9090/targets and confirm all four jobs — `redpanda`, `order_service`, `inventory_service`, `payment_service` — show state **UP**.
4. **Create the dashboard**: *Dashboards → New dashboard → Add visualization → Prometheus*, titled **Distributed Saga Control Plane**, with these panels:

   | Panel                  | PromQL                                                                                    |
   | ---------------------- | ----------------------------------------------------------------------------------------- |
   | Order throughput       | `sum(rate(http_requests_total{job="order_service"}[1m]))` |
   | p95 latency            | `histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{job="order_service"}[1m])))` |
   | Stock decrements       | `inventory_reserved_total`, `inventory_failed_total` |
   | Payment settlements    | `payments_successful_total`, `payments_failed_total` |
   | Compensation rollbacks | `inventory_refunded_total` |

5. **Verify the panels move**: place a few orders (`POST /orders`), then trigger one successful payment and one above `PAYMENT_LIMIT_THRESHOLD`. Within the 2s scrape window you should see order throughput and p95 latency respond to API traffic, the reservation and settlement counters climb, and `inventory_refunded_total` tick up once per saga compensation (stock rollback).

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
>>>>>>> 838a34b (feat(observability): integrate prometheus scraping and grafana control plane for saga workflow)
