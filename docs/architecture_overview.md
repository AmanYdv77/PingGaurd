# PingGuard — Architecture Overview

## Distributed Uptime Monitoring System

PingGuard is designed around a strict separation of concerns to handle high concurrency, non-deterministic network latency, and fault isolation.

---

## 1. High-Level Macro Architecture

```
                                  PINGGUARD ARCHITECTURE
                                  
  [ Client / DevOps Dashboard ]
               │  (HTTP REST)
               ▼
   ┌───────────────────────┐
   │  FastAPI Control Plane │  ◄── Chapter 1 (The Request Layer)
   └───────────┬───────────┘
               │  (Async SQLAlchemy 2.0 / asyncpg)
               ▼
   ┌───────────────────────┐
   │ PostgreSQL 16 Database│  ◄── Chapter 2 (The Persistence Layer)
   └───────────┬───────────┘
               │  (Periodic Polling with SKIP LOCKED)
               ▼
   ┌───────────────────────┐
   │   Celery Beat Clock   │  ◄── Chapter 4 (The Scheduling Heartbeat)
   └───────────┬───────────┘
               │  (Batch Task Enqueue)
               ▼
   ┌───────────────────────┐
   │     Redis 7 Broker    │  ◄── Chapter 3 (Distributed Task Execution)
   └───────────┬───────────┘
               │  (Prefetch = 1, acks_late = True)
               ▼
   ┌───────────────────────┐
   │  Celery Worker Pool   │
   └───────────┬───────────┘
               │  (Timeout-bound Resilient Probes)
               ▼
   ┌───────────────────────┐
   │  HTTPX Probing Engine │  ◄── Chapter 5 (Network Resilience & Probing)
   └───────────┬───────────┘
               │
               ▼
   [ Target Internet Host ]
```

---

## 2. Fast Side vs. Slow Side Decoupling

PingGuard strictly separates operations based on latency predictability:

* **The Fast Side (FastAPI API):**
  * Sub-5ms response times.
  * Ingests, validates, and stores monitor definitions.
  * Pure async I/O against database/cache.
  * Never touches third-party external networks.

* **The Slow Side (Celery Worker Pool + HTTPX):**
  * Bounded by external network conditions (50ms to 10,000ms timeouts).
  * Executes asynchronous HTTP probes.
  * Handles DNS failures, packet loss, and connection resets.
  * Isolates worker failures so they never impact API availability.

---

## 3. Chapter Roadmap & Milestones

* **Chapter 1: FastAPI & Async Python — The Request Layer** *(Current)*
* **Chapter 2: SQLAlchemy 2.0 Async & Alembic — The Persistence Layer**
* **Chapter 3: Celery & Redis — Distributed Task Execution**
* **Chapter 4: Celery Beat — The Scheduling Heartbeat**
* **Chapter 5: HTTPX & Network Resilience — The Probing Engine**
* **Chapter 6: Docker & Docker Compose — Orchestration & Deployment**
