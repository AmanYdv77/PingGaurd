# PingGuard — Distributed Uptime Monitoring System

PingGuard is a high-performance, fault-tolerant distributed uptime monitoring system. It is designed to reliably monitor thousands of remote endpoints across the internet without letting slow or unresponsive targets block user-facing APIs.

The system strictly decouples the **API Control Plane** (FastAPI + PostgreSQL) from the **Distributed Probing Data Plane** (Celery, Redis, and HTTPX) so that slow external network I/O never degrades API throughput.

---

## 1. Project Roadmap & Implementation Status

PingGuard is developed in six sequential, independently verifiable chapters:

| Chapter | Component & Technology | Status | Description |
| :--- | :--- | :---: | :--- |
| **Chapter 1** | **The Request Layer** *(FastAPI & Async Python)* | **Completed** | Non-blocking REST API, Pydantic v2 validation, and boundary guards. |
| **Chapter 2** | **The Persistence Layer** *(SQLAlchemy 2.0 & Alembic)* | **Completed** | Relational persistence with PostgreSQL, asyncpg driver, typed Mapped[] ORM, and Alembic migrations. |
| **Chapter 3** | **Distributed Task Execution** *(Celery & Redis)* | **Up Next** | Worker pool, Redis task broker, idempotency locks, and late task acknowledgements. |
| **Chapter 4** | **The Scheduling Heartbeat** *(Celery Beat)* | Planned | Periodic database sweep (`next_check_at`, `next_keep_alive_at`) with `FOR UPDATE SKIP LOCKED`. |
| **Chapter 5** | **Network Resilience** *(HTTPX Prober)* | Planned | Fine-grained timeout budgets, SSRF defense, outcome classification (UP/DEGRADED/DOWN/UNREACHABLE). |
| **Chapter 6** | **Container Orchestration** *(Docker Compose)* | Planned | Multi-container environment with health-check dependency chains. |

---

## 2. Chapter 2 Architecture: The Persistence Layer

Chapter 2 establishes the durable relational foundation for PingGuard, replacing Chapter 1's in-memory store with an asynchronous PostgreSQL persistence layer:

```
Client
  ↓
FastAPI
  ↓
Pydantic v2 Validation
  ↓
AsyncSession (Dependency Injection via get_db)
  ↓
SQLAlchemy 2.0 Typed ORM
  ↓
PostgreSQL (monitors, ping_results, alembic_version)
```

### Core Relational Models

1. **`Monitor` (`monitors` table):**
   Represents endpoint configuration, polling intervals, keep-alive parameters, and future scheduler state.
   * `id`: Integer primary key (auto-incrementing)
   * `name`: String(120), target label
   * `url`: String(2048), target URL (intentionally non-unique to allow multiple test profiles per URL)
   * `check_interval_seconds`: Integer (default 60s, range 15s–86400s)
   * `status`: String(20) (`pending`, `up`, `degraded`, `down`, `unreachable`)
   * `last_checked_at`: Timezone-aware UTC timestamp (nullable)
   * `next_check_at`: Timezone-aware UTC timestamp (**indexed** for periodic scheduler polling)
   * **Keep-Alive Configuration:**
     * `mode`: String(30) (`monitor`, `keep_alive`, `monitor_and_keep_alive`)
     * `keep_alive_enabled`: Boolean (default `false`)
     * `keep_alive_interval_seconds`: Integer (nullable, range 15s–86400s)
     * `keep_alive_path`: String(255) (nullable, relative URI path e.g. `/health`)
     * `next_keep_alive_at`: Timezone-aware UTC timestamp (**indexed** for future keep-alive scheduler sweep)
   * `results`: One-to-many relationship to `PingResult` with `cascade="all, delete-orphan"` and `passive_deletes=True`.

2. **`PingResult` (`ping_results` table):**
   Represents historical telemetry from individual probe executions.
   * `id`: Integer primary key
   * `monitor_id`: Integer foreign key (`monitors.id`, `ON DELETE CASCADE`, indexed)
   * `check_type`: String(20) (`"monitor"` for health probes, `"keep_alive"` for wake-up activity pings)
   * `status_code`: Integer HTTP status code (nullable)
   * `latency_ms`: Float round-trip latency in milliseconds (nullable)
   * `error`: String(500) error summary on probe failure (nullable)
   * `checked_at`: Timezone-aware UTC timestamp (indexed)
   * Composite Index: `("monitor_id", "checked_at")` for fast historical timeline queries.

---

## 3. Keep-Alive Design & Chapter 2 Scope Boundary

* **Configuration Only at this Milestone:**
  Chapter 2 persists the user's desired Keep-Alive parameters (`mode`, `keep_alive_enabled`, `keep_alive_interval_seconds`, `keep_alive_path`, and `next_keep_alive_at`).
* **Strict Boundary:**
  **Chapter 2 executes ZERO outbound network requests.** Registering or retrieving a monitor with Keep-Alive enabled does not contact the remote server. Outbound probing is strictly isolated to background workers in Chapter 3/5.
* **Realistic Expectations:**
  Keep-Alive is an optional activity/wake-up ping designed to reduce idle sleeping on platforms that spin down. It is not an SLA guarantee of permanent uptime.

---

## 4. Database Setup & Migrations

### 1. Environment Configuration
PingGuard reads its database connection string from the `DATABASE_URL` environment variable or a local `.env` file:

```bash
# .env
DATABASE_URL=postgresql+asyncpg://postgres:password@localhost:5432/pingguard
```

> **Note on Drivers:**
> The FastAPI application runtime uses `asyncpg` for non-blocking asynchronous access.
> Alembic migrations use synchronous `psycopg2` (configured automatically in `alembic/env.py`).

### 2. Alembic Migration Commands

Alembic manages schema evolution cleanly without running migrations automatically on app startup.

```bash
# Apply migrations to head
alembic upgrade head

# Check for schema drift between ORM models and database
alembic check

# Generate a new migration revision if models change
alembic revision --autogenerate -m "describe schema change"

# Roll back all migrations (test rebuild from scratch)
alembic downgrade base
```

---

## 5. Quickstart & Testing

### Prerequisites
* **Python 3.12+**
* **PostgreSQL 14+**

### 1. Installation
```bash
# Clone the repository
git clone https://github.com/AmanYdv77/PingGaurd.git
cd PingGaurd

# Create and activate a virtual environment
python -m venv .venv

# Windows:
.\.venv\Scripts\Activate.ps1
# Linux/macOS:
source .venv/bin/activate

# Install required dependencies
pip install -r requirements.txt
```

### 2. Apply Database Migrations
```bash
alembic upgrade head
```

### 3. Start the API Server
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 4. Run Automated Test Suite
```bash
python run_tests.py
```
*Executes all 27 automated tests covering API validation, keep-alive rules, PostgreSQL persistence, app restart durability, ORM relationships, and cascade deletes.*

---

## 6. API Endpoints Summary

| Method | Route | Status Code | Description |
| :--- | :--- | :--- | :--- |
| `GET` | `/health` | `200 OK` | Service operational health check & milestone info |
| `POST` | `/monitors/` | `201 Created` | Register and persist a new monitor endpoint in PostgreSQL |
| `GET` | `/monitors/{id}` | `200 OK` / `404` | Retrieve monitor configuration and scheduling state from PostgreSQL |
| `PATCH` | `/monitors/{id}` | `200 OK` / `404` | Partially update monitor configuration or keep-alive parameters |
| `PUT` | `/monitors/{id}` | `200 OK` / `404` | Update monitor configuration with consistency validation |
| `GET` | `/monitors/` | `200 OK` | List registered monitors with offset/limit pagination (`?skip=0&limit=100`) |

---

## 7. Interactive Documentation

* **Swagger UI:** [http://localhost:8000/docs](http://localhost:8000/docs)
* **ReDoc:** [http://localhost:8000/redoc](http://localhost:8000/redoc)
* **OpenAPI Specification:** [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json)

---

## 8. Chapter 3 Handoff

Chapter 2 completes the **Persistence Layer**.
The next milestone will implement **Chapter 3: Distributed Task Execution (Celery & Redis)**:
* Connect Celery workers to the PostgreSQL database to read due `Monitor` rows.
* Configure Redis as the distributed task message broker.
* Establish worker pools, task idempotency locks, and late task acknowledgements.
* In Chapter 4, the scheduler (Celery Beat) will use the indexed `next_check_at` and `next_keep_alive_at` timestamps to dispatch monitoring and keep-alive tasks asynchronously.
