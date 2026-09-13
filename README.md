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
| **Chapter 3** | **Distributed Task Execution** *(Celery & Redis)* | **Completed** | Celery worker pool, Redis message broker, late acks, prefetch multiplier 1, and synchronous worker DB sessions. |
| **Chapter 4** | **The Scheduling Heartbeat** *(Celery Beat)* | **Up Next** | Periodic database sweep (`next_check_at`, `next_keep_alive_at`) with `FOR UPDATE SKIP LOCKED`. |
| **Chapter 5** | **Network Resilience** *(HTTPX Prober)* | Planned | Fine-grained timeout budgets, SSRF defense, outcome classification (UP/DEGRADED/DOWN/UNREACHABLE). |
| **Chapter 6** | **Container Orchestration** *(Docker Compose)* | Planned | Multi-container environment with health-check dependency chains. |

---

## 2. Chapter 3 Architecture: Distributed Task Execution

Chapter 3 implements the distributed background processing pipeline, decoupling network I/O from the FastAPI web service:

```
FastAPI / future Celery Beat
       │
       │ Enqueue task (.delay(monitor_id))
       ▼
  Redis Queue (Broker: redis://localhost:6379/0)
       │
       │ Worker consumes task (prefetch_multiplier=1, acks_late=True)
       ▼
  Celery Worker Process
       │
       ├──► execute_ping(monitor_id) -------> Target URL (Health Probe)
       │                                            │
       └──► execute_keep_alive(monitor_id) -> Target URL + path (Activity Ping)
                                                    │
                                                    ▼
                                           PostgreSQL (pingguard)
                                           - ping_results (check_type='monitor' | 'keep_alive')
                                           - monitors (status & last_checked_at)
```

### Key Differences: FastAPI vs. Celery Worker

| Feature | FastAPI API Layer | Celery Worker Layer |
| :--- | :--- | :--- |
| **Role** | API Control Plane (HTTP CRUD, user requests) | Data Plane (Network probes, wake-up pings) |
| **Database Access** | Asynchronous (`AsyncSessionLocal`, `get_db`) via `asyncpg` | Synchronous (`SyncSessionLocal`, `get_sync_db`) via `psycopg2` |
| **Concurrency Model**| Single-process async event loop (ASGI) | Multi-process / threaded worker pool |
| **Network Probing** | **Strictly Forbidden** (never blocks on external HTTP) | **Authorized** (bounded execution with timeouts) |

---

## 3. Worker Tasks: Monitoring vs. Keep-Alive

PingGuard provides two dedicated background tasks with distinct operational semantics:

### 1. `execute_ping(monitor_id: int)`
* **Task Name:** `app.tasks.execute_ping`
* **Purpose:** Evaluates whether the target service is online and healthy.
* **Workflow:**
  1. Loads current `Monitor` from PostgreSQL by `monitor_id`.
  2. Issues HTTP GET request with a bounded timeout (`timeout=5.0s`).
  3. Records latency in milliseconds.
  4. Updates `Monitor.status` (`"up"` for 2xx/3xx, `"degraded"` for 4xx, `"down"` for 5xx/connection failure).
  5. Updates `Monitor.last_checked_at`.
  6. Persists historical record to `ping_results` with `check_type="monitor"`.
  7. Retries transient failures (`Timeout`, `ConnectionError`) up to 3 times with exponential backoff.

### 2. `execute_keep_alive(monitor_id: int)`
* **Task Name:** `app.tasks.execute_keep_alive`
* **Purpose:** Transmits lightweight activity requests to touch services prone to spinning down on idle.
* **Gatekeeper:** If `keep_alive_enabled` is `False`, the task cleanly skips without sending any HTTP request.
* **URL Construction:** Safely combines `monitor.url` and `monitor.keep_alive_path` (e.g. `https://xyz.com` + `/health` -> `https://xyz.com/health`).
* **Telemetry:** Persists a `PingResult` record with `check_type="keep_alive"`.
* **Important Semantic Boundary:**
  * Keep-Alive is an activity attempt, **NOT a permanent uptime guarantee**.
  * Keep-Alive results record transmission outcomes and status codes, but do **NOT** alter `Monitor.status` (uptime health classification is reserved strictly for `execute_ping`).

---

## 4. Reliability & Concurrency Configurations

The Celery application in `app/worker.py` is configured with production-grade reliability parameters:

* **`task_serializer = "json"`, `accept_content = ["json"]`:** Ensures safe JSON-only serialization and prevents arbitrary object exploitation.
* **`task_acks_late = True`:** Tasks are acknowledged only after execution completes. If a worker crashes during execution, the task remains safely in the queue.
* **`worker_prefetch_multiplier = 1`:** Workers reserve only 1 task at a time, eliminating head-of-line blocking caused by slow or unresponsive remote endpoints.
* **`task_soft_time_limit = 10`, `task_time_limit = 15`:** Bounded execution prevents stuck worker processes.

---

## 5. Development Workflow & Starting the Services

For local development, PingGuard uses a 4-terminal architecture:

### Terminal 1: PostgreSQL
Ensure PostgreSQL is running on port `5432` and apply migrations:
```powershell
alembic upgrade head
```

### Terminal 2: Redis
Start the Redis message broker on port `6379`:
```powershell
redis-server
```

### Terminal 3: FastAPI Web Server
Start the API server:
```powershell
.\.venv\Scripts\uvicorn.exe app.main:app --reload --host 0.0.0.0 --port 8000
```

### Terminal 4: Celery Worker
Start the Celery worker process.
> **Windows Note:** Because billiard's prefork pool is not supported on Windows, start the worker using `-P solo` or `-P threads`:
```powershell
.\.venv\Scripts\celery.exe -A app.worker.celery_app worker -l info -P solo
```

---

## 6. Manual Task Triggering & Verification

Tasks can be triggered asynchronously via Python:

```python
from app.tasks import execute_ping, execute_keep_alive

# Enqueue health monitoring task
async_ping = execute_ping.delay(1)
print("Enqueued ping task ID:", async_ping.id)

# Enqueue keep-alive task
async_ka = execute_keep_alive.delay(1)
print("Enqueued keep-alive task ID:", async_ka.id)
```

The call to `.delay()` returns immediately without blocking. The Celery worker picks up the task from Redis, executes the HTTP request, and writes the telemetry record into the `ping_results` table in PostgreSQL.

---

## 7. Running the Automated Test Suite

PingGuard includes comprehensive automated tests covering API validation, database persistence, app restart durability, and background task execution:

```powershell
.\.venv\Scripts\python.exe run_tests.py
```
*Executes all 37 automated tests across `test_api.py` and `test_tasks.py`.*

---

## 8. Chapter 4 Handoff: The Scheduling Heartbeat

Chapter 3 provides executable, safe background tasks. It does **not** contain scheduling logic or database sweeps.

In **Chapter 4: The Scheduling Heartbeat (Celery Beat)**:
1. Celery Beat will run periodic sweeps against PostgreSQL:
   * Query monitors where `next_check_at <= now()`.
   * Query monitors where `keep_alive_enabled=True` and `next_keep_alive_at <= now()`.
2. Beat will use `SELECT ... FOR UPDATE SKIP LOCKED` to lock rows and advance timestamps atomically.
3. For each due monitor, Beat will call `execute_ping.delay(monitor_id)` or `execute_keep_alive.delay(monitor_id)`, handing off execution to the Chapter 3 worker plane.
