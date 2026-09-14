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
| **Chapter 4** | **The Scheduling Heartbeat** *(Celery Beat)* | **Completed** | Database-driven periodic sweep (`next_check_at`, `next_keep_alive_at`), concurrency-safe `FOR UPDATE SKIP LOCKED`, and anti-storm recovery. |
| **Chapter 5** | **Network Resilience** *(HTTPX Prober)* | **Completed** | Fine-grained timeout budgets, SSRF defense, redirect interception, streaming memory limits, outcome classification (UP/DEGRADED/DOWN/UNREACHABLE). |
| **Chapter 6** | **Container Orchestration** *(Docker Compose)* | **Up Next** | Multi-container environment with health-check dependency chains. |

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
Start the Celery worker process:
> **Windows Note:** Because billiard's prefork pool is not supported on Windows, start the worker using `-P solo` or `-P threads`:
```powershell
.\.venv\Scripts\celery.exe -A app.worker.celery_app worker -l info -P solo
```

### Terminal 5: Celery Beat (The Scheduling Heartbeat)
Start the Celery Beat periodic scheduler process:
> **Singleton Note:** Celery Beat MUST run as exactly one instance (`replicas = 1`).
```powershell
.\.venv\Scripts\celery.exe -A app.worker.celery_app beat -l info
```

---

## 6. Chapter 4: Celery Beat Scheduling Architecture

Chapter 4 introduces automated, database-driven scheduling that decides **WHEN** checks are performed without performing any network I/O in the scheduler:

* **Single Static Sweep:** A single entry `"sweep-due-monitors"` in `celery_app.conf.beat_schedule` fires periodically (default `15.0s`, configurable via `SWEEP_INTERVAL_SECONDS`).
* **Dual Independent Schedules:**
  * **Monitoring Schedule:** Queries `next_check_at <= now` for monitors in mode `monitor` or `monitor_and_keep_alive`.
  * **Keep-Alive Schedule:** Queries `next_keep_alive_at <= now` for monitors with `keep_alive_enabled=True` in mode `keep_alive` or `monitor_and_keep_alive`.
* **Concurrency-Safe Row Claiming:** Uses `SELECT ... FOR UPDATE SKIP LOCKED` (`with_for_update(skip_locked=True)`), ensuring overlapping sweep runs skip already-locked rows without duplicate task dispatch.
* **Timestamp Advancement:** Timestamps advance forward based on each monitor's configured interval from the reference sweep time (`now + check_interval_seconds`).
* **Anti-Storm Recovery:** Missed schedules after Beat restarts trigger exactly ONE check per overdue monitor, advancing from the current time rather than replaying missed historical intervals.

---

## 7. Chapter 5: Network Resilience & SSRF Defense (`app/net.py`)

Chapter 5 equips the Celery worker probing fleet with a hardened, bounded, observable network execution engine powered by `httpx.AsyncClient`:

* **Granular Phased Timeouts:** Separates request execution into connect (2.0s), read (5.0s), write (5.0s), and pool acquisition (2.0s), eliminating worker starvation.
* **Multi-Layer SSRF Defense:** Rejects loopback (`127.0.0.0/8`, `::1`), private RFC1918 subnets (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), link-local IPs, and cloud instance metadata (`169.254.169.254`).
* **DNS Resolution & Rebinding Mitigation:** Resolves all hostnames via `socket.getaddrinfo` before socket establishment, verifying every candidate IP against the blocklist.
* **HTTP Redirect Interception:** Utilizes `httpx` response event hooks to validate target `Location` headers on HTTP 3xx responses. Redirects to private or cloud metadata IPs are intercepted and aborted before connection.
* **Memory-Bounded Streaming:** Streams response bodies with a hard 1MB (`1,048,576` bytes) ceiling using `client.stream("GET", url)`. Responses exceeding the limit abort gracefully to protect workers from OOM crashes.
* **Monotonic Latency Tracking:** Latency is calculated using `time.monotonic()`, immune to NTP adjustments and system clock jumps.
* **Structured Outcome Classification:** Normalizes status codes and errors into `PingOutcome` (`UP`, `DEGRADED`, `DOWN`, `UNREACHABLE`).
* **Credential Redaction:** Sanitizes basic authentication credentials (`user:pass@`) in logs and database entries.
* **Strict Keep-Alive Independence:** Keep-Alive activity pings record telemetry to `ping_results` (`check_type="keep_alive"`), but **NEVER alter `Monitor.status`**.

---

## 8. Running the Automated Test Suite

PingGuard includes comprehensive automated tests covering API validation, database persistence, app restart durability, Celery tasks, Celery Beat scheduling, and HTTPX network resilience:

```powershell
.\.venv\Scripts\python.exe run_tests.py
```
*Executes all 59 automated tests across `test_api.py`, `test_tasks.py`, `test_scheduler.py`, and `test_net.py`.*

---

## 9. Chapter 6 Handoff: Container Orchestration & Production Deployment

With Chapters 1 through 5 fully operational:
* **Chapter 1:** Validates and ingests monitoring configurations.
* **Chapter 2:** Durably persists models and historical telemetry in PostgreSQL.
* **Chapter 3:** Distributes task execution across Celery workers via Redis.
* **Chapter 4:** Periodically evaluates schedules and claims due checks concurrency-safely.
* **Chapter 5:** Safely executes network probes with SSRF defense, timeouts, and bounded streaming.

In **Chapter 6: Container Orchestration (Docker Compose)**:
1. Package the entire ecosystem (FastAPI, PostgreSQL, Redis, Celery Worker, Celery Beat) into container images.
2. Define `docker-compose.yml` with health-check dependency chains (`depends_on: condition: service_healthy`).
3. Configure isolated internal networking ensuring only the API reverse proxy is exposed publicly.

