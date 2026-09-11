# PingGuard — Distributed Uptime Monitoring System

PingGuard is a high-performance, fault-tolerant distributed uptime monitoring system. It is designed to reliably monitor thousands of remote endpoints across the internet without letting slow or unresponsive targets block user-facing APIs.

The system strictly decouples the **API Control Plane** (FastAPI) from the **Distributed Probing Data Plane** (Celery, Redis, and HTTPX) so that slow external network I/O never degrades API throughput.

---

## 1. Project Roadmap & Implementation Status

PingGuard is developed in six sequential, independently verifiable chapters:

| Chapter | Component & Technology | Status | Description |
| :--- | :--- | :---: | :--- |
| **Chapter 1** | **The Request Layer** *(FastAPI & Async Python)* | **Completed** | Non-blocking REST API, Pydantic v2 validation, in-memory repository, and 20 automated tests. |
| **Chapter 2** | **The Persistence Layer** *(SQLAlchemy 2.0 & Alembic)* | **Up Next** | Relational persistence with PostgreSQL 16, asyncpg driver, and versioned schema migrations. |
| **Chapter 3** | **Distributed Task Execution** *(Celery & Redis)* | Planned | Worker pool, Redis task broker, idempotency locks, and late task acknowledgements. |
| **Chapter 4** | **The Scheduling Heartbeat** *(Celery Beat)* | Planned | Periodic database sweep with `SELECT ... FOR UPDATE SKIP LOCKED` and batch chunking. |
| **Chapter 5** | **Network Resilience** *(HTTPX Prober)* | Planned | Fine-grained timeout budgets (DNS/Connect/Read), SSRF defense, and alert flapping hysteresis. |
| **Chapter 6** | **Container Orchestration** *(Docker Compose)* | Planned | Multi-container environment with health-check dependency chains. |

---

## 2. What Is Currently Implemented (Chapter 1)

Chapter 1 provides the core HTTP API foundation:
* **Asynchronous Route Handlers:** All endpoints run as `async def` on ASGI / Uvicorn.
* **Perimeter Validation (Pydantic v2):** Automatically enforces `HttpUrl` formats, string constraints, check interval limits ($15\text{s} - 86400\text{s}$), and keep-alive configuration consistency. Rejects malformed payloads with HTTP 422.
* **Isolated Storage Abstraction:** An in-memory store injected via FastAPI `Depends()`, designed to be cleanly swapped with PostgreSQL `AsyncSession` in Chapter 2 without modifying route logic.
* **Strict Architecture Boundary:** No outbound network pings inside API routes. Network latency is completely isolated from the request layer.
* **Test Suite:** 23 comprehensive unit and integration tests passing in $<0.2\text{s}$.
* **Interactive API Documentation:** Automatic Swagger UI (`/docs`), ReDoc (`/redoc`), and OpenAPI 3.1 JSON (`/openapi.json`).

---

## 3. Keep-Alive Design (Optional Capability)

PingGuard supports an optional keep-alive mechanism alongside traditional uptime monitoring:

* **Primary Purpose:** PingGuard's primary responsibility remains reliable uptime and health monitoring. Keep-alive is an **optional secondary capability** intended to periodically touch services that spin down or sleep after periods of inactivity (e.g. serverless containers or free-tier hosting platforms).
* **Wake-up Attempt, Not Guaranteed Uptime:** Whether a request prevents sleeping depends entirely on the remote hosting provider's resource management behavior. Keep-alive should be treated as a **periodic wake-up / activity attempt**, not as an absolute guarantee of permanent uptime.
* **Unified Pipeline:** Keep-alive does not require a separate architecture. In later chapters, keep-alive tasks will reuse the exact same `Celery Beat → Redis → Worker → HTTPX` execution pipeline as standard health checks.
* **Chapter 1 Scope Boundary:** Chapter 1 only stores and returns the user's configured intent (`mode`, `keep_alive_enabled`, `keep_alive_interval_seconds`, `keep_alive_path`). **No background tasks are scheduled and no HTTP requests are dispatched during this milestone.**

---

## 4. What Is Coming Up Next (Chapter 2)

The next milestone will implement **Chapter 2: The Persistence Layer**:
* Integrate **PostgreSQL 16** using the **SQLAlchemy 2.0 Async** engine and `asyncpg`.
* Set up **Alembic** to manage version-controlled, zero-downtime database migrations.
* Define declarative ORM models (`MonitorModel`, `PingLogModel`) with composite indexing (`monitor_id`, `created_at`).
* Replace the `InMemoryMonitorStore` dependency in `app/store.py` with an async session factory (`AsyncSession`).

---

## 5. Project Structure & File Details

```
pingguard/
│
├── app/
│   ├── __init__.py          # Package declaration & version metadata
│   ├── main.py              # FastAPI app configuration & async route handlers
│   ├── schemas.py           # Pydantic v2 data models (MonitorCreate, MonitorRead, etc.)
│   └── store.py             # In-memory store & FastAPI dependency injection provider
│
├── tests/
│   ├── __init__.py          # Test package marker
│   └── test_api.py          # 23-case automated test suite (FastAPI TestClient)
│
├── .gitignore               # Ignored files (Python cache, virtualenvs, secrets, docs)
├── pyproject.toml           # Poetry project configuration & dependencies
├── requirements.txt         # Standard pip dependencies
├── run_tests.py             # Standalone test runner script
└── README.md                # Project documentation & operational guide
```

---

## 6. Quickstart & Setup

### Prerequisites
* **Python 3.12+**

### 1. Installation
```bash
# Clone the repository
git clone https://github.com/AmanYdv77/PingGaurd.git
cd PingGaurd

# Create and activate a virtual environment
python -m venv venv

# Windows:
.\venv\Scripts\Activate.ps1
# Linux/macOS:
source venv/bin/activate

# Install required dependencies
pip install -r requirements.txt
```

### 2. Start the API Server
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 3. Run Automated Tests
```bash
python run_tests.py
```

---

## 7. API Endpoints Summary

| Method | Route | Status Code | Description |
| :--- | :--- | :--- | :--- |
| `GET` | `/health` | `200 OK` | Service operational health check |
| `POST` | `/monitors/` | `201 Created` | Register a new monitor endpoint (initial status `pending`) |
| `GET` | `/monitors/{id}` | `200 OK` / `404` | Retrieve details for a single monitor by ID |
| `PATCH` | `/monitors/{id}` | `200 OK` / `404` | Partially update monitor name, check interval, or keep-alive |
| `PUT` | `/monitors/{id}` | `200 OK` / `404` | Update monitor configuration |
| `GET` | `/monitors/` | `200 OK` | List registered monitors (`?skip=0&limit=100`) |

---

## 8. Interactive Documentation

Once the server is running, explore the live endpoints and test requests interactively:
* **Swagger UI:** [http://localhost:8000/docs](http://localhost:8000/docs)
* **ReDoc:** [http://localhost:8000/redoc](http://localhost:8000/redoc)
* **OpenAPI Specification:** [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json)
