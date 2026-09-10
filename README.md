# PingGuard — Chapter 1: The Request Layer (FastAPI & Async Python)

PingGuard is an enterprise distributed uptime monitoring system. This repository contains the reference implementation of **Chapter 1: The Request Layer**, built according to the PingGuard Technical Architecture Blueprint.

Chapter 1 provides a clean, asynchronous HTTP control plane for monitor registration and management without relying on external databases or network worker processes.

---

## 1. Architectural Scope & Boundary

### Responsibilities (Chapter 1)
- Ingest client HTTP requests (`POST`, `GET`, `PATCH`, `PUT`).
- Enforce strict perimeter validation via **Pydantic v2** (`HttpUrl`, interval limits, string lengths).
- Reject malformed payloads automatically with HTTP 422 before touching business logic.
- Manage monitor states (`PENDING`, `UP`, `DEGRADED`, `DOWN`).
- Store monitor records in an in-memory repository designed for clean replacement by SQLAlchemy AsyncSession in Chapter 2.
- Expose interactive OpenAPI documentation (`/docs`, `/openapi.json`).

### Architectural Non-Goals (Preserved for Later Chapters)
```
CLIENT ──► FASTAPI ──► PYDANTIC VALIDATION ──► ASYNC ROUTE ──► IN-MEMORY STORE ──► JSON RESPONSE
```
> **CRITICAL BOUNDARY:** There is **NO direct network probing or pinging** (`requests.get`, `httpx` probes) inside these routes. Executing synchronous outbound network calls inside ASGI routes would block Python's asynchronous event loop and freeze concurrency for other clients. Probing is strictly offloaded to Celery background workers in Chapter 3.

---

## 2. Technology Stack

* **Language:** Python 3.12+
* **Framework:** [FastAPI](https://fastapi.tiangolo.com/) (ASGI web framework & routing)
* **Server:** [Uvicorn](https://www.uvicorn.org/) (High-performance ASGI server)
* **Validation & Serialization:** [Pydantic v2](https://docs.pydantic.dev/latest/)
* **Configuration:** Pydantic Settings

---

## 3. Project Structure

```
pingguard/
├── app/
│   ├── __init__.py      # Package declaration
│   ├── main.py          # FastAPI application & async route handlers
│   ├── schemas.py       # Pydantic v2 request/response contracts
│   └── store.py         # In-memory persistence abstraction & DI provider
├── tests/
│   ├── __init__.py
│   └── test_api.py      # Comprehensive 20-test verification suite
├── pyproject.toml       # Poetry dependency manifest
├── requirements.txt     # Standard pip requirements
├── run_tests.py         # Standalone test runner
└── README.md            # Architecture & operational guide
```

---

## 4. Installation & Setup

### Option A: Using virtual environment + pip
```bash
# 1. Navigate to the project directory
cd pingguard

# 2. Create and activate a Python 3.12 virtual environment
python -m venv venv

# Windows:
.\venv\Scripts\Activate.ps1
# Linux/macOS:
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

### Option B: Using Poetry
```bash
cd pingguard
poetry install
poetry shell
```

---

## 5. Running the Application

Start the development server with Uvicorn:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Once started, the following services are available:
* **API Base URL:** [http://localhost:8000](http://localhost:8000)
* **Interactive Swagger UI:** [http://localhost:8000/docs](http://localhost:8000/docs)
* **ReDoc Documentation:** [http://localhost:8000/redoc](http://localhost:8000/redoc)
* **OpenAPI Schema JSON:** [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json)

---

## 6. API Endpoints

| Method | Path | Status Code | Description |
| :--- | :--- | :--- | :--- |
| `GET` | `/health` | `200 OK` | Service operational health check |
| `POST` | `/monitors/` | `201 Created` | Register a new monitor endpoint |
| `GET` | `/monitors/{id}` | `200 OK` | Retrieve single monitor details (or 404) |
| `PATCH` | `/monitors/{id}` | `200 OK` | Partially update name or interval |
| `PUT` | `/monitors/{id}` | `200 OK` | Update monitor configuration |
| `GET` | `/monitors/` | `200 OK` | Paginated listing of monitors (`?skip=0&limit=100`) |

---

## 7. Example Requests & Responses

### 1. Create a Monitor (POST /monitors/)
```bash
curl -X POST http://localhost:8000/monitors/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Stripe API Health",
    "url": "https://api.stripe.com/health",
    "check_interval_seconds": 30
  }'
```

**Response (`201 Created`):**
```json
{
  "id": 1,
  "name": "Stripe API Health",
  "url": "https://api.stripe.com/health",
  "check_interval_seconds": 30,
  "status": "pending",
  "last_checked_at": null,
  "next_check_at": "2026-09-11T01:17:40.123456Z"
}
```

### 2. Validation Error Example (POST /monitors/ with invalid interval)
```bash
curl -X POST http://localhost:8000/monitors/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Invalid Monitor",
    "url": "https://example.com",
    "check_interval_seconds": 5
  }'
```

**Response (`422 Unprocessable Entity`):**
```json
{
  "detail": [
    {
      "type": "greater_than_equal",
      "loc": ["body", "check_interval_seconds"],
      "msg": "Input should be greater than or equal to 15",
      "input": 5,
      "ctx": { "ge": 15 }
    }
  ]
}
```

### 3. Retrieve Monitor by ID (GET /monitors/1)
```bash
curl http://localhost:8000/monitors/1
```

**Response (`200 OK`):**
```json
{
  "id": 1,
  "name": "Stripe API Health",
  "url": "https://api.stripe.com/health",
  "check_interval_seconds": 30,
  "status": "pending",
  "last_checked_at": null,
  "next_check_at": "2026-09-11T01:17:40.123456Z"
}
```

### 4. Non-Existent Monitor (GET /monitors/99999)
```bash
curl http://localhost:8000/monitors/99999
```

**Response (`404 Not Found`):**
```json
{
  "detail": "Monitor not found"
}
```

### 5. Update Monitor (PATCH /monitors/1)
```bash
curl -X PATCH http://localhost:8000/monitors/1 \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Stripe Global Gateway",
    "check_interval_seconds": 60
  }'
```

**Response (`200 OK`):**
```json
{
  "id": 1,
  "name": "Stripe Global Gateway",
  "url": "https://api.stripe.com/health",
  "check_interval_seconds": 60,
  "status": "pending",
  "last_checked_at": null,
  "next_check_at": "2026-09-11T01:17:40.123456Z"
}
```

---

## 8. Automated Verification & Testing

Execute the automated test suite covering all 8 milestone verification scenarios:

```bash
python run_tests.py
```

Output:
```
Ran 20 tests in 0.110s
OK
```

### Test Coverage Breakdown:
1. **Valid Creation:** Default interval 60s, `pending` status, initial timestamps.
2. **URL Validation:** Rejection of non-URL strings, missing schemes, unsupported protocols.
3. **Interval Bounds:** Strict rejection of $<15\text{s}$ and $>86400\text{s}$.
4. **Name Constraints:** Rejection of empty names and names $>120$ characters.
5. **Lookup by ID:** HTTP 200 for existing records, HTTP 404 for missing records.
6. **Updates:** Partial updates via PATCH and PUT, preserving unchanged attributes.
7. **Pagination:** Offset/limit query controls preventing unbounded memory loading.
8. **Documentation:** Structural integrity verification of `/openapi.json` and `/docs`.

---

## 9. Chapter 1 → Chapter 2 Handoff

When transitioning to **Chapter 2: The Persistence Layer (SQLAlchemy 2.0 & Alembic)**:

1. **Dependency Injection Replacement:**
   In Chapter 1, `app/store.py` provides `get_store()`.
   In Chapter 2, replace this with an `AsyncSession` generator:
   ```python
   # Chapter 2 app/db.py
   async def get_db() -> AsyncGenerator[AsyncSession, None]:
       async with AsyncSessionFactory() as session:
           yield session
   ```
2. **Schema Compatibility (`from_attributes=True`):**
   `MonitorRead` in `app/schemas.py` is configured with `ConfigDict(from_attributes=True)`. In Chapter 2, route handlers can directly return SQLAlchemy ORM `MonitorModel` instances, and Pydantic will serialize them without any manual dictionary conversion:
   ```python
   # Chapter 2 route handler
   @app.get("/monitors/{id}", response_model=MonitorRead)
   async def get_monitor(id: int, db: AsyncSession = Depends(get_db)):
       monitor = await db.get(MonitorModel, id)
       if not monitor:
           raise HTTPException(status_code=404, detail="Monitor not found")
       return monitor  # Serialized automatically by MonitorRead!
   ```
3. **Database Migration:**
   Alembic will generate declarative table definitions for `monitors` and `ping_logs` with composite indices (`(monitor_id, created_at)`).
