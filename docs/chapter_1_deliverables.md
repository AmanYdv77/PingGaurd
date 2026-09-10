# PingGuard — Chapter 1 Final Deliverables Report
**Component:** The Request Layer (FastAPI & Async Python)  
**Milestone:** Chapter 1 of 6  
**Status:** Completed & Fully Verified  

---

## 1. Milestone Executive Summary

Chapter 1 establishes the **Management Control Plane** for PingGuard, a distributed uptime monitoring system. It implements an asynchronous, non-blocking HTTP REST interface using FastAPI, Uvicorn, and Pydantic v2.

### Core Architectural Principle
```
CLIENT ──► FASTAPI ──► PYDANTIC VALIDATION ──► ASYNC ROUTE HANDLER ──► IN-MEMORY STORE ──► JSON RESPONSE
```
**Strict Architectural Boundary:**  
No outbound HTTP probes (`requests.get`, `httpx` pings) are executed within this layer. Network probing involves non-deterministic latency, packet loss, and potential slowloris tarpits. Executing synchronous outbound network calls inside ASGI routes would block Python's asynchronous event loop and freeze API concurrency for all concurrent clients. Network probing is strictly isolated to the Celery background worker pool in Chapter 3.

---

## 2. Project File Structure

```
pingguard/
│
├── app/
│   ├── __init__.py          # Application package declaration & version metadata
│   ├── main.py              # FastAPI application, OpenAPI metadata, and async route handlers
│   ├── schemas.py           # Pydantic v2 data contracts & validation constraints
│   └── store.py             # In-memory storage abstraction & FastAPI dependency provider
│
├── docs/
│   ├── chapter_1_deliverables.md  # Comprehensive deliverables & verification report
│   ├── architecture_overview.md   # Architectural blueprint & isolation boundary guide
│   └── PingGuard_Technical_Blueprint.pdf # Full 34-page engineering blueprint
│
├── tests/
│   ├── __init__.py          # Test package marker
│   └── test_api.py          # 20-case automated test suite (FastAPI TestClient)
│
├── .gitignore               # Python, virtualenv, and IDE ignore patterns
├── pyproject.toml           # Poetry dependency specification
├── requirements.txt         # Standard pip requirements manifest
├── run_tests.py             # Standalone test runner
└── README.md                # Project documentation & operational quickstart
```

---

## 3. Component Details & File Roles

### 1. `app/schemas.py`
Defines the Pydantic v2 data models for input validation and output serialization:
* **`MonitorStatus` (`str, Enum`):**
  * `UP = "up"`
  * `DEGRADED = "degraded"`
  * `DOWN = "down"`
  * `PENDING = "pending"`
* **`MonitorCreate` (`BaseModel`):**
  * `name`: string, length between 1 and 120 characters.
  * `url`: `HttpUrl` strictly validated by Pydantic.
  * `check_interval_seconds`: integer, default `60`, bounds: $[15, 86400]$.
* **`MonitorRead` (`BaseModel`):**
  * `id`: integer primary identifier.
  * `name`: string.
  * `url`: `HttpUrl`.
  * `check_interval_seconds`: integer.
  * `status`: `MonitorStatus`.
  * `last_checked_at`: `datetime | None`.
  * `next_check_at`: `datetime | None`.
  * `model_config = ConfigDict(from_attributes=True)`: allows direct serialization from future SQLAlchemy ORM objects.
* **`MonitorUpdate` (`BaseModel`):**
  * `name`: optional string (`min_length=1, max_length=120`).
  * `check_interval_seconds`: optional integer (`ge=15, le=86400`).

### 2. `app/store.py`
Provides the isolated in-memory repository:
* Auto-incrementing unique IDs starting at 1.
* Maintains initial status `PENDING` and sets `next_check_at` to the current UTC timestamp.
* Exposes asynchronous CRUD interfaces (`create`, `get_by_id`, `update`, `list_monitors`, `delete`, `clear`).
* Injected into FastAPI routes via `Depends(get_store)`.

### 3. `app/main.py`
FastAPI ASGI application with asynchronous route handlers:
* `POST /monitors/` (`201 Created`, `response_model=MonitorRead`)
* `GET /monitors/{monitor_id}` (`200 OK` or `404 Not Found`)
* `PATCH /monitors/{monitor_id}` (`200 OK` or `404 Not Found`)
* `PUT /monitors/{monitor_id}` (`200 OK` or `404 Not Found`)
* `GET /monitors/` (`200 OK`, paginated slice with `skip` and `limit`)
* `GET /health` (`200 OK`, service status)
* Automatic interactive OpenAPI documentation at `/docs` and `/redoc`.

---

## 4. Installation & Operational Run Instructions

### 1. Prerequisites
* Python 3.12+

### 2. Virtual Environment Setup
```bash
# Clone the repository
git clone https://github.com/AmanYdv77/PingGaurd.git
cd PingGaurd

# Create and activate virtual environment
python -m venv venv

# On Windows:
.\venv\Scripts\Activate.ps1
# On Linux/macOS:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Launching the API Server
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 4. Interactive Documentation
* Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs)
* ReDoc: [http://localhost:8000/redoc](http://localhost:8000/redoc)
* OpenAPI Schema: [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json)

---

## 5. API Endpoint Specifications & Example Payloads

### A. Create Monitor
* **Method & Route:** `POST /monitors/`
* **Status:** `201 Created`
* **Request:**
```bash
curl -X POST http://localhost:8000/monitors/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Stripe API Health",
    "url": "https://api.stripe.com/health",
    "check_interval_seconds": 30
  }'
```
* **Response:**
```json
{
  "id": 1,
  "name": "Stripe API Health",
  "url": "https://api.stripe.com/health",
  "check_interval_seconds": 30,
  "status": "pending",
  "last_checked_at": null,
  "next_check_at": "2026-09-10T19:49:14.898024Z"
}
```

### B. Retrieve Monitor by ID
* **Method & Route:** `GET /monitors/{monitor_id}`
* **Status:** `200 OK`
```bash
curl http://localhost:8000/monitors/1
```

### C. Update Monitor
* **Method & Route:** `PATCH /monitors/{monitor_id}`
* **Status:** `200 OK`
* **Request:**
```bash
curl -X PATCH http://localhost:8000/monitors/1 \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Stripe Core Ingress",
    "check_interval_seconds": 45
  }'
```

### D. List Monitors
* **Method & Route:** `GET /monitors/?skip=0&limit=50`
* **Status:** `200 OK`
```bash
curl "http://localhost:8000/monitors/?skip=0&limit=50"
```

---

## 6. Validation & Error Handling

FastAPI and Pydantic automatically intercept malformed requests and return standardized HTTP 422 errors:

### Invalid URL
* Request: `{"name": "Bad", "url": "not-a-url"}`
* Response: `422 Unprocessable Entity`
```json
{
  "detail": [
    {
      "type": "url_parsing",
      "loc": ["body", "url"],
      "msg": "Input should be a valid URL, relative URL without a base",
      "input": "not-a-url"
    }
  ]
}
```

### Interval Below Minimum (< 15s)
* Request: `{"name": "Fast", "url": "https://example.com", "check_interval_seconds": 5}`
* Response: `422 Unprocessable Entity`
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

### Non-Existent Monitor Lookup
* Request: `GET /monitors/99999`
* Response: `404 Not Found`
```json
{
  "detail": "Monitor not found"
}
```

---

## 7. Automated Test Verification Results

All 20 test cases pass cleanly via `python run_tests.py`:

```
test_create_monitor_custom_interval ... ok
test_create_monitor_empty_name ... ok
test_create_monitor_interval_above_maximum ... ok
test_create_monitor_interval_below_minimum ... ok
test_create_monitor_interval_wrong_type ... ok
test_create_monitor_invalid_url ... ok
test_create_monitor_missing_name ... ok
test_create_monitor_name_too_long ... ok
test_create_monitor_valid ... ok
test_get_existing_monitor ... ok
test_get_non_existent_monitor ... ok
test_health_check ... ok
test_list_monitors ... ok
test_list_monitors_pagination ... ok
test_openapi_schema ... ok
test_swagger_ui ... ok
test_update_invalid_interval ... ok
test_update_monitor_patch ... ok
test_update_monitor_put ... ok
test_update_non_existent_monitor ... ok

----------------------------------------------------------------------
Ran 20 tests in 0.110s

OK
```

---

## 8. Chapter 1 → Chapter 2 Handoff

In **Chapter 2: The Persistence Layer (SQLAlchemy 2.0 Async & Alembic)**:
1. **Store Replacement:** Replace `get_store()` with an `AsyncSession` dependency provider:
   ```python
   # Chapter 2: app/db.py
   async def get_db() -> AsyncGenerator[AsyncSession, None]:
       async with AsyncSessionFactory() as session:
           yield session
   ```
2. **Seamless ORM Serialization:** `MonitorRead` already declares `from_attributes=True`. Returning a SQLAlchemy model instance from the route will serialize automatically without any schema modifications.
3. **Database Tables:** Alembic will generate the schema for `monitors` and `ping_logs` tables with composite indexing on `(monitor_id, created_at)`.
