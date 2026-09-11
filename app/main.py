"""
PingGuard API — Chapter 1: The Request Layer

FastAPI application entry point.
Implements non-blocking asynchronous REST endpoints for monitor registration,
retrieval, update, and listing, supporting both standard uptime monitoring
and optional keep-alive configuration.

Architectural Rule (Chapter 1 Boundary):
----------------------------------------
This layer ONLY handles HTTP parsing, Pydantic validation, and data handoff.
Outbound network pings (requests.get, httpx probes) MUST NEVER be executed
inside these routes, as doing so would stall the ASGI event loop and freeze
API concurrency for other clients. Background execution is handled in Chapter 3.
Keep-alive is an optional activity/wake-up configuration and NOT a guarantee
of permanent uptime.
"""

from typing import Annotated
from fastapi import Depends, FastAPI, HTTPException, Path, Query, status

from app.schemas import MonitorCreate, MonitorRead, MonitorUpdate
from app.store import InMemoryMonitorStore, get_store

# Application metadata for OpenAPI /docs and /redoc
app = FastAPI(
    title="PingGuard API",
    description=(
        "Distributed Uptime Monitoring System — Chapter 1: The Request Layer.\n\n"
        "Provides non-blocking endpoints for registering and managing URL monitors "
        "and optional keep-alive configurations. All requests are validated via "
        "Pydantic v2 schemas before reaching business logic.\n\n"
        "**Note on Keep-Alive:** Keep-alive activity sends periodic lightweight requests "
        "to reduce idle sleeping on platforms that spin down. It is an optional activity attempt, "
        "not a provider-level guarantee of permanent uptime."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


@app.get(
    "/health",
    tags=["System"],
    summary="Health check",
    description="Returns the operational status of the PingGuard Chapter 1 API service."
)
async def health_check() -> dict[str, str]:
    """Basic service health check endpoint."""
    return {
        "status": "healthy",
        "service": "PingGuard API",
        "milestone": "Chapter 1 — The Request Layer (with Optional Keep-Alive)",
    }


@app.post(
    "/monitors/",
    response_model=MonitorRead,
    status_code=status.HTTP_201_CREATED,
    tags=["Monitors"],
    summary="Create a new monitor",
    description=(
        "Validates and registers a new monitor endpoint with optional keep-alive settings. "
        "Initial status is set to PENDING. No outbound network probe or keep-alive ping is performed in this request."
    ),
)
async def create_monitor(
    payload: MonitorCreate,
    store: Annotated[InMemoryMonitorStore, Depends(get_store)],
) -> MonitorRead:
    """
    Asynchronously registers a target URL for monitoring and/or keep-alive.
    
    - Validates payload via MonitorCreate schema.
    - Enforces keep-alive configuration consistency.
    - Assigns an auto-incremented integer identifier.
    - Initializes status to PENDING and marks next_check_at as due immediately.
    - Persists into memory (to be replaced by async SQLAlchemy in Chapter 2).
    - Returns serialized MonitorRead.
    """
    record = await store.create(payload)
    return MonitorRead.model_validate(record)


@app.get(
    "/monitors/{monitor_id}",
    response_model=MonitorRead,
    tags=["Monitors"],
    summary="Get monitor by ID",
    description="Retrieves configuration, status, and keep-alive settings for a single monitor by its integer ID.",
)
async def get_monitor(
    monitor_id: Annotated[int, Path(..., description="The unique integer ID of the monitor", ge=1)],
    store: Annotated[InMemoryMonitorStore, Depends(get_store)],
) -> MonitorRead:
    """
    Asynchronously fetches a monitor by its unique ID.
    
    Raises HTTP 404 if the monitor does not exist.
    """
    record = await store.get_by_id(monitor_id)
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Monitor not found",
        )
    return MonitorRead.model_validate(record)


@app.patch(
    "/monitors/{monitor_id}",
    response_model=MonitorRead,
    tags=["Monitors"],
    summary="Update monitor (Partial)",
    description="Partially updates an existing monitor's name, check interval, or keep-alive configuration.",
)
@app.put(
    "/monitors/{monitor_id}",
    response_model=MonitorRead,
    tags=["Monitors"],
    summary="Update monitor",
    description="Updates an existing monitor's configuration, including keep-alive parameters.",
)
async def update_monitor(
    monitor_id: Annotated[int, Path(..., description="The unique integer ID of the monitor", ge=1)],
    payload: MonitorUpdate,
    store: Annotated[InMemoryMonitorStore, Depends(get_store)],
) -> MonitorRead:
    """
    Asynchronously updates an existing monitor.
    
    Supports both PATCH and PUT semantics. Fields set to None are preserved.
    Enforces configuration consistency across the merged monitor state.
    Raises HTTP 404 if the monitor does not exist, or HTTP 422 if the update creates an invalid state.
    """
    updated_record = await store.update(monitor_id, payload)
    if updated_record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Monitor not found",
        )
    return MonitorRead.model_validate(updated_record)


@app.get(
    "/monitors/",
    response_model=list[MonitorRead],
    tags=["Monitors"],
    summary="List all monitors",
    description="Retrieves a paginated list of registered monitors including keep-alive configuration.",
)
async def list_monitors(
    store: Annotated[InMemoryMonitorStore, Depends(get_store)],
    skip: Annotated[int, Query(description="Number of records to skip (offset)", ge=0)] = 0,
    limit: Annotated[int, Query(description="Maximum records to return", ge=1, le=1000)] = 100,
) -> list[MonitorRead]:
    """
    Asynchronously retrieves a paginated slice of monitors.
    
    Guarded with default limit=100 to prevent unbounded memory serialization.
    """
    records = await store.list_monitors(skip=skip, limit=limit)
    return [MonitorRead.model_validate(r) for r in records]
