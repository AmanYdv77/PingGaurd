"""
In-Memory Store — Chapter 1 Persistence Layer.

Provides an isolated in-memory storage abstraction for PingGuard monitors.
Decoupled from route handlers via FastAPI dependency injection so Chapter 2
can replace this store with an async SQLAlchemy session provider with zero
changes to the API contract.
"""

from datetime import datetime, timezone
from typing import Any
from app.schemas import MonitorCreate, MonitorStatus, MonitorUpdate


class InMemoryMonitorStore:
    """
    Thread-safe (asyncio single-process) in-memory repository for Chapter 1.
    
    Acts as a stand-in for the future PostgreSQL 'monitors' table.
    """

    def __init__(self) -> None:
        self._monitors: dict[int, dict[str, Any]] = {}
        self._next_id: int = 1

    async def create(self, data: MonitorCreate) -> dict[str, Any]:
        """
        Creates and persists a new monitor record in memory.
        Initializes status to PENDING and sets next_check_at to now UTC.
        """
        monitor_id = self._next_id
        self._next_id += 1

        now = datetime.now(timezone.utc)
        record = {
            "id": monitor_id,
            "name": data.name,
            "url": str(data.url),
            "check_interval_seconds": data.check_interval_seconds,
            "status": MonitorStatus.PENDING,
            "last_checked_at": None,
            "next_check_at": now,
        }
        self._monitors[monitor_id] = record
        return dict(record)

    async def get_by_id(self, monitor_id: int) -> dict[str, Any] | None:
        """Retrieves a single monitor record by its integer ID."""
        record = self._monitors.get(monitor_id)
        return dict(record) if record else None

    async def update(self, monitor_id: int, data: MonitorUpdate) -> dict[str, Any] | None:
        """
        Updates an existing monitor's mutable fields (name, check_interval_seconds).
        Returns None if monitor does not exist.
        """
        record = self._monitors.get(monitor_id)
        if not record:
            return None

        if data.name is not None:
            record["name"] = data.name
        if data.check_interval_seconds is not None:
            record["check_interval_seconds"] = data.check_interval_seconds

        return dict(record)

    async def list_monitors(self, skip: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        """
        Lists stored monitors with basic offset/limit pagination.
        Guards against unbounded response sizes.
        """
        all_records = list(self._monitors.values())
        return [dict(r) for r in all_records[skip : skip + limit]]

    async def delete(self, monitor_id: int) -> bool:
        """Deletes a monitor by ID. Returns True if deleted, False if not found."""
        if monitor_id in self._monitors:
            del self._monitors[monitor_id]
            return True
        return False

    def clear(self) -> None:
        """Resets storage. Used exclusively for isolating automated test runs."""
        self._monitors.clear()
        self._next_id = 1


# Global singleton instance for the Chapter 1 runtime
_global_store = InMemoryMonitorStore()


async def get_store() -> InMemoryMonitorStore:
    """
    FastAPI dependency that provides access to the monitor store.
    
    Chapter 1 -> Chapter 2 Handoff:
    In Chapter 2, this dependency will be swapped for an AsyncSession generator:
        async def get_db() -> AsyncGenerator[AsyncSession, None]: ...
    allowing the route handlers to remain clean and decoupled.
    """
    return _global_store
