"""
Architecture and Schema Layering Tests.

Verifies:
1. app.models does not import from app.schemas (enums <- models <- schemas).
2. The redundant index 'ix_ping_results_monitor_id' is dropped while composite
   and timestamp indexes remain intact.
"""

import ast
from pathlib import Path

from app.db import sync_engine
from sqlalchemy import inspect


def test_models_does_not_import_schemas() -> None:
    """Verify app/models.py does not import from app.schemas (strictly enforces layer direction)."""
    models_path = Path(__file__).resolve().parent.parent / "app" / "models.py"
    with open(models_path, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=str(models_path))

    imported_modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported_modules.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)

    schemas_imports = [
        mod for mod in imported_modules if mod == "app.schemas" or mod.startswith("app.schemas.")
    ]
    assert not schemas_imports, (
        f"app/models.py illegally imports from API schemas layer: {schemas_imports}"
    )


def test_redundant_monitor_id_index_dropped_in_database() -> None:
    """
    Verify that the single-column 'ix_ping_results_monitor_id' index is absent
    from PostgreSQL ping_results table, while composite and checked_at indexes exist.
    """
    inspector = inspect(sync_engine)
    indexes = inspector.get_indexes("ping_results")
    index_names = {idx["name"] for idx in indexes}

    # Redundant index must NOT be present
    assert "ix_ping_results_monitor_id" not in index_names, (
        f"Redundant index 'ix_ping_results_monitor_id' still exists in database: {index_names}"
    )

    # Composite index and checked_at index MUST be present
    assert "idx_ping_results_monitor_checked" in index_names, (
        f"Composite index 'idx_ping_results_monitor_checked' missing from database: {index_names}"
    )
    assert "ix_ping_results_checked_at" in index_names, (
        f"Timestamp index 'ix_ping_results_checked_at' missing from database: {index_names}"
    )
