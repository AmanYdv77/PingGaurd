"""add_status_and_mode_check_constraints

Revision ID: 7a2e4b8c9d10
Revises: ea1acc2dea1b
Create Date: 2026-09-21 16:55:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7a2e4b8c9d10'
down_revision: Union[str, Sequence[str], None] = 'ea1acc2dea1b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply data cleanup and add named check constraints to monitors and ping_results."""
    # 1. Clean up any existing invalid monitor status values before applying constraint
    op.execute("UPDATE monitors SET status = 'down' WHERE status NOT IN ('up', 'degraded', 'down', 'pending')")

    # 2. Add named CHECK constraints
    op.create_check_constraint(
        constraint_name="ck_monitors_status",
        table_name="monitors",
        condition="status IN ('up', 'degraded', 'down', 'pending')",
    )
    op.create_check_constraint(
        constraint_name="ck_monitors_mode",
        table_name="monitors",
        condition="mode IN ('monitor', 'keep_alive', 'monitor_and_keep_alive')",
    )
    op.create_check_constraint(
        constraint_name="ck_ping_results_check_type",
        table_name="ping_results",
        condition="check_type IN ('monitor', 'keep_alive')",
    )


def downgrade() -> None:
    """Drop named check constraints."""
    op.drop_constraint("ck_ping_results_check_type", "ping_results", type_="check")
    op.drop_constraint("ck_monitors_mode", "monitors", type_="check")
    op.drop_constraint("ck_monitors_status", "monitors", type_="check")
