"""drop_redundant_monitor_id_index

Revision ID: 8d3f1a4e2c6b
Revises: 7a2e4b8c9d10
Create Date: 2026-09-22 17:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8d3f1a4e2c6b'
down_revision: Union[str, Sequence[str], None] = '7a2e4b8c9d10'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Drop redundant single-column index ix_ping_results_monitor_id.
    
    The composite index idx_ping_results_monitor_checked (monitor_id, checked_at)
    already has monitor_id as its leading left-prefix, making the single-column
    index redundant and unnecessary overhead for row insertions.
    """
    op.drop_index('ix_ping_results_monitor_id', table_name='ping_results')


def downgrade() -> None:
    """Recreate single-column index on monitor_id."""
    op.create_index('ix_ping_results_monitor_id', 'ping_results', ['monitor_id'], unique=False)
