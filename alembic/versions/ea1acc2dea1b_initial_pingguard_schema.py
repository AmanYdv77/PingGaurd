"""initial_pingguard_schema

Revision ID: ea1acc2dea1b
Revises: 
Create Date: 2026-09-13 22:16:32.559470

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ea1acc2dea1b'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create monitors and ping_results tables with indexes and foreign keys."""
    # 1. Create 'monitors' table
    op.create_table(
        'monitors',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(length=120), nullable=False),
        sa.Column('url', sa.String(length=2048), nullable=False),
        sa.Column('check_interval_seconds', sa.Integer(), server_default='60', nullable=False),
        sa.Column('status', sa.String(length=20), server_default='pending', nullable=False),
        sa.Column('last_checked_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('next_check_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('mode', sa.String(length=30), server_default='monitor', nullable=False),
        sa.Column('keep_alive_enabled', sa.Boolean(), server_default=sa.text('false'), nullable=False),
        sa.Column('keep_alive_interval_seconds', sa.Integer(), nullable=True),
        sa.Column('keep_alive_path', sa.String(length=255), nullable=True),
        sa.Column('next_keep_alive_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_monitors')),
    )
    op.create_index(op.f('ix_monitors_next_check_at'), 'monitors', ['next_check_at'], unique=False)
    op.create_index(op.f('ix_monitors_next_keep_alive_at'), 'monitors', ['next_keep_alive_at'], unique=False)

    # 2. Create 'ping_results' table
    op.create_table(
        'ping_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('monitor_id', sa.Integer(), nullable=False),
        sa.Column('check_type', sa.String(length=20), server_default='monitor', nullable=False, comment="'monitor' for health checks, 'keep_alive' for wake-up pings"),
        sa.Column('status_code', sa.Integer(), nullable=True),
        sa.Column('latency_ms', sa.Float(), nullable=True),
        sa.Column('error', sa.String(length=500), nullable=True),
        sa.Column('checked_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['monitor_id'], ['monitors.id'], name=op.f('fk_ping_results_monitor_id_monitors'), ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_ping_results')),
    )
    op.create_index(op.f('ix_ping_results_checked_at'), 'ping_results', ['checked_at'], unique=False)
    op.create_index(op.f('ix_ping_results_monitor_id'), 'ping_results', ['monitor_id'], unique=False)
    op.create_index('idx_ping_results_monitor_checked', 'ping_results', ['monitor_id', 'checked_at'], unique=False)


def downgrade() -> None:
    """Drop ping_results and monitors tables with indexes."""
    op.drop_index('idx_ping_results_monitor_checked', table_name='ping_results')
    op.drop_index(op.f('ix_ping_results_monitor_id'), table_name='ping_results')
    op.drop_index(op.f('ix_ping_results_checked_at'), table_name='ping_results')
    op.drop_table('ping_results')
    op.drop_index(op.f('ix_monitors_next_keep_alive_at'), table_name='monitors')
    op.drop_index(op.f('ix_monitors_next_check_at'), table_name='monitors')
    op.drop_table('monitors')
