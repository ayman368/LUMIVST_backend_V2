"""add screener_daily_trend_counts table

Revision ID: a1b2c3d4e5f8
Revises: c77f4f4ac74e
Create Date: 2026-05-24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "a1b2c3d4e5f8"
down_revision: Union[str, Sequence[str], None] = "c77f4f4ac74e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "screener_daily_trend_counts",
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("trend_1m", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("trend_4m", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("trend_5m_wide", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("alrayan", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("date"),
    )


def downgrade() -> None:
    op.drop_table("screener_daily_trend_counts")
