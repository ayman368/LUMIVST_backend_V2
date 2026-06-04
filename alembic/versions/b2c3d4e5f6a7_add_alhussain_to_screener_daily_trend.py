"""add alhussain column to screener_daily_trend_counts

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f8
Create Date: 2026-06-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "screener_daily_trend_counts",
        sa.Column("alhussain", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("screener_daily_trend_counts", "alhussain")
