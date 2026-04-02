"""add_static_stock_fields

Revision ID: h1a2b3c4d5e7
Revises: g1a2b3c4d5e6
Create Date: 2026-04-01 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'h1a2b3c4d5e7'
down_revision: Union[str, Sequence[str], None] = 'g1a2b3c4d5e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('prices', sa.Column('approval_with_controls', sa.String(length=150), nullable=True))
    op.add_column('prices', sa.Column('purge_amount', sa.Numeric(precision=18, scale=6), nullable=True))
    op.add_column('prices', sa.Column('marginable_percent', sa.Numeric(precision=10, scale=4), nullable=True))


def downgrade() -> None:
    op.drop_column('prices', 'marginable_percent')
    op.drop_column('prices', 'purge_amount')
    op.drop_column('prices', 'approval_with_controls')
