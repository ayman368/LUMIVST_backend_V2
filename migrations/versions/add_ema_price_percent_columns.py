"""Add price_vs_ema_10/21_percent columns to prices table

Revision ID: add_ema_price_pct
Revises: recreate_initial_schema
Create Date: 2026-03-06

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_ema_price_pct'
down_revision = 'recreate_initial_schema'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add the new columns to the prices table
    op.add_column('prices', sa.Column('price_vs_ema_10_percent', sa.Numeric(8, 2), nullable=True))
    op.add_column('prices', sa.Column('price_vs_ema_21_percent', sa.Numeric(8, 2), nullable=True))


def downgrade() -> None:
    # Drop the columns if rolling back
    op.drop_column('prices', 'price_vs_ema_21_percent')
    op.drop_column('prices', 'price_vs_ema_10_percent')
