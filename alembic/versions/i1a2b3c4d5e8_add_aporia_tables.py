"""add_aporia_analytics_and_charts_tables

Revision ID: i1a2b3c4d5e8
Revises: 001_valuation_tables, b2c3d4e5f6a7, fix_indicator_precision_v2, h1a2b3c4d5e7
Create Date: 2026-07-19 08:35:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'i1a2b3c4d5e8'
down_revision: Union[str, Sequence[str], None] = ('001_valuation_tables', 'b2c3d4e5f6a7', 'fix_indicator_precision_v2', 'h1a2b3c4d5e7')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create aporia_analytics table
    op.create_table('aporia_analytics',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('filter_category', sa.String(), nullable=True),
        sa.Column('ticker', sa.String(), nullable=True),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('sector', sa.String(), nullable=True),
        sa.Column('market_cap', sa.String(), nullable=True),
        sa.Column('val_avg_3mo', sa.String(), nullable=True),
        sa.Column('trailingPE', sa.String(), nullable=True),
        sa.Column('last', sa.String(), nullable=True),
        sa.Column('mtd_rtn', sa.String(), nullable=True),
        sa.Column('mo3_rtn', sa.String(), nullable=True),
        sa.Column('year_rtn', sa.String(), nullable=True),
        sa.Column('daily_trend', sa.String(), nullable=True),
        sa.Column('weekly_trend', sa.String(), nullable=True),
        sa.Column('monthly_trend', sa.String(), nullable=True),
        sa.Column('trend_rank', sa.String(), nullable=True),
        sa.Column('pfh_250', sa.String(), nullable=True),
        sa.Column('days_since_high_250', sa.String(), nullable=True),
        sa.Column('breakout', sa.String(), nullable=True),
        sa.Column('longest_consolidation_window', sa.String(), nullable=True),
        sa.Column('position', sa.String(), nullable=True),
        sa.Column('price_extreme', sa.String(), nullable=True),
        sa.Column('vol_5_day_chng', sa.String(), nullable=True),
        sa.Column('vol_20_day_chng', sa.String(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_aporia_analytics_id'), 'aporia_analytics', ['id'], unique=False)
    op.create_index(op.f('ix_aporia_analytics_filter_category'), 'aporia_analytics', ['filter_category'], unique=False)
    op.create_index(op.f('ix_aporia_analytics_ticker'), 'aporia_analytics', ['ticker'], unique=False)

    # Create aporia_charts table
    op.create_table('aporia_charts',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('ticker', sa.String(), nullable=True),
        sa.Column('chart_type', sa.String(), nullable=True),
        sa.Column('chart_data', sa.JSON(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_aporia_charts_id'), 'aporia_charts', ['id'], unique=False)
    op.create_index(op.f('ix_aporia_charts_ticker'), 'aporia_charts', ['ticker'], unique=False)
    op.create_index(op.f('ix_aporia_charts_chart_type'), 'aporia_charts', ['chart_type'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_aporia_charts_chart_type'), table_name='aporia_charts')
    op.drop_index(op.f('ix_aporia_charts_ticker'), table_name='aporia_charts')
    op.drop_index(op.f('ix_aporia_charts_id'), table_name='aporia_charts')
    op.drop_table('aporia_charts')
    
    op.drop_index(op.f('ix_aporia_analytics_ticker'), table_name='aporia_analytics')
    op.drop_index(op.f('ix_aporia_analytics_filter_category'), table_name='aporia_analytics')
    op.drop_index(op.f('ix_aporia_analytics_id'), table_name='aporia_analytics')
    op.drop_table('aporia_analytics')
