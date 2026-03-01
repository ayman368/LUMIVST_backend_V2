"""Add financial metric categories and display settings tables

Revision ID: f1a2b3c4d5e6
Revises: e8905d5da169
Create Date: 2026-02-26 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'f1a2b3c4d5e6'
down_revision: Union[str, Sequence[str], None] = 'e8905d5da169'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - create new tables for metric categories and display settings."""
    
    # Create financial_metric_categories table
    op.create_table(
        'financial_metric_categories',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('metric_name', sa.String(length=150), nullable=False),
        sa.Column('section', sa.String(length=50), nullable=False),
        sa.Column('subsection', sa.String(length=100), nullable=True),
        sa.Column('description_en', sa.String(length=500), nullable=True),
        sa.Column('description_ar', sa.String(length=500), nullable=True),
        sa.Column('unit', sa.String(length=50), server_default='SAR', nullable=True),
        sa.Column('display_order', sa.Integer(), server_default='0', nullable=True),
        sa.Column('is_key_metric', sa.Boolean(), server_default='false', nullable=True),
        sa.Column('is_calculated', sa.Boolean(), server_default='false', nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_financial_metric_categories')),
        sa.UniqueConstraint('metric_name', name=op.f('uq_financial_metric_categories_metric_name'))
    )
    op.create_index(op.f('ix_financial_metric_categories_metric_name'), 'financial_metric_categories', ['metric_name'], unique=False)
    op.create_index(op.f('ix_financial_metric_categories_section'), 'financial_metric_categories', ['section'], unique=False)
    op.create_index(op.f('ix_financial_metric_categories_subsection'), 'financial_metric_categories', ['subsection'], unique=False)

    # Create company_metric_display_settings table
    op.create_table(
        'company_metric_display_settings',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('company_symbol', sa.String(length=20), nullable=False),
        sa.Column('metric_name', sa.String(length=150), nullable=False),
        sa.Column('is_visible', sa.Boolean(), server_default='true', nullable=True),
        sa.Column('custom_display_order', sa.Integer(), nullable=True),
        sa.Column('custom_display_label', sa.String(length=255), nullable=True),
        sa.Column('custom_unit', sa.String(length=50), nullable=True),
        sa.Column('notes', sa.String(length=500), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=True),
        sa.ForeignKeyConstraint(['company_symbol'], ['companies.symbol'], name=op.f('fk_company_metric_display_settings_company_symbol_companies')),
        sa.ForeignKeyConstraint(['metric_name'], ['financial_metric_categories.metric_name'], name=op.f('fk_company_metric_display_settings_metric_name_financial_metric_categories')),
        sa.PrimaryKeyConstraint('id', name=op.f('pk_company_metric_display_settings')),
        sa.UniqueConstraint('company_symbol', 'metric_name', name=op.f('uix_company_metric_display'))
    )
    op.create_index(op.f('ix_company_metric_display_settings_company_symbol'), 'company_metric_display_settings', ['company_symbol'], unique=False)
    op.create_index(op.f('ix_company_metric_display_settings_metric_name'), 'company_metric_display_settings', ['metric_name'], unique=False)

    # Add new columns to company_financial_metrics table
    op.add_column('company_financial_metrics', sa.Column('label_ar', sa.String(length=500), nullable=True))
    op.add_column('company_financial_metrics', sa.Column('data_quality_score', sa.Float(), server_default='1.0', nullable=True))
    op.add_column('company_financial_metrics', sa.Column('is_verified', sa.Boolean(), server_default='false', nullable=True))
    op.add_column('company_financial_metrics', sa.Column('verification_date', sa.DateTime(timezone=True), nullable=True))
    op.add_column('company_financial_metrics', sa.Column('source_date', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True))
    op.add_column('company_financial_metrics', sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True))
    op.add_column('company_financial_metrics', sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=True))

    # Update metric_name column to have foreign key
    op.alter_column('company_financial_metrics', 'metric_name',
               existing_type=sa.String(),
               type_=sa.String(length=150),
               existing_nullable=True,
               nullable=False)
    
    # Add foreign key constraint to metric_name
    op.create_foreign_key(
        op.f('fk_company_financial_metrics_metric_name_financial_metric_categories'),
        'company_financial_metrics', 'financial_metric_categories',
        ['metric_name'], ['metric_name']
    )

    # Update company_symbol column
    op.alter_column('company_financial_metrics', 'company_symbol',
               existing_type=sa.String(),
               type_=sa.String(length=20),
               existing_nullable=True,
               nullable=False)
    
    # Update year and period columns to be non-nullable
    op.alter_column('company_financial_metrics', 'year',
               existing_type=sa.Integer(),
               existing_nullable=True,
               nullable=False)
    op.alter_column('company_financial_metrics', 'period',
               existing_type=sa.String(length=20),
               existing_nullable=True,
               nullable=False)
    
    # Update label_en column length
    op.alter_column('company_financial_metrics', 'label_en',
               existing_type=sa.String(),
               type_=sa.String(length=500),
               existing_nullable=True)


def downgrade() -> None:
    """Downgrade schema - remove new tables and revert changes."""
    
    # Revert company_financial_metrics changes
    op.drop_constraint(op.f('fk_company_financial_metrics_metric_name_financial_metric_categories'), 'company_financial_metrics', type_='foreignkey')
    
    op.alter_column('company_financial_metrics', 'label_en',
               existing_type=sa.String(length=500),
               type_=sa.String(),
               existing_nullable=True)
    op.alter_column('company_financial_metrics', 'period',
               existing_type=sa.String(length=20),
               existing_nullable=False,
               nullable=True)
    op.alter_column('company_financial_metrics', 'year',
               existing_type=sa.Integer(),
               existing_nullable=False,
               nullable=True)
    op.alter_column('company_financial_metrics', 'company_symbol',
               existing_type=sa.String(length=20),
               type_=sa.String(),
               existing_nullable=False,
               nullable=True)
    op.alter_column('company_financial_metrics', 'metric_name',
               existing_type=sa.String(length=150),
               type_=sa.String(),
               existing_nullable=False,
               nullable=True)
    
    op.drop_column('company_financial_metrics', 'updated_at')
    op.drop_column('company_financial_metrics', 'created_at')
    op.drop_column('company_financial_metrics', 'source_date')
    op.drop_column('company_financial_metrics', 'verification_date')
    op.drop_column('company_financial_metrics', 'is_verified')
    op.drop_column('company_financial_metrics', 'data_quality_score')
    op.drop_column('company_financial_metrics', 'label_ar')

    # Drop company_metric_display_settings table
    op.drop_index(op.f('ix_company_metric_display_settings_metric_name'), table_name='company_metric_display_settings')
    op.drop_index(op.f('ix_company_metric_display_settings_company_symbol'), table_name='company_metric_display_settings')
    op.drop_table('company_metric_display_settings')

    # Drop financial_metric_categories table
    op.drop_index(op.f('ix_financial_metric_categories_subsection'), table_name='financial_metric_categories')
    op.drop_index(op.f('ix_financial_metric_categories_section'), table_name='financial_metric_categories')
    op.drop_index(op.f('ix_financial_metric_categories_metric_name'), table_name='financial_metric_categories')
    op.drop_table('financial_metric_categories')
