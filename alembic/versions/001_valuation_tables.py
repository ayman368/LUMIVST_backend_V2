"""Create valuation tables: eps_estimates, system_config, valuation_zones, tasi_components

Revision ID: 001_valuation_tables
Revises: (set this to your latest migration revision ID)
Create Date: 2026-06-13
"""

from alembic import op
import sqlalchemy as sa

revision = "001_valuation_tables"
down_revision = None   # <-- replace with your latest revision ID
branch_labels = None
depends_on = None


def upgrade():
    # ── eps_estimates ─────────────────────────────────────────────────────────
    op.create_table(
        "eps_estimates",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("value", sa.Numeric(10, 2), nullable=False),
        sa.Column("type", sa.String(20), nullable=True),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.Column("created_by", sa.String(100), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("year", name="uq_eps_year"),
    )

    # ── system_config ─────────────────────────────────────────────────────────
    op.create_table(
        "system_config",
        sa.Column("key", sa.String(100), nullable=False),
        sa.Column("value", sa.String(500), nullable=False),
        sa.Column("data_type", sa.String(20), nullable=True),
        sa.Column("description", sa.String(500), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("key"),
    )

    # ── valuation_zones ───────────────────────────────────────────────────────
    op.create_table(
        "valuation_zones",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("label", sa.String(100), nullable=False),
        sa.Column("label_ar", sa.String(100), nullable=True),
        sa.Column("price_from", sa.Numeric(10, 2), nullable=False),
        sa.Column("price_to", sa.Numeric(10, 2), nullable=False),
        sa.Column("return_pct_low", sa.Integer(), nullable=True),
        sa.Column("return_pct_high", sa.Integer(), nullable=True),
        sa.Column("color_code", sa.String(20), nullable=True),
        sa.Column("description", sa.String(500), nullable=True),
        sa.Column("order_seq", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # ── tasi_components ───────────────────────────────────────────────────────
    op.create_table(
        "tasi_components",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.String(10), nullable=False),
        sa.Column("company_name", sa.String(200), nullable=False),
        sa.Column("company_name_ar", sa.String(200), nullable=True),
        sa.Column("sector", sa.String(100), nullable=True),
        sa.Column("sector_ar", sa.String(100), nullable=True),
        sa.Column("current_price", sa.Numeric(10, 4), nullable=True),
        sa.Column("market_cap", sa.Numeric(20, 2), nullable=True),
        sa.Column("weight_in_index", sa.Numeric(10, 6), nullable=True),
        sa.Column("weight_adjusted", sa.Numeric(10, 6), nullable=True),
        sa.Column("eps", sa.Numeric(10, 4), nullable=True),
        sa.Column("pe_ratio", sa.Numeric(10, 2), nullable=True),
        sa.Column("dividend_yield", sa.Numeric(8, 4), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("symbol", name="uq_tasi_symbol"),
    )

    op.create_index("idx_tasi_symbol", "tasi_components", ["symbol"])
    op.create_index("idx_tasi_sector", "tasi_components", ["sector"])
    op.create_index("idx_tasi_weight", "tasi_components", ["weight_in_index"])


def downgrade():
    op.drop_index("idx_tasi_weight", table_name="tasi_components")
    op.drop_index("idx_tasi_sector", table_name="tasi_components")
    op.drop_index("idx_tasi_symbol", table_name="tasi_components")
    op.drop_table("tasi_components")
    op.drop_table("valuation_zones")
    op.drop_table("system_config")
    op.drop_table("eps_estimates")
