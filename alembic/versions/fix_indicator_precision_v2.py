"""Increase precision of all indicator columns in stock_indicators table

Revision ID: fix_indicator_precision_v2
Revises: increase_price_precision
Create Date: 2026-03-07 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'fix_indicator_precision_v2'
down_revision = 'increase_price_precision'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade indicator columns from Numeric(5,2) and Numeric(10,2) to Numeric(12,4)"""
    
    # List of columns to migrate from Numeric(5, 2) to Numeric(12, 4)
    columns_from_5_2 = [
        'rsi_14', 'rsi_3',
        'sma9_rsi', 'wma45_rsi', 'ema45_rsi',
        'sma3_rsi3', 'ema20_sma3',
        'rsi_w', 'rsi_3_w', 'sma3_rsi3_w', 'sma9_rsi_w', 'wma45_rsi_w', 'ema45_rsi_w', 'ema20_sma3_w',
        'rsi_14_9days_ago', 'stamp_a_value',
        'stamp_s9rsi', 'stamp_e45cfg', 'stamp_e45rsi', 'stamp_e20sma3',
        'rsi_14_9days_ago_w', 'stamp_a_value_w',
        'stamp_s9rsi_w', 'stamp_e45cfg_w', 'stamp_e45rsi_w', 'stamp_e20sma3_w',
        'cfg_daily', 'cfg_sma4', 'cfg_sma9', 'cfg_sma20', 'cfg_ema20', 'cfg_ema45', 'cfg_wma45',
        'cfg_w', 'cfg_sma4_w', 'cfg_sma9_w', 'cfg_ema20_w', 'cfg_ema45_w', 'cfg_wma45_w',
        'rsi_14_9days_ago_cfg', 'rsi_14_minus_9', 'rsi_14_minus_9_w', 'rsi_14_w_shifted',
        'aroon_up', 'aroon_down', 'aroon_up_w', 'aroon_down_w',
    ]
    
    # List of columns to migrate from Numeric(10, 2) to Numeric(12, 4)
    columns_from_10_2 = [
        'close',
        'sma9_close', 'high_sma13', 'low_sma13', 'high_sma65', 'low_sma65',
        'the_number', 'the_number_hl', 'the_number_ll',
        'sma9_close_w', 'the_number_w', 'the_number_hl_w', 'the_number_ll_w',
        'high_sma13_w', 'low_sma13_w', 'high_sma65_w', 'low_sma65_w',
        'sma4', 'sma9', 'sma18', 'wma45_close',
        'close_w', 'sma4_w', 'sma9_w', 'sma18_w', 'wma45_close_w',
        'cci', 'cci_ema20', 'cci_w', 'cci_ema20_w',
    ]
    
    # Migrate columns from Numeric(5, 2) to Numeric(12, 4)
    for col in columns_from_5_2:
        op.alter_column('stock_indicators', col,
                       existing_type=sa.Numeric(precision=5, scale=2),
                       type_=sa.Numeric(precision=12, scale=4),
                       existing_nullable=True)
    
    # Migrate columns from Numeric(10, 2) to Numeric(12, 4)
    for col in columns_from_10_2:
        op.alter_column('stock_indicators', col,
                       existing_type=sa.Numeric(precision=10, scale=2),
                       type_=sa.Numeric(precision=12, scale=4),
                       existing_nullable=True)


def downgrade() -> None:
    """Downgrade indicator columns back to original precision"""
    
    columns_from_5_2 = [
        'rsi_14', 'rsi_3',
        'sma9_rsi', 'wma45_rsi', 'ema45_rsi',
        'sma3_rsi3', 'ema20_sma3',
        'rsi_w', 'rsi_3_w', 'sma3_rsi3_w', 'sma9_rsi_w', 'wma45_rsi_w', 'ema45_rsi_w', 'ema20_sma3_w',
        'rsi_14_9days_ago', 'stamp_a_value',
        'stamp_s9rsi', 'stamp_e45cfg', 'stamp_e45rsi', 'stamp_e20sma3',
        'rsi_14_9days_ago_w', 'stamp_a_value_w',
        'stamp_s9rsi_w', 'stamp_e45cfg_w', 'stamp_e45rsi_w', 'stamp_e20sma3_w',
        'cfg_daily', 'cfg_sma4', 'cfg_sma9', 'cfg_sma20', 'cfg_ema20', 'cfg_ema45', 'cfg_wma45',
        'cfg_w', 'cfg_sma4_w', 'cfg_sma9_w', 'cfg_ema20_w', 'cfg_ema45_w', 'cfg_wma45_w',
        'rsi_14_9days_ago_cfg', 'rsi_14_minus_9', 'rsi_14_minus_9_w', 'rsi_14_w_shifted',
        'aroon_up', 'aroon_down', 'aroon_up_w', 'aroon_down_w',
    ]
    
    columns_from_10_2 = [
        'close',
        'sma9_close', 'high_sma13', 'low_sma13', 'high_sma65', 'low_sma65',
        'the_number', 'the_number_hl', 'the_number_ll',
        'sma9_close_w', 'the_number_w', 'the_number_hl_w', 'the_number_ll_w',
        'high_sma13_w', 'low_sma13_w', 'high_sma65_w', 'low_sma65_w',
        'sma4', 'sma9', 'sma18', 'wma45_close',
        'close_w', 'sma4_w', 'sma9_w', 'sma18_w', 'wma45_close_w',
        'cci', 'cci_ema20', 'cci_w', 'cci_ema20_w',
    ]
    
    # Revert columns from Numeric(12, 4) back to Numeric(5, 2)
    for col in columns_from_5_2:
        op.alter_column('stock_indicators', col,
                       existing_type=sa.Numeric(precision=12, scale=4),
                       type_=sa.Numeric(precision=5, scale=2),
                       existing_nullable=True)
    
    # Revert columns from Numeric(12, 4) back to Numeric(10, 2)
    for col in columns_from_10_2:
        op.alter_column('stock_indicators', col,
                       existing_type=sa.Numeric(precision=12, scale=4),
                       type_=sa.Numeric(precision=10, scale=2),
                       existing_nullable=True)
