"""
Company Metric Display Settings model - for controlling what metrics are visible per company.
Allows per-company customization of metric visibility, order, and display labels.
"""

from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.sql import func
from app.core.database import Base


class CompanyMetricDisplaySetting(Base):
    __tablename__ = "company_metric_display_settings"

    id = Column(Integer, primary_key=True, index=True)
    
    # Foreign Keys
    company_symbol = Column(String(20), ForeignKey("companies.symbol"), nullable=False, index=True)
    metric_name = Column(
        String(150), 
        ForeignKey("financial_metric_categories.metric_name"), 
        nullable=False, 
        index=True
    )
    
    # Visibility control
    is_visible = Column(Boolean, default=True)  # Toggle display on/off
    
    # Custom display settings
    custom_display_order = Column(Integer, nullable=True)  # Override default order if set
    custom_display_label = Column(String(255), nullable=True)  # Custom label if different
    custom_unit = Column(String(50), nullable=True)  # Override unit if needed
    
    # Additional metadata
    notes = Column(String(500), nullable=True)  # Admin notes
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    
    # Unique constraint: one setting per company-metric pair
    __table_args__ = (
        UniqueConstraint(
            'company_symbol', 
            'metric_name', 
            name='uix_company_metric_display'
        ),
    )

    def __repr__(self):
        return f"<CompanyMetricDisplaySetting(symbol='{self.company_symbol}', metric='{self.metric_name}', visible={self.is_visible})>"
