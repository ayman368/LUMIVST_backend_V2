from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, UniqueConstraint, ForeignKey
from sqlalchemy.sql import func
from app.core.database import Base

class CompanyFinancialMetric(Base):
    __tablename__ = "company_financial_metrics"

    id = Column(Integer, primary_key=True, index=True)
    
    # Company and Time Period
    company_symbol = Column(String(20), index=True, nullable=False)
    year = Column(Integer, index=True, nullable=False)
    period = Column(String(20), index=True, nullable=False)  # 'ANNUAL', 'Q1', 'Q2', 'Q3', 'Q4'
    
    # The metric name (cleaned, snake_case) e.g., 'total_assets', 'net_profit'
    # Foreign key to financial_metric_categories
    metric_name = Column(
        String(150), 
        ForeignKey("financial_metric_categories.metric_name"), 
        index=True, 
        nullable=False
    )
    
    # Values: numeric and/or text
    metric_value = Column(Float, nullable=True)  # Numeric value
    metric_text = Column(String, nullable=True)  # Text value (for non-numeric metrics)
    
    # Display Information
    label_en = Column(String(500), nullable=True)  # Original label from source
    label_ar = Column(String(500), nullable=True)  # Arabic label if available
    
    # Data Quality
    data_quality_score = Column(Float, default=1.0)  # 0.0 to 1.0
    is_verified = Column(Boolean, default=False)
    verification_date = Column(DateTime(timezone=True), nullable=True)
    
    # Source Information
    source_file = Column(String, nullable=True)
    source_date = Column(DateTime(timezone=True), server_default=func.now())
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
