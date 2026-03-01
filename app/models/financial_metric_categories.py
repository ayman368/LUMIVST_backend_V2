"""
Financial Metric Categories model - for organizing and classifying metrics.
Categorizes metrics into sections (Income Statement, Cash Flow, Balance Sheet, etc.)
"""

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.sql import func
from app.core.database import Base


class FinancialMetricCategory(Base):
    __tablename__ = "financial_metric_categories"

    id = Column(Integer, primary_key=True, index=True)
    
    # Unique metric identifier (e.g., 'total_assets', 'net_income', 'cash_flow')
    metric_name = Column(String(150), unique=True, nullable=False, index=True)
    
    # Classification
    section = Column(
        String(50), 
        nullable=False, 
        index=True
    )  # Values: 'income_statement', 'cash_flow', 'balance_sheet', 'ratios', 'other'
    
    subsection = Column(
        String(100), 
        nullable=True, 
        index=True
    )  # Values: 'operating_activities', 'investing_activities', 'financing_activities', etc.
    
    # Display Information
    description_en = Column(String(500), nullable=True)
    description_ar = Column(String(500), nullable=True)
    
    # Unit/Format
    unit = Column(String(50), default='SAR')  # 'SAR', 'percentage', 'count', 'ratio', etc.
    
    # Display control
    display_order = Column(Integer, default=0)  # 0-indexed for sorting
    is_key_metric = Column(Boolean, default=False)  # Mark important metrics
    is_calculated = Column(Boolean, default=False)  # True if derived, False if from source
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    def __repr__(self):
        return f"<FinancialMetricCategory(metric_name='{self.metric_name}', section='{self.section}')>"
