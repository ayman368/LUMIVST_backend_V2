"""
Schemas for Financial Metrics Management APIs
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class FinancialMetricCategorySchema(BaseModel):
    """Schema for Financial Metric Category"""
    id: Optional[int] = None
    metric_name: str
    section: str  # 'income_statement', 'cash_flow', 'balance_sheet', 'ratios', 'other'
    subsection: Optional[str] = None
    description_en: Optional[str] = None
    description_ar: Optional[str] = None
    unit: str = "SAR"
    display_order: int = 0
    is_key_metric: bool = False
    is_calculated: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class CompanyMetricDisplaySettingSchema(BaseModel):
    """Schema for Company Metric Display Setting"""
    id: Optional[int] = None
    company_symbol: str
    metric_name: str
    is_visible: bool = True
    custom_display_order: Optional[int] = None
    custom_display_label: Optional[str] = None
    custom_unit: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class UpdateMetricDisplaySettingRequest(BaseModel):
    """Request schema for updating metric display settings"""
    is_visible: Optional[bool] = None
    custom_display_order: Optional[int] = None
    custom_display_label: Optional[str] = None
    custom_unit: Optional[str] = None
    notes: Optional[str] = None


class MetricsBySection(BaseModel):
    """Grouped metrics by section for frontend display"""
    section: str
    subsection: Optional[str] = None
    metrics: List[Dict[str, Any]]
    count: int


class CompanyFinancialMetricsResponse(BaseModel):
    """Response for company financial metrics with display settings applied"""
    symbol: str
    period: str
    year: int
    sections: List[MetricsBySection]
    total_visible_metrics: int
    total_metrics: int


class BulkUpdateDisplaySettingsRequest(BaseModel):
    """Request for bulk updating display settings"""
    section: Optional[str] = None  # If provided, apply to all metrics in this section
    metrics: Optional[List[str]] = None  # If provided, apply to these specific metrics
    is_visible: Optional[bool] = None
    display_order_offset: Optional[int] = None  # Offset to add to current display order


class MetricCategoryWithDisplaySettings(BaseModel):
    """Combines metric category with display settings for a specific company"""
    metric_name: str
    section: str
    subsection: Optional[str] = None
    description_en: Optional[str] = None
    unit: str
    display_order: int
    is_visible: bool
    custom_display_label: Optional[str] = None
    custom_unit: Optional[str] = None
    is_key_metric: bool = False

    class Config:
        from_attributes = True
