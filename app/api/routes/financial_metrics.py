from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import and_
from typing import List, Optional, Dict, Any
from app.core.database import get_db
from app.models.financial_metrics import CompanyFinancialMetric
from app.models.financial_metric_categories import FinancialMetricCategory
from app.models.company_metric_display_settings import CompanyMetricDisplaySetting
from app.schemas.financial_metrics_management import (
    FinancialMetricCategorySchema,
    CompanyMetricDisplaySettingSchema,
    UpdateMetricDisplaySettingRequest,
    BulkUpdateDisplaySettingsRequest,
    MetricCategoryWithDisplaySettings,
)
from pydantic import BaseModel
from collections import defaultdict

router = APIRouter()

# ==================== Schema Definitions ====================

class MetricResponse(BaseModel):
    id: int
    year: int
    period: str
    metric_name: str
    metric_value: Optional[float]
    metric_text: Optional[str]
    label_en: Optional[str]
    source_file: Optional[str]
    
    class Config:
        from_attributes = True


# ==================== Existing Endpoints ====================

@router.get("/compare", response_model=List[Dict[str, Any]])
def compare_companies(
    symbols: List[str] = Query(..., description="List of company symbols to compare"),
    metric_name: str = Query(..., description="Metric key to compare (e.g., net_profit)"),
    period: Optional[str] = Query('ANNUAL', description="Period to filter (Default: ANNUAL)"),
    years: Optional[List[int]] = Query(None, description="Specific years to compare"),
    db: Session = Depends(get_db)
):
    """
    Compare a specific metric across multiple companies.
    Returns a list of data points suitable for charting or tables.
    Example: Compare 'net_profit' for [1010, 1120] in 'ANNUAL' reports.
    """
    query = db.query(CompanyFinancialMetric).filter(
        CompanyFinancialMetric.company_symbol.in_(symbols),
        CompanyFinancialMetric.metric_name == metric_name,
        CompanyFinancialMetric.period == period
    )
    
    if years:
        query = query.filter(CompanyFinancialMetric.year.in_(years))
        
    results = query.order_by(CompanyFinancialMetric.year).all()
    
    # Transform into a structured list
    comparison_data = []
    for r in results:
        comparison_data.append({
            "symbol": r.company_symbol,
            "year": r.year,
            "period": r.period,
            "value": r.metric_value,
            "label": r.label_en
        })
        
    return comparison_data


# ==================== Metric Categories CRUD ====================

@router.get("/metric-categories", response_model=List[FinancialMetricCategorySchema], tags=["Metric Categories"])
def get_metric_categories(
    section: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """Get all metric categories, optionally filtered by section"""
    query = db.query(FinancialMetricCategory)
    
    if section:
        query = query.filter(FinancialMetricCategory.section == section)
    
    categories = query.order_by(
        FinancialMetricCategory.section,
        FinancialMetricCategory.display_order
    ).all()
    
    return categories


@router.get("/metric-categories/{metric_name}", response_model=FinancialMetricCategorySchema, tags=["Metric Categories"])
def get_metric_category(metric_name: str, db: Session = Depends(get_db)):
    """Get a specific metric category by name"""
    category = db.query(FinancialMetricCategory).filter(
        FinancialMetricCategory.metric_name == metric_name
    ).first()
    
    if not category:
        raise HTTPException(status_code=404, detail="Metric category not found")
    
    return category


@router.post("/metric-categories", response_model=FinancialMetricCategorySchema, tags=["Metric Categories"])
def create_metric_category(
    category: FinancialMetricCategorySchema,
    db: Session = Depends(get_db)
):
    """Create a new metric category"""
    # Check if already exists
    existing = db.query(FinancialMetricCategory).filter(
        FinancialMetricCategory.metric_name == category.metric_name
    ).first()
    
    if existing:
        raise HTTPException(status_code=409, detail="Metric category already exists")
    
    db_category = FinancialMetricCategory(**category.dict(exclude_unset=True))
    db.add(db_category)
    db.commit()
    db.refresh(db_category)
    
    return db_category


@router.put("/metric-categories/{metric_name}", response_model=FinancialMetricCategorySchema, tags=["Metric Categories"])
def update_metric_category(
    metric_name: str,
    category: FinancialMetricCategorySchema,
    db: Session = Depends(get_db)
):
    """Update a metric category"""
    db_category = db.query(FinancialMetricCategory).filter(
        FinancialMetricCategory.metric_name == metric_name
    ).first()
    
    if not db_category:
        raise HTTPException(status_code=404, detail="Metric category not found")
    
    update_data = category.dict(exclude_unset=True, exclude={'created_at'})
    for key, value in update_data.items():
        setattr(db_category, key, value)
    
    db.add(db_category)
    db.commit()
    db.refresh(db_category)
    
    return db_category


# ==================== Display Settings CRUD ====================

@router.get("/metric-settings/{symbol}", response_model=List[MetricCategoryWithDisplaySettings], tags=["Display Settings"])
def get_company_metric_settings(
    symbol: str,
    section: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """Get all metric display settings for a company"""
    # Get all metric categories
    categories_query = db.query(FinancialMetricCategory)
    if section:
        categories_query = categories_query.filter(FinancialMetricCategory.section == section)
    
    categories = categories_query.order_by(
        FinancialMetricCategory.section,
        FinancialMetricCategory.display_order
    ).all()
    
    # Get display settings for this company
    settings = db.query(CompanyMetricDisplaySetting).filter(
        CompanyMetricDisplaySetting.company_symbol == symbol
    ).all()
    
    settings_map = {s.metric_name: s for s in settings}
    
    # Merge categories with settings
    result = []
    for category in categories:
        setting = settings_map.get(category.metric_name)
        
        result.append(MetricCategoryWithDisplaySettings(
            metric_name=category.metric_name,
            section=category.section,
            subsection=category.subsection,
            description_en=category.description_en,
            unit=setting.custom_unit if setting and setting.custom_unit else category.unit,
            display_order=setting.custom_display_order if setting and setting.custom_display_order is not None else category.display_order,
            is_visible=setting.is_visible if setting else True,
            custom_display_label=setting.custom_display_label if setting else None,
            custom_unit=setting.custom_unit if setting else None,
            is_key_metric=category.is_key_metric,
        ))
    
    return result


@router.get("/metric-settings/{symbol}/{metric_name}", response_model=CompanyMetricDisplaySettingSchema, tags=["Display Settings"])
def get_company_metric_setting(
    symbol: str,
    metric_name: str,
    db: Session = Depends(get_db)
):
    """Get display setting for a specific metric in a company"""
    setting = db.query(CompanyMetricDisplaySetting).filter(
        and_(
            CompanyMetricDisplaySetting.company_symbol == symbol,
            CompanyMetricDisplaySetting.metric_name == metric_name
        )
    ).first()
    
    if not setting:
        # Return default (visible with no custom settings)
        return CompanyMetricDisplaySettingSchema(
            company_symbol=symbol,
            metric_name=metric_name,
            is_visible=True
        )
    
    return setting


@router.post("/metric-settings/{symbol}/{metric_name}", response_model=CompanyMetricDisplaySettingSchema, tags=["Display Settings"])
def create_company_metric_setting(
    symbol: str,
    metric_name: str,
    setting: UpdateMetricDisplaySettingRequest,
    db: Session = Depends(get_db)
):
    """Create or update display setting for a metric in a company"""
    # Check if metric category exists
    category = db.query(FinancialMetricCategory).filter(
        FinancialMetricCategory.metric_name == metric_name
    ).first()
    
    if not category:
        raise HTTPException(status_code=404, detail="Metric category not found")
    
    # Try to find existing setting
    db_setting = db.query(CompanyMetricDisplaySetting).filter(
        and_(
            CompanyMetricDisplaySetting.company_symbol == symbol,
            CompanyMetricDisplaySetting.metric_name == metric_name
        )
    ).first()
    
    if not db_setting:
        # Create new
        db_setting = CompanyMetricDisplaySetting(
            company_symbol=symbol,
            metric_name=metric_name,
            is_visible=setting.is_visible if setting.is_visible is not None else True,
            custom_display_order=setting.custom_display_order,
            custom_display_label=setting.custom_display_label,
            custom_unit=setting.custom_unit,
            notes=setting.notes
        )
    else:
        # Update existing
        update_data = setting.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(db_setting, key, value)
    
    db.add(db_setting)
    db.commit()
    db.refresh(db_setting)
    
    return db_setting


@router.put("/metric-settings/{symbol}/{metric_name}", response_model=CompanyMetricDisplaySettingSchema, tags=["Display Settings"])
def update_company_metric_setting(
    symbol: str,
    metric_name: str,
    setting: UpdateMetricDisplaySettingRequest,
    db: Session = Depends(get_db)
):
    """Update display setting for a metric in a company"""
    db_setting = db.query(CompanyMetricDisplaySetting).filter(
        and_(
            CompanyMetricDisplaySetting.company_symbol == symbol,
            CompanyMetricDisplaySetting.metric_name == metric_name
        )
    ).first()
    
    if not db_setting:
        raise HTTPException(status_code=404, detail="Setting not found, use POST to create")
    
    update_data = setting.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_setting, key, value)
    
    db.add(db_setting)
    db.commit()
    db.refresh(db_setting)
    
    return db_setting


@router.delete("/metric-settings/{symbol}/{metric_name}", status_code=204, tags=["Display Settings"])
def delete_company_metric_setting(
    symbol: str,
    metric_name: str,
    db: Session = Depends(get_db)
):
    """Delete display setting for a metric (revert to defaults)"""
    db_setting = db.query(CompanyMetricDisplaySetting).filter(
        and_(
            CompanyMetricDisplaySetting.company_symbol == symbol,
            CompanyMetricDisplaySetting.metric_name == metric_name
        )
    ).first()
    
    if db_setting:
        db.delete(db_setting)
        db.commit()
    
    return None


@router.post("/metric-settings/{symbol}/bulk-update", response_model=Dict[str, Any], tags=["Display Settings"])
def bulk_update_display_settings(
    symbol: str,
    request: BulkUpdateDisplaySettingsRequest,
    db: Session = Depends(get_db)
):
    """Bulk update display settings for multiple metrics"""
    # Get metrics to update
    query = db.query(FinancialMetricCategory)
    
    if request.section:
        query = query.filter(FinancialMetricCategory.section == request.section)
    
    if request.metrics:
        query = query.filter(FinancialMetricCategory.metric_name.in_(request.metrics))
    
    metrics = query.all()
    
    if not metrics:
        raise HTTPException(status_code=404, detail="No metrics found matching criteria")
    
    updated_count = 0
    
    for metric in metrics:
        setting = db.query(CompanyMetricDisplaySetting).filter(
            and_(
                CompanyMetricDisplaySetting.company_symbol == symbol,
                CompanyMetricDisplaySetting.metric_name == metric.metric_name
            )
        ).first()
        
        if not setting:
            setting = CompanyMetricDisplaySetting(
                company_symbol=symbol,
                metric_name=metric.metric_name
            )
        
        if request.is_visible is not None:
            setting.is_visible = request.is_visible
        
        if request.display_order_offset is not None and setting.custom_display_order is None:
            setting.custom_display_order = metric.display_order + request.display_order_offset
        
        db.add(setting)
        updated_count += 1
    
    db.commit()
    
    return {
        "success": True,
        "updated_count": updated_count,
        "message": f"Updated {updated_count} metric display settings"
    }


# ==================== Financial Data with Display Settings ====================

@router.get("/{symbol}/data-by-section", tags=["Financial Data"])
def get_company_financial_data_by_section(
    symbol: str,
    year: Optional[int] = Query(None),
    period: Optional[str] = Query(None),
    period_type: Optional[str] = Query(None, description="'annual', 'quarterly', or 'all'"),
    db: Session = Depends(get_db)
):
    """
    Get company financial data grouped by section with display settings applied.
    
    Query params:
    - year: Filter by specific year (optional)
    - period: Filter by specific period - ANNUAL, Q1, Q2, Q3, Q4 (optional)
    - period_type: Filter by 'annual' or 'quarterly' 
    """
    from sqlalchemy.orm import joinedload
    from sqlalchemy import func
    
    # Get metrics with JOINs to avoid N+1 queries
    query = db.query(CompanyFinancialMetric).filter(
        CompanyFinancialMetric.company_symbol == symbol
    )
    
    if year:
        query = query.filter(CompanyFinancialMetric.year == year)
    
    if period:
        query = query.filter(CompanyFinancialMetric.period == period)
        
    if period_type:
        ptype = period_type.lower()
        if ptype == 'annual':
            query = query.filter(func.lower(CompanyFinancialMetric.period) == 'annual')
        elif ptype == 'quarterly':
            query = query.filter(func.lower(CompanyFinancialMetric.period) != 'annual')
            
    metrics = query.order_by(
        CompanyFinancialMetric.year.desc(),
        CompanyFinancialMetric.period
    ).all()
    
    if not metrics:
        return {}
    
    # Get display settings and categories in one go (only for metrics we have)
    metric_names = set(m.metric_name for m in metrics)
    
    settings = db.query(CompanyMetricDisplaySetting).filter(
        CompanyMetricDisplaySetting.company_symbol == symbol,
        CompanyMetricDisplaySetting.metric_name.in_(metric_names)
    ).all()
    settings_map = {s.metric_name: s for s in settings}
    
    categories = db.query(FinancialMetricCategory).filter(
        FinancialMetricCategory.metric_name.in_(metric_names)
    ).all()
    categories_map = {c.metric_name: c for c in categories}
    
    # Group by period then by section
    result = defaultdict(lambda: defaultdict(list))
    
    for metric in metrics:
        # Check if should be displayed
        setting = settings_map.get(metric.metric_name)
        if setting and not setting.is_visible:
            continue
        
        # Get category info
        category = categories_map.get(metric.metric_name)
        section = category.section if category else 'other'
        
        source_str = metric.source_file if metric.source_file else "unknown"
        period_key = f"{metric.year} {metric.period}"
        
        metric_obj = {
            "key": metric.metric_name,
            "label": setting.custom_display_label if setting and setting.custom_display_label else metric.label_en,
            "value": metric.metric_value,
            "text": metric.metric_text,
        }
        
        result[period_key][section].append(metric_obj)
    
    # Convert to dict structure expected by frontend
    output = {}
    for period_key, sections in result.items():
        output[period_key] = dict(sections)
    
    return output


@router.get("/{symbol}/metrics-summary", tags=["Financial Data"])
def get_company_metrics_summary(symbol: str, db: Session = Depends(get_db)):
    """Get summary statistics about company metrics and display settings"""
    # Total metrics
    total_metrics = db.query(CompanyFinancialMetric).filter(
        CompanyFinancialMetric.company_symbol == symbol
    ).count()
    
    # Metrics by section
    metrics_by_section = db.query(
        FinancialMetricCategory.section,
        CompanyFinancialMetric.metric_name
    ).join(
        CompanyFinancialMetric,
        FinancialMetricCategory.metric_name == CompanyFinancialMetric.metric_name
    ).filter(
        CompanyFinancialMetric.company_symbol == symbol
    ).distinct().all()
    
    section_counts = defaultdict(int)
    for section, _ in metrics_by_section:
        section_counts[section] += 1
    
    # Display settings
    visible_count = db.query(CompanyMetricDisplaySetting).filter(
        and_(
            CompanyMetricDisplaySetting.company_symbol == symbol,
            CompanyMetricDisplaySetting.is_visible == True
        )
    ).count()
    
    hidden_count = db.query(CompanyMetricDisplaySetting).filter(
        and_(
            CompanyMetricDisplaySetting.company_symbol == symbol,
            CompanyMetricDisplaySetting.is_visible == False
        )
    ).count()
    
    return {
        "symbol": symbol,
        "total_metrics": total_metrics,
        "metrics_by_section": dict(section_counts),
        "visible_custom_settings": visible_count,
        "hidden_custom_settings": hidden_count,
    }
