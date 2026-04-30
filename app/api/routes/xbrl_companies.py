from fastapi import APIRouter, HTTPException

from app.schemas.xbrl_financials import CompanyFinancials, CompanyListItem
from app.services import xbrl_data_service

router = APIRouter(prefix="/api/companies", tags=["XBRL Companies"])


@router.get("/", response_model=list[CompanyListItem])
def list_companies():
    return xbrl_data_service.list_companies()


@router.get("/{symbol}", response_model=CompanyFinancials)
def get_company(symbol: str):
    company = xbrl_data_service.get_company(symbol)
    if not company:
        raise HTTPException(404, detail=f"Company '{symbol}' not found")
    return company


@router.get("/{symbol}/sections")
def get_sections(symbol: str):
    company = xbrl_data_service.get_company(symbol)
    if not company:
        raise HTTPException(404, detail=f"Company '{symbol}' not found")
    return {"symbol": symbol, "sections": list(company.sections.keys())}


@router.get("/{symbol}/sections/{section_key}")
def get_section(symbol: str, section_key: str):
    company = xbrl_data_service.get_company(symbol)
    if not company:
        raise HTTPException(404, detail=f"Company '{symbol}' not found")
    section = company.sections.get(section_key)
    if not section:
        raise HTTPException(
            404,
            detail=f"Section '{section_key}' not found. Available: {list(company.sections.keys())}",
        )
    return {"symbol": symbol, "section": section_key, "meta": company.meta, **section.model_dump()}
