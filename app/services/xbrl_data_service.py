"""
Loads company JSON files from the output directory.
In production, replace file I/O with object storage or database reads.
"""

import json
import os
from pathlib import Path

from app.schemas.xbrl_financials import CompanyFinancials, CompanyListItem

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))


def _all_json_files() -> list[Path]:
    if not OUTPUT_DIR.exists():
        return []
    return sorted(OUTPUT_DIR.glob("*_financials.json"))


def list_companies() -> list[CompanyListItem]:
    result: list[CompanyListItem] = []
    for fp in _all_json_files():
        try:
            with open(fp, encoding="utf-8") as f:
                data = json.load(f)
            meta = data.get("meta", {})
            all_periods: set[str] = set()
            for sec in data.get("sections", {}).values():
                all_periods.update(sec.get("periods", []))
            result.append(
                CompanyListItem(
                    symbol=meta.get("symbol", fp.stem.split("_")[0]),
                    company_name=meta.get("company_name", "Unknown"),
                    sector=meta.get("sector"),
                    report_end=meta.get("report_end"),
                    periods_count=len(all_periods),
                )
            )
        except Exception:
            continue
    return result


def get_company(symbol: str) -> CompanyFinancials | None:
    fp = OUTPUT_DIR / f"{symbol}_financials.json"
    if not fp.exists():
        return None
    with open(fp, encoding="utf-8") as f:
        raw = json.load(f)
    return CompanyFinancials(**raw)


def save_company(symbol: str, data: dict) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fp = OUTPUT_DIR / f"{symbol}_financials.json"
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return fp
