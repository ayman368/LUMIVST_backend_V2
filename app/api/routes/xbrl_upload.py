from pathlib import Path

from fastapi import APIRouter, File, HTTPException, UploadFile

from app.services import xbrl_data_service
from app.services.xbrl_parser import parse_and_merge_xbrl_files

router = APIRouter(prefix="/api/upload", tags=["XBRL Upload"])

UPLOAD_TMP_DIR = Path("app/data/uploads/tmp")


@router.post("/xbrl")
async def upload_xbrl(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    UPLOAD_TMP_DIR.mkdir(parents=True, exist_ok=True)
    saved_paths: list[Path] = []

    try:
        for file in files:
            if not file.filename:
                continue
            suffix = Path(file.filename).suffix.lower()
            if suffix not in {".xls", ".xlsx"}:
                raise HTTPException(status_code=400, detail=f"Unsupported file type: {file.filename}")
            target = UPLOAD_TMP_DIR / file.filename
            with open(target, "wb") as f:
                f.write(await file.read())
            saved_paths.append(target)

        if not saved_paths:
            raise HTTPException(status_code=400, detail="No valid files were uploaded")

        merged = parse_and_merge_xbrl_files(saved_paths)
        symbol = str(merged.get("meta", {}).get("symbol", "")).strip()
        if not symbol:
            raise HTTPException(status_code=422, detail="Unable to determine company symbol from uploaded files")

        output_path = xbrl_data_service.save_company(symbol, merged)
        periods = sorted(
            {
                period
                for section in merged.get("sections", {}).values()
                for period in section.get("periods", [])
            }
        )
        return {"symbol": symbol, "periods": periods, "output_file": str(output_path)}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to process XBRL upload: {exc}") from exc
