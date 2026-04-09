from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request
from fastapi.responses import JSONResponse
from app.core.config import settings
from urllib.parse import urlparse

class CSRFMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method in ["POST", "PUT", "DELETE", "PATCH"]:
            path = request.url.path

            excluded_prefixes = [
                "/api/scraper",
                "/api/public",
            ]

            if not any(path.startswith(prefix) for prefix in excluded_prefixes):
                csrf_header = request.headers.get("x-csrf-token")
                if not csrf_header:
                    return JSONResponse(
                        status_code=403,
                        content={"detail": "CSRF verification failed. Missing x-csrf-token header."}
                    )

                origin = request.headers.get("origin")
                referrer = request.headers.get("referer")
                allowed_origins = settings.ALLOWED_ORIGINS

                if allowed_origins != ["*"]:
                    origin_valid = False

                    if origin:
                        origin_valid = origin in allowed_origins
                    elif referrer:
                        try:
                            parsed_referrer = urlparse(referrer)
                            referrer_origin = f"{parsed_referrer.scheme}://{parsed_referrer.netloc}"
                            origin_valid = referrer_origin in allowed_origins
                        except Exception:
                            origin_valid = False
                    else:
                        origin_valid = True

                    if not origin_valid:
                        return JSONResponse(
                            status_code=403,
                            content={"detail": "CSRF verification failed. Invalid origin or referrer."}
                        )

        return await call_next(request)
