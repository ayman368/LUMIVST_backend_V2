from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes import stocks, financials, cache, auth, contact, rs, rs_v2, admin, scraper, official_filings, financial_details, prices, technical_screener, financial_metrics, screeners, market_breadth, market_reports

# ... (Previous code)

from app.core.redis import redis_cache
from app.core.database import create_tables
from app.services.rs_rating import calculate_all_rs_ratings
import asyncio
import logging
import os
from app.core.config import settings 

try:
    from pythonjsonlogger import jsonlogger

    log_handler = logging.StreamHandler()
    log_handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root_logger = logging.getLogger()
    root_logger.handlers = [log_handler]
    root_logger.setLevel(logging.INFO)
except Exception:
    logging.basicConfig(level=logging.INFO)

SENTRY_DSN = os.getenv("SENTRY_DSN")
if SENTRY_DSN:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

        sentry_sdk.init(
            dsn=SENTRY_DSN,
            environment=settings.ENVIRONMENT,
            traces_sample_rate=float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.1")),
            integrations=[FastApiIntegration(), SqlalchemyIntegration()],
        )
    except Exception:
        logging.getLogger(__name__).exception("Failed to initialize Sentry")

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
except Exception:
    logging.getLogger(__name__).warning("OpenTelemetry runtime not initialized")

app = FastAPI(
    title="Saudi Stocks API",
    description="API for Saudi Stock Market data",
    version="1.0.0",
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None
)

from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from app.core.limiter import limiter
from app.core.csrf import CSRFMiddleware

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(CSRFMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def _calculate_and_save_rs(symbols: list):
    """دالة خلفية لحساب RS وحفظه في DB"""
    try:
        rs_data = await calculate_all_rs_ratings(symbols)
        
        if not rs_data:
            print("❌ No RS data calculated")
            return
        
        from app.core.database import get_db
        from app.models.quote import StockQuote
        
        db = next(get_db())
        
        try:
            for symbol, rs_scores in rs_data.items():
                quote = db.query(StockQuote).filter(StockQuote.symbol == symbol).first()
                if quote:
                    for key, value in rs_scores.items():
                        if hasattr(quote, key):
                            setattr(quote, key, value)
            
            db.commit()
            print(f"✅ RS ratings saved to DB for {len(rs_data)} stocks")
            
        except Exception as e:
            db.rollback()
            print(f"❌ Error saving RS to DB: {e}")
        finally:
            db.close()
            
        await redis_cache.delete("tadawul:all:Saudi Arabia")
        
    except Exception as e:
        print(f"❌ Error in background RS calculation: {e}")

# Register routers
# Register routers
app.include_router(auth.router, prefix="/api", tags=["auth"])
app.include_router(contact.router, prefix="/api")

# Protected Routers (Require Authentication)
from fastapi import Depends
from app.api.deps import get_current_admin, get_current_user

protected_dependencies = [Depends(get_current_user)]
admin_dependencies = [Depends(get_current_admin)]

app.include_router(stocks.router, dependencies=protected_dependencies)
app.include_router(financials.router, dependencies=protected_dependencies)
app.include_router(cache.router, dependencies=protected_dependencies)

app.include_router(rs.router, prefix="/api", dependencies=protected_dependencies)  # RS V1 endpoints
app.include_router(rs_v2.router, prefix="/api", dependencies=protected_dependencies)  # RS V2 endpoints
app.include_router(admin.router, prefix="/api", dependencies=admin_dependencies) # /api/admin/*
app.include_router(scraper.router)  # /api/scraper/*
app.include_router(official_filings.router, prefix="/api") # /api/ingest/official-reports & /api/reports/{symbol}
app.include_router(financial_details.router, prefix="/api/financial-details", tags=["Financial Details"])
app.include_router(prices.router, prefix="/api") # /api/prices/latest
from app.api.routes import industry_groups
app.include_router(industry_groups.router, prefix="/api/industry-groups", tags=["Industry Groups"])
app.include_router(technical_screener.router, prefix="/api", dependencies=protected_dependencies)  # Technical Screener
app.include_router(screeners.router, prefix="/api")  # Stock Screeners (PUBLIC)
app.include_router(financial_metrics.router, prefix="/api/financial-metrics", tags=["Financial Metrics"])  # /api/financial-metrics/*
app.include_router(market_breadth.router, prefix="/api")  # /api/market-breadth/*
app.include_router(market_reports.router, prefix="/api/market-reports", tags=["Market Reports"])

# Event handlers
@app.on_event("startup")
async def startup_event():
    print("🚀 Starting Saudi Stocks API...")
    

    create_tables()
    
    redis_connected = await redis_cache.init_redis()
    if not redis_connected:
        print("⚠️ سنتحدث بدون كاش Redis")
    else:
        print("✅ Redis cache initialized successfully")
    
    # Scheduler removed in favor of Render Cron Job
    # The daily update script is now run independently.

@app.get("/")
async def root():
    return {
        "message": "Saudi Stocks API",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    """فحص صحة التطبيق"""
    import datetime
    
    redis_status = "connected" if redis_cache.redis_client else "disconnected"
    return {
        "status": "healthy",
        "redis": redis_status,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "message": "API is running" + (" with cache" if redis_cache.redis_client else " without cache")
    }