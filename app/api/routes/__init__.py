# # # app/api/routes/__init__.py
# # from .stocks import router as companies_router
# # from .financials import router as financials_router

# # علّق الـ cache إذا كان يسبب مشاكل
# # try:
# #     from .cache import router as cache_router
# #     __all__ = ["companies_router", "financials_router", "cache_router"]
# # except ImportError:
# #     __all__ = ["companies_router", "financials_router"]


# # from .stocks import router as stocks_router
# # from .financials import router as financials_router
# # from .cache import router as cache_router
# # from .profile import router as profile_router
# # from .quote import router as quote_router

# # __all__ = ["stocks_router", "financials_router", "cache_router", "profile_router", "quote_router"]



# from .stocks import router as stocks_router
# from .financials import router as financials_router
# from .cache import router as cache_router
# # from .profile import router as profile_router
# # from .quote import router as quote_router
# from .statistics import router as statistics_router
# from .technical_indicators import router as technical_indicators_router  # ⭐ الجديد

# __all__ = [
#     "stocks_router", 
#     "financials_router", 
#     "cache_router", 
#     # "profile_router", 
#     # "quote_router",
#     "statistics_router",
#     "technical_indicators_router"  # ⭐ الجديد
# ]
# backend/app/api/endpoints/__init__.py

from . import stocks

__all__ = [ "stocks"]

# from . import auth, admin, cache, financials, profile, queue, statistics, stocks

# __all__ = ["auth", "admin", "cache", "financials", "profile", "queue", "statistics", "stocks"]