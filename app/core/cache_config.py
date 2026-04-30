"""
Cache Configuration & TTL Constants
Single source of truth for all cache TTLs
"""

# ============================================================================
# TTL VALUES (in seconds) - Production Configuration
# ============================================================================

# RS Latest / RS V2 Latest / Screeners / Technical Screener
CACHE_TTL_SCREENERS = 600  # 10 minutes

# Prices Latest
CACHE_TTL_PRICES_LATEST = 300  # 5 minutes

# Prices History by Symbol
CACHE_TTL_PRICES_HISTORY = 3600  # 60 minutes

# RS History and RS V2 History
CACHE_TTL_RS_HISTORY = 1800  # 30 minutes

# Industry Groups Latest
CACHE_TTL_INDUSTRY_GROUPS_LATEST = 900  # 15 minutes

# Industry Groups Stocks
CACHE_TTL_INDUSTRY_GROUPS_STOCKS = 1200  # 20 minutes

# RS V2 Stats / RS V2 Industries / RS V2 Top-Movers
CACHE_TTL_RS_V2_STATS = 600  # 10 minutes

# ============================================================================
# Cache Key Prefixes (Namespace Organization)
# ============================================================================

PREFIX_RS = "rs"
PREFIX_RS_V2 = "rsv2"
PREFIX_SCREENER = "screener"
PREFIX_TECHNICAL_SCREENER = "technical"
PREFIX_PRICES = "prices"
PREFIX_INDUSTRY_GROUPS = "industry:groups"
