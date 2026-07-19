"""Quick script to create the aporia tables (aporia_analytics + aporia_charts)."""
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import engine, Base
from app.models.aporia import AporiaAnalytics, AporiaChart

# create_all only creates tables that don't exist yet, so it's safe
Base.metadata.create_all(bind=engine, tables=[AporiaAnalytics.__table__, AporiaChart.__table__])
print("Done - aporia_analytics and aporia_charts tables created.")
