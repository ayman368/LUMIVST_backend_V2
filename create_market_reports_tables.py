#!/usr/bin/env python
"""
Script to create market reports tables directly from SQLAlchemy models
"""
import os
import sys

# Add backend to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.database import Base, engine
from app.models.market_reports import (
    SubstantialShareholder,
    NetShortPosition,
    ForeignHeadroom,
    ShareBuyback,
    SBLPosition,
)

if __name__ == "__main__":
    print("🔨 Creating market reports tables...")
    
    try:
        # Create all tables (this will skip existing tables and only create new ones)
        Base.metadata.create_all(bind=engine)
        print("✅ Market reports tables created successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
