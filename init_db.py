import os
import sys

# Add the app directory to the system path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app.core.database import create_tables
from app.models.economic_indicators import EconomicIndicator

def main():
    print("🚀 Initializing Database Tables...")
    try:
        # create_tables calls Base.metadata.create_all(bind=engine)
        # That will automatically create any tables that don't exist yet, 
        # such as the new economic_indicators table.
        create_tables()
        print("✅ Database tables successfully created or already exist.")
    except Exception as e:
        print(f"❌ Error setting up database: {e}")

if __name__ == "__main__":
    main()
